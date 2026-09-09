"""Benchmark harness for the LDBC SNB Interactive complex-14 query suite.

Times each of the 14 queries in ``query_complex14.py`` over several measured
rounds (plus warmup) and prints a markdown summary table.

Usage (run from this directory with the project venv)::

    ../.venv/bin/python benchmark_complex14.py [db_path] [--rounds 5] [--warmup 1]
    ../.venv/bin/python benchmark_complex14.py --queries "1,7,13"

Notes:
- The database is opened READ-ONLY; per-round result printing is suppressed
  during measurement, so timings reflect query execution only.
- Prepared-statement caching stays enabled except around Q1/Q6/Q10/Q12,
  which segfault on repeated cached execution (LadybugDB/ladybug#906).
- Raw timings are also saved as JSON + markdown under ``../results/``.
"""

import contextlib
import io
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import ladybug as lb
import pytest

import query_complex14 as qo

DB_DEFAULT = "./ldbc_snb_sf1.lbdb"

# Queries that segfault when re-executed with the prepared-statement cache
# workaround for buggy versions of ladybug
UNCACHED_QUERIES = {}

DB_PATH = Path(__file__).with_name("ldbc_snb_sf1.lbdb")


@pytest.fixture(scope="session")
def connection():
    db = lb.Database(str(DB_PATH), read_only=True)
    conn = lb.Connection(db)
    yield conn


def _row_count(result) -> int:
    try:
        return len(result)
    except TypeError:
        return 1


def _parse_args(argv: list[str]):
    db_path = DB_DEFAULT
    rounds = 5
    warmup = 1
    selected = None
    isolate = False
    json_stdout = False
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--rounds":
            rounds = int(argv[i + 1])
            i += 2
        elif a == "--warmup":
            warmup = int(argv[i + 1])
            i += 2
        elif a == "--queries":
            selected = [int(p) for p in argv[i + 1].split(",") if p.strip()]
            i += 2
        elif a == "--isolate":
            isolate = True
            i += 1
        elif a == "--json-stdout":
            json_stdout = True
            i += 1
        elif a.startswith("-"):
            raise ValueError(f"Unknown flag: {a}")
        else:
            db_path = a
            i += 1
    return db_path, rounds, warmup, selected, isolate, json_stdout


def main() -> None:
    db_path, rounds, warmup, selected, isolate, json_stdout = _parse_args(sys.argv[1:])
    if selected is None:
        selected = sorted(qo.QUERY_FUNCTIONS.keys())

    if isolate and len(selected) > 1:
        _main_isolated(db_path, rounds, warmup, selected)
        return

    summary = _run_suite(db_path, rounds, warmup, selected)
    _report(summary, db_path, warmup, rounds, json_stdout)


def _main_isolated(db_path: str, rounds: int, warmup: int, selected: list[int]) -> None:
    """Run each query in a fresh child process (memory isolation), merge."""
    import subprocess

    merged: list[dict] = []
    for idx in selected:
        cmd = [
            sys.executable, str(Path(__file__)), db_path,
            "--rounds", str(rounds), "--warmup", str(warmup),
            "--queries", str(idx), "--json-stdout",
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=Path(__file__).parent)
        if proc.returncode != 0:
            print(f"[isolate] Q{idx} child failed (exit {proc.returncode}):")
            print(proc.stdout[-2000:] if proc.stdout else "")
            print(proc.stderr[-2000:] if proc.stderr else "")
            raise SystemExit(f"benchmark child for Q{idx} failed")
        start = proc.stdout.find("@@BENCH_JSON@@")
        end = proc.stdout.find("@@END_BENCH_JSON@@")
        if start < 0 or end < 0:
            raise SystemExit(f"benchmark child for Q{idx} produced no JSON payload")
        payload = json.loads(proc.stdout[start + len("@@BENCH_JSON@@"):end])
        merged.extend(payload["results"])
        s = payload["results"][0]
        print(
            f"Q{idx:>2}: mean={s['mean_ms']:10.2f} ms  "
            f"median={s['median_ms']:10.2f} ms  "
            f"min={s['min_ms']:10.2f} ms  max={s['max_ms']:10.2f} ms  "
            f"rows={s['rows']}  cache={s['cache']}  ({s['name']})"
        )
    _report(merged, db_path, warmup, rounds, False)


def _run_suite(db_path: str, rounds: int, warmup: int, selected: list[int]) -> list[dict]:
    db = lb.Database(db_path, read_only=True)
    conn = lb.Connection(db)

    print(f"DB: {db_path} (read-only) | warmup={warmup} measured_rounds={rounds}")
    print(f"Queries: {selected}\n")

    summary: list[dict] = []
    for idx in selected:
        func = qo.QUERY_FUNCTIONS.get(idx)
        if func is None:
            print(f"Skipping unknown query index: {idx}")
            continue
        name = (func.__doc__ or "").strip().splitlines()[0]
        uncached = idx in UNCACHED_QUERIES
        cache_label = "uncached" if uncached else "cached"
        if uncached:
            conn.execute("CALL enable_cached_prepared_statement='none';")
        try:
            for _ in range(warmup):
                with contextlib.redirect_stdout(io.StringIO()):
                    func(conn)
            samples: list[float] = []
            rows = 0
            for _ in range(rounds):
                with contextlib.redirect_stdout(io.StringIO()):
                    t0 = time.perf_counter()
                    result = func(conn)
                    t1 = time.perf_counter()
                samples.append((t1 - t0) * 1000.0)
                rows = _row_count(result)
        finally:
            if uncached:
                conn.execute("CALL enable_cached_prepared_statement='both';")
        summary.append(
            {
                "query": f"Q{idx}",
                "name": name,
                "cache": cache_label,
                "rows": rows,
                "rounds": rounds,
                "mean_ms": statistics.mean(samples),
                "median_ms": statistics.median(samples),
                "min_ms": min(samples),
                "max_ms": max(samples),
                "stdev_ms": statistics.stdev(samples) if len(samples) > 1 else 0.0,
            }
        )
        s = summary[-1]
        print(
            f"Q{idx:>2}: mean={s['mean_ms']:10.2f} ms  "
            f"median={s['median_ms']:10.2f} ms  "
            f"min={s['min_ms']:10.2f} ms  max={s['max_ms']:10.2f} ms  "
            f"rows={rows}  cache={s['cache']}  ({name})"
        )
    return summary


def _report(summary: list[dict], db_path: str, warmup: int, rounds: int, json_stdout: bool) -> None:
    total_mean = sum(s["mean_ms"] for s in summary)
    lines = [
        "",
        "| Query | Description | Rows | Cache | Mean (ms) | Median (ms) | Min (ms) | Max (ms) |",
        "| --- | --- | ---: | --- | ---: | ---: | ---: | ---: |",
    ]
    for s in summary:
        lines.append(
            f"| {s['query']} | {s['name']} | {s['rows']} | {s['cache']} | "
            f"{s['mean_ms']:.2f} | {s['median_ms']:.2f} | "
            f"{s['min_ms']:.2f} | {s['max_ms']:.2f} |"
        )
    lines.append(f"| **Total (sum of means)** | | | **{total_mean:.2f}** | | | |")
    table = "\n".join(lines)
    print(table)

    # persist raw results alongside the repo's other benchmark outputs
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    outdir = Path(__file__).with_name("..").joinpath("results", f"{stamp}-complex14").resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    payload = {
        "timestamp_utc": stamp,
        "db": str(db_path),
        "warmup_rounds": warmup,
        "measured_rounds": rounds,
        "engine": "ladybug",
        "results": summary,
        "total_mean_ms": total_mean,
    }
    (outdir / "benchmark_complex14.json").write_text(json.dumps(payload, indent=2))
    (outdir / "benchmark_complex14.md").write_text(
        f"# LDBC SNB Interactive complex-14 benchmark ({stamp})\n\n"
        f"DB: `{db_path}` (read-only), warmup={warmup}, rounds={rounds}.\n" + table + "\n"
    )
    print(f"\nSaved JSON + markdown to {outdir}")
    if json_stdout:
        print("@@BENCH_JSON@@" + json.dumps(payload) + "@@END_BENCH_JSON@@")


def test_benchmark_complex1(benchmark, connection):
    benchmark(qo.run_query1, connection)


def test_benchmark_complex2(benchmark, connection):
    benchmark(qo.run_query2, connection)


def test_benchmark_complex3(benchmark, connection):
    benchmark(qo.run_query3, connection)


def test_benchmark_complex4(benchmark, connection):
    benchmark(qo.run_query4, connection)


def test_benchmark_complex5(benchmark, connection):
    benchmark(qo.run_query5, connection)


def test_benchmark_complex6(benchmark, connection):
    benchmark(qo.run_query6, connection)


def test_benchmark_complex7(benchmark, connection):
    benchmark(qo.run_query7, connection)


def test_benchmark_complex8(benchmark, connection):
    benchmark(qo.run_query8, connection)


def test_benchmark_complex9(benchmark, connection):
    benchmark(qo.run_query9, connection)


def test_benchmark_complex10(benchmark, connection):
    benchmark(qo.run_query10, connection)


def test_benchmark_complex11(benchmark, connection):
    benchmark(qo.run_query11, connection)


def test_benchmark_complex12(benchmark, connection):
    benchmark(qo.run_query12, connection)


def test_benchmark_complex13(benchmark, connection):
    benchmark(qo.run_query13, connection)


def test_benchmark_complex14(benchmark, connection):
    benchmark(qo.run_query14, connection)


if __name__ == "__main__":
    main()
