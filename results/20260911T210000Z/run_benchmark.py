"""Run one full engine suite, retaining output and validating the run contract."""

import hashlib
import json
import platform
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path

OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[1]
ENGINES = {
    "ladybug": ("ladybugdb", "ladybug-0.20.4"),
    "kuzu": ("kuzu", "kuzu-0.11.3"),
    "lance-graph": ("lance_graph", "lance-graph-0.5.4"),
    "neo4j": ("neo4j", "neo4j-2025.12.1"),
}
OPTIONS = [
    "--benchmark-min-rounds=5",
    "--benchmark-min-time=0.000005",
    "--benchmark-max-time=1.0",
    "--benchmark-timer=time.perf_counter",
    "--benchmark-calibration-precision=10",
    "--benchmark-warmup=off",
    "--benchmark-warmup-iterations=5",
    "--benchmark-disable-gc",
    "--benchmark-sort=fullname",
]


def main():
    engine = sys.argv[1]
    directory, label = ENGINES[engine]
    selected = (
        [int(value) for value in sys.argv[2].split(",")]
        if len(sys.argv) > 2
        else list(range(1, 31))
    )
    assert (
        selected
        and len(selected) == len(set(selected))
        and all(1 <= n <= 30 for n in selected)
    )
    if len(sys.argv) > 2:
        label += "-q" + "-".join(map(str, selected))
    assert metadata.version("ladybug") == "0.20.4"
    assert metadata.version("pytest-benchmark") == "5.2.3"
    validation = json.loads((OUT / f"validate-{engine}.json").read_text())
    assert validation["validated"]
    for suffix in (".json", ".txt", ".xml", "-run.json"):
        assert not (OUT / f"{label}{suffix}").exists(), (
            "Refusing to overwrite previous evidence"
        )
    command = [
        "uv",
        "run",
        "--frozen",
        "pytest",
        *(
            [f"benchmark_query.py::test_benchmark_query{n}" for n in selected]
            if len(sys.argv) > 2
            else ["benchmark_query.py"]
        ),
        *OPTIONS,
        f"--benchmark-json={OUT / (label + '.json')}",
        f"--junitxml={OUT / (label + '.xml')}",
    ]
    record = {
        "engine": engine,
        "queries": selected,
        "diagnostic_subset": len(sys.argv) > 2,
        "command": command,
        "cwd": str(ROOT / directory),
        "start_utc": datetime.now(UTC).isoformat(),
        "git_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "lock_sha256": hashlib.sha256((ROOT / "uv.lock").read_bytes()).hexdigest(),
        "source_sha256": {
            name: hashlib.sha256((ROOT / directory / name).read_bytes()).hexdigest()
            for name in ("query.py", "benchmark_query.py", "build_graph.py")
        },
        "python": sys.version,
        "platform": platform.platform(),
        "packages": {d.metadata["Name"]: d.version for d in metadata.distributions()},
    }
    with (OUT / f"{label}.txt").open("x") as log:
        proc = subprocess.Popen(
            command,
            cwd=ROOT / directory,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        for line in proc.stdout:
            log.write(line)
            log.flush()
            print(line, end="", flush=True)
        record["exit_code"] = proc.wait()
    record["finish_utc"] = datetime.now(UTC).isoformat()
    errors = []
    try:
        suites = list(ET.parse(OUT / f"{label}.xml").getroot().iter("testsuite"))
        totals = {
            k: sum(int(s.get(k, 0)) for s in suites)
            for k in ("tests", "failures", "errors", "skipped")
        }
        record["test_totals"] = totals
        if totals != {"tests": len(selected), "failures": 0, "errors": 0, "skipped": 0}:
            errors.append(f"Unexpected JUnit totals: {totals}")
        data = json.loads((OUT / f"{label}.json").read_text())
        benchmarks = data["benchmarks"]
        if {b["name"] for b in benchmarks} != {
            f"test_benchmark_query{i}" for i in selected
        }:
            errors.append("Incomplete benchmark query set")
        for b in benchmarks:
            if b["stats"]["rounds"] < 5:
                errors.append(f"Insufficient rounds: {b['name']}")
            expected_options = {
                "disable_gc": True,
                "min_rounds": 5,
                "max_time": 1.0,
                "min_time": 0.000005,
                "warmup": False,
            }
            for key, expected in expected_options.items():
                if b["options"].get(key) != expected:
                    errors.append(f"Unexpected {key}: {b['name']}")
            if "perf_counter" not in b["options"]["timer"]:
                errors.append(f"Unexpected timer: {b['name']}")
        record["round_range"] = [
            min(b["stats"]["rounds"] for b in benchmarks),
            max(b["stats"]["rounds"] for b in benchmarks),
        ]
    except (OSError, ValueError, KeyError, ET.ParseError) as exc:
        errors.append(str(exc))
    record["validation_errors"] = errors
    record["accepted"] = record["exit_code"] == 0 and not errors
    with (OUT / f"{label}-run.json").open("x") as f:
        json.dump(record, f, indent=2)
        f.write("\n")
    print(
        json.dumps(
            {
                k: record[k]
                for k in ("engine", "exit_code", "accepted", "validation_errors")
            }
        )
    )
    return 0 if record["accepted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
