# Ladybug 0.20.4 without cache workarounds

Only Ladybug was rerun. The full suite ran from 2026-09-11 20:57:54 to 20:58:12 UTC on Apple M5, 10 logical CPUs, 24 GiB RAM, macOS 26.6.2, Python 3.13.14, pytest-benchmark 5.2.3. Colima was stopped. The result directory is a run identifier; exact execution timestamps are recorded in `ladybug-0.20.4-run.json`.

## Upgrade and upstream fix

The dependency and lockfile pin Ladybug exactly to 0.20.4. Only the Ladybug package and the root project's Ladybug requirement changed in the lockfile; no other packages were upgraded.

The [v0.20.2...v0.20.4 comparison](https://github.com/LadybugDB/ladybug/compare/v0.20.2...v0.20.4) contains [commit d571b2f](https://github.com/LadybugDB/ladybug/commit/d571b2fc18ab6f46fa9ade19d4e871ffd579d421), specifically fixing re-execution of DISTINCT aggregates on the cached-plan fast path for upstream issue #906. The commit rebuilds consumed distinct queues from saved schemas and adds a repeated-execution regression test. This confirms fix inclusion independently of issue closure.

## Harness and validation

Removed Q11's cache-disable/cache-restore calls and the dummy parameter in the shared execution helper. All 30 queries now use plain `conn.execute(query)`. Query text, assertions, ANALYZE, result materialization/printing/cleanup, and all six ART indexes are unchanged. The 0.20.4 Python driver routes parameterless strings through its query API rather than the parameterized prepared-statement cache, so these measurements reflect both the upgrade and the requested execution-path change; they do not isolate release-only performance differences.

The existing SF1 database was reused. The earlier `verify_graph.py ladybug` checker was run again using 0.20.4, verifying every node/relationship count, primary keys and six ART indexes. Results are in `validate-ladybug.json`: 3,181,724 nodes and 17,256,038 relationships.

`check_q11.py` passed 20 consecutive plain executions and, separately, 20 executions reusing one prepared statement on the same connection. Every result was `[{"num_e": 190, "o.name": "MDLR_Airlines"}]`. Logs are `q11-plain.txt` and `q11-prepared.txt`. Explicit preparation is used only in this regression check to exercise the formerly crashing path; the driver emits a deprecation warning for that API. The old cache-toggle mock tests are retained as `../20260903T210653Z/test_q11_cache_workaround.py.txt` for historical reference rather than collected against the new harness.

## Benchmark result and provenance

All 30 benchmarks passed in one process in 17.35 seconds, with no failures or skips and 5–980 measured rounds. Q11 mean: 8.789 ms (90 rounds). The settings match the previous suite: minimum 5 rounds, minimum time 0.000005 s, maximum time 1 s, perf_counter, calibration precision 10, warmup off, warmup iterations 5, GC disabled, sorted by fullname. Result conversion and printing remain timed.

`run_benchmark.py ladybug` saves CLI output, raw benchmark JSON, JUnit XML and a run record with command, timestamps, package versions, source hashes, lockfile hash and acceptance checks. `ladybug-0.20.4-run.json` records `accepted: true`.

The comparison table uses this Ladybug run and the accepted Neo4j, Kuzu and Lance Graph JSON from `../20260903T204340Z/`. Their query, benchmark and ingestion source hashes were verified unchanged. Those engines were not rerun. Neo4j's earlier deployment used a native-arm64 Colima VM with 4 CPUs and 10 GiB RAM; comparisons remain deployment-specific rather than equal-resource or controlled cold-cache measurements.

The previous top-level Ladybug 0.20.2 CLI output was moved byte-for-byte to `../archived/ladybug-0.20.2.txt`. Its original timestamped JSON and other evidence remain intact. New top-level CLI output is `../ladybug-0.20.4.txt`.
