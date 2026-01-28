# Neo4j

This section describes how to benchmark the social network data in Neo4j.

All timing numbers shown below are on an M3 Macbook Pro with 32 GB of RAM.

## Setup

Neo4j runs as a server, so you need a running instance (for example via Docker)
reachable at `bolt://localhost:7687`. Configure credentials via environment
variables:

- `NEO4J_URI` (optional, defaults to `bolt://localhost:7687`)
- `NEO4J_USER`
- `NEO4J_PASSWORD`
- `NEO4J_DATABASE` (optional, defaults to `neo4j`)

## Build graph

The script `build_graph.py` contains the necessary methods to connect to the Neo4j DB and ingest the data from the CSV files, in batches for large amounts of data.

```sh
python build_graph.py
```

## Visualize graph

You can visualize the graph in the Neo4j browser by a) downloading the Neo4j Desktop tool, or b) in the browser via `http://localhost:7474`.

## Execute queries

The query suite consists of 30 queries that test for n-hop retrievals from the
graph using a combination of selectivity filters and projections.

Run the full query suite using the provided script below.

```bash
uv run query.py
```

Run a subset by passing a comma-separated list of query numbers:

```bash
uv run query.py "1,2,6"
```

### Run benchmark

The benchmark can be run using the following command. The results are output to
a table that can be programmatically parsed for timing comparisons with other
systems.

```bash
❯ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
==================================== test session starts ====================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark-ldbc-snb
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 29 items                                                                          

benchmark_query.py .............................                                      [100%]


-------------------------------------------------------------------------------------- benchmark: 29 tests --------------------------------------------------------------------------------------
Name (time in ms)               Min                 Max                Mean             StdDev              Median                IQR            Outliers       OPS            Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1        4.5662 (4.07)       7.9165 (2.15)       5.7428 (3.78)      1.2804 (2.71)       5.3974 (3.84)      1.1992 (6.12)          1;0  174.1311 (0.26)          5           1
test_benchmark_query10       4.3096 (3.85)       7.6700 (2.08)       4.9036 (3.23)      0.8107 (1.72)       4.5665 (3.25)      0.3377 (1.72)          3;3  203.9318 (0.31)         21           1
test_benchmark_query11      11.3090 (10.09)     16.4480 (4.46)      12.9206 (8.51)      1.7023 (3.61)      12.1890 (8.68)      2.8055 (14.32)         2;0   77.3955 (0.12)         14           1
test_benchmark_query12       3.4635 (3.09)       6.5704 (1.78)       4.6609 (3.07)      1.1609 (2.46)       4.4706 (3.18)      1.1712 (5.98)          2;0  214.5527 (0.33)          5           1
test_benchmark_query13       7.6419 (6.82)      10.2188 (2.77)       8.7770 (5.78)      0.9356 (1.98)       8.5917 (6.12)      0.9677 (4.94)          2;0  113.9337 (0.17)          5           1
test_benchmark_query14       1.1795 (1.05)       4.0575 (1.10)       1.5923 (1.05)      0.5706 (1.21)       1.4067 (1.00)      0.2915 (1.49)          4;4  628.0120 (0.95)         35           1
test_benchmark_query15       2.7099 (2.42)       5.9078 (1.60)       3.2335 (2.13)      0.5319 (1.13)       3.1391 (2.23)      0.2711 (1.38)          2;2  309.2583 (0.47)         33           1
test_benchmark_query16       1.4991 (1.34)       4.2685 (1.16)       1.9887 (1.31)      0.5715 (1.21)       1.8387 (1.31)      0.4499 (2.30)          3;2  502.8378 (0.76)         27           1
test_benchmark_query17       3.6983 (3.30)      16.9161 (4.59)       5.2950 (3.49)      3.1083 (6.59)       4.1400 (2.95)      0.7694 (3.93)          2;3  188.8588 (0.29)         21           1
test_benchmark_query18       3.6140 (3.23)       6.8964 (1.87)       4.0790 (2.69)      0.6890 (1.46)       3.9085 (2.78)      0.2310 (1.18)          2;3  245.1596 (0.37)         22           1
test_benchmark_query19       5.3842 (4.80)       8.9943 (2.44)       6.1461 (4.05)      1.5929 (3.38)       5.4534 (3.88)      0.9807 (5.00)          1;1  162.7046 (0.25)          5           1
test_benchmark_query2        5.5077 (4.92)       7.7213 (2.09)       6.4048 (4.22)      0.8509 (1.80)       6.0194 (4.28)      1.5655 (7.99)          4;0  156.1333 (0.24)          9           1
test_benchmark_query20     400.9177 (357.78)   463.1863 (125.58)   421.8090 (277.94)   24.5619 (52.06)    412.9683 (293.94)   27.1205 (138.38)        1;0    2.3707 (0.00)          5           1
test_benchmark_query21       1.2233 (1.09)       4.1606 (1.13)       1.5734 (1.04)      0.5670 (1.20)       1.4747 (1.05)      0.1960 (1.0)           1;2  635.5555 (0.96)         25           1
test_benchmark_query22       3.2502 (2.90)       5.2348 (1.42)       3.6827 (2.43)      0.7463 (1.58)       3.3378 (2.38)      0.6475 (3.30)          1;1  271.5424 (0.41)          7           1
test_benchmark_query23       3.2370 (2.89)       6.6556 (1.80)       4.0356 (2.66)      0.8064 (1.71)       3.9351 (2.80)      0.7647 (3.90)          4;2  247.7977 (0.38)         25           1
test_benchmark_query24       1.1206 (1.0)        4.0367 (1.09)       1.5177 (1.0)       0.4718 (1.0)        1.4050 (1.0)       0.3551 (1.81)          1;1  658.9131 (1.0)          37           1
test_benchmark_query25       2.6251 (2.34)       5.6793 (1.54)       3.0771 (2.03)      0.6817 (1.44)       2.8469 (2.03)      0.3698 (1.89)          3;3  324.9813 (0.49)         25           1
test_benchmark_query26       1.2396 (1.11)       3.6884 (1.0)        1.7845 (1.18)      0.5056 (1.07)       1.7100 (1.22)      0.3985 (2.03)          5;2  560.3905 (0.85)         25           1
test_benchmark_query27       2.6475 (2.36)       5.5377 (1.50)       3.3515 (2.21)      1.0913 (2.31)       2.8824 (2.05)      0.4261 (2.17)          1;1  298.3708 (0.45)          6           1
test_benchmark_query28       3.0628 (2.73)       6.0044 (1.63)       4.3319 (2.85)      1.2427 (2.63)       3.6485 (2.60)      2.2453 (11.46)         3;0  230.8469 (0.35)          8           1
test_benchmark_query29       2.5316 (2.26)       5.5024 (1.49)       3.0835 (2.03)      0.4901 (1.04)       2.9675 (2.11)      0.3126 (1.60)          6;3  324.3094 (0.49)         39           1
test_benchmark_query3        1.9932 (1.78)       6.7290 (1.82)       2.7822 (1.83)      1.1888 (2.52)       2.4327 (1.73)      0.5806 (2.96)          1;1  359.4313 (0.55)         14           1
test_benchmark_query4        4.1825 (3.73)       7.5235 (2.04)       4.8082 (3.17)      0.7715 (1.64)       4.5888 (3.27)      0.5105 (2.60)          2;2  207.9802 (0.32)         18           1
test_benchmark_query5        4.5011 (4.02)       6.3177 (1.71)       5.0708 (3.34)      0.6389 (1.35)       4.8507 (3.45)      0.2684 (1.37)          1;1  197.2073 (0.30)          6           1
test_benchmark_query6        3.3081 (2.95)       6.7625 (1.83)       4.1487 (2.73)      0.7289 (1.54)       3.9722 (2.83)      0.4310 (2.20)          4;3  241.0393 (0.37)         21           1
test_benchmark_query7        1.6739 (1.49)       5.2899 (1.43)       2.1989 (1.45)      1.0538 (2.23)       1.7943 (1.28)      0.5013 (2.56)          1;1  454.7639 (0.69)         11           1
test_benchmark_query8       13.4295 (11.98)     17.6011 (4.77)      15.5802 (10.27)     1.7500 (3.71)      15.4720 (11.01)     3.4450 (17.58)         5;0   64.1840 (0.10)          9           1
test_benchmark_query9        1.9796 (1.77)       5.6878 (1.54)       2.6149 (1.72)      0.9271 (1.97)       2.2784 (1.62)      0.5254 (2.68)          2;2  382.4230 (0.58)         19           1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
==================================== 29 passed in 13.19s ====================================
```
