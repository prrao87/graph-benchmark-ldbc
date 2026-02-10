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
=========================================== test session starts ===========================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark-ldbc-snb
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 30 items                                                                                        

benchmark_query.py ..............................                                                   [100%]


------------------------------------------------------------------------------------------ benchmark: 30 tests ------------------------------------------------------------------------------------------
Name (time in ms)                 Min                   Max                  Mean             StdDev                Median                IQR            Outliers       OPS            Rounds  Iterations
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1          3.8717 (3.18)         7.4454 (1.97)         4.8933 (2.98)      1.4850 (3.61)         4.2990 (2.72)      1.6227 (9.69)          1;0  204.3628 (0.34)          5           1
test_benchmark_query10         4.1303 (3.39)         7.1820 (1.90)         4.9262 (3.00)      0.6567 (1.60)         4.8589 (3.08)      0.6227 (3.72)          3;1  202.9957 (0.33)         19           1
test_benchmark_query11        12.9360 (10.62)       18.0553 (4.79)        14.9252 (9.08)      1.6719 (4.06)        14.4897 (9.18)      2.7231 (16.26)         5;0   67.0007 (0.11)         12           1
test_benchmark_query12         5.6573 (4.64)        14.3229 (3.80)         8.0887 (4.92)      3.5239 (8.56)         6.7725 (4.29)      2.4496 (14.62)         1;1  123.6298 (0.20)          5           1
test_benchmark_query13         8.3255 (6.83)        13.8492 (3.67)         9.8533 (6.00)      2.2754 (5.53)         8.8053 (5.58)      1.9628 (11.72)         1;1  101.4885 (0.17)          5           1
test_benchmark_query14         1.2736 (1.05)         4.3214 (1.15)         1.6888 (1.03)      0.5472 (1.33)         1.6169 (1.02)      0.3128 (1.87)          2;2  592.1243 (0.97)         31           1
test_benchmark_query15         2.8993 (2.38)         5.9215 (1.57)         3.3052 (2.01)      0.5641 (1.37)         3.2112 (2.04)      0.2019 (1.21)          2;3  302.5522 (0.50)         28           1
test_benchmark_query16         1.8147 (1.49)         5.0642 (1.34)         2.4007 (1.46)      0.6697 (1.63)         2.2750 (1.44)      0.4678 (2.79)          2;2  416.5537 (0.68)         24           1
test_benchmark_query17         3.3443 (2.75)        16.6362 (4.41)         4.9384 (3.01)      3.3127 (8.05)         3.7099 (2.35)      0.8293 (4.95)          2;3  202.4931 (0.33)         19           1
test_benchmark_query18         3.2864 (2.70)         6.3495 (1.68)         3.6899 (2.25)      0.6538 (1.59)         3.5115 (2.23)      0.2724 (1.63)          2;2  271.0126 (0.45)         22           1
test_benchmark_query19         5.6897 (4.67)        15.6924 (4.16)         7.7676 (4.73)      4.4306 (10.77)        5.8476 (3.71)      2.5726 (15.36)         1;1  128.7393 (0.21)          5           1
test_benchmark_query2          4.6268 (3.80)         7.5288 (2.00)         5.9006 (3.59)      0.9334 (2.27)         5.8460 (3.71)      1.0465 (6.25)          3;0  169.4754 (0.28)          9           1
test_benchmark_query20       433.0199 (355.47)     599.6303 (158.94)     475.7287 (289.49)   70.1786 (170.56)     451.5764 (286.20)   60.4575 (360.94)        1;1    2.1020 (0.00)          5           1
test_benchmark_query21         1.2413 (1.02)         4.1425 (1.10)         1.8157 (1.10)      0.6634 (1.61)         1.6061 (1.02)      0.2651 (1.58)          2;3  550.7550 (0.91)         24           1
test_benchmark_query22         3.0699 (2.52)         6.0628 (1.61)         3.6491 (2.22)      1.0811 (2.63)         3.1857 (2.02)      0.4395 (2.62)          1;1  274.0428 (0.45)          7           1
test_benchmark_query23         3.1808 (2.61)         6.2377 (1.65)         3.9493 (2.40)      0.7700 (1.87)         3.6955 (2.34)      0.3280 (1.96)          5;5  253.2080 (0.42)         26           1
test_benchmark_query24         1.2182 (1.0)          3.7727 (1.0)          1.6434 (1.0)       0.4115 (1.0)          1.5779 (1.0)       0.1933 (1.15)          5;3  608.5094 (1.0)          36           1
test_benchmark_query25         2.4818 (2.04)         5.5808 (1.48)         3.0793 (1.87)      0.6214 (1.51)         3.0067 (1.91)      0.4830 (2.88)          2;2  324.7485 (0.53)         26           1
test_benchmark_query26         1.4263 (1.17)         4.1306 (1.09)         1.8218 (1.11)      0.5590 (1.36)         1.6973 (1.08)      0.2476 (1.48)          2;2  548.9102 (0.90)         24           1
test_benchmark_query27         2.4140 (1.98)         6.7904 (1.80)         3.5443 (2.16)      1.6484 (4.01)         3.0405 (1.93)      1.1359 (6.78)          1;1  282.1438 (0.46)          6           1
test_benchmark_query28         2.6972 (2.21)         5.7863 (1.53)         3.9115 (2.38)      1.1912 (2.90)         3.2406 (2.05)      1.8656 (11.14)         3;0  255.6591 (0.42)          7           1
test_benchmark_query29         2.6210 (2.15)         6.2340 (1.65)         2.9144 (1.77)      0.5743 (1.40)         2.8131 (1.78)      0.1810 (1.08)          1;2  343.1289 (0.56)         38           1
test_benchmark_query3          2.2591 (1.85)         6.8616 (1.82)         2.9493 (1.79)      1.2008 (2.92)         2.5070 (1.59)      0.4899 (2.92)          1;1  339.0627 (0.56)         14           1
test_benchmark_query30     1,213.2225 (995.94)   1,350.6868 (358.01)   1,255.1823 (763.79)   55.0428 (133.77)   1,234.8569 (782.62)   51.8661 (309.65)        1;0    0.7967 (0.00)          5           1
test_benchmark_query4          3.5955 (2.95)         6.8703 (1.82)         4.1218 (2.51)      0.7246 (1.76)         3.8414 (2.43)      0.4304 (2.57)          1;1  242.6149 (0.40)         19           1
test_benchmark_query5          4.1471 (3.40)         5.9247 (1.57)         4.8703 (2.96)      0.8128 (1.98)         4.4418 (2.82)      1.5192 (9.07)          2;0  205.3253 (0.34)          6           1
test_benchmark_query6          2.9778 (2.44)         6.0410 (1.60)         3.8818 (2.36)      0.6363 (1.55)         3.6772 (2.33)      0.4742 (2.83)          4;2  257.6104 (0.42)         22           1
test_benchmark_query7          1.6695 (1.37)         5.1811 (1.37)         2.2006 (1.34)      0.9576 (2.33)         1.9172 (1.22)      0.2810 (1.68)          1;1  454.4178 (0.75)         12           1
test_benchmark_query8         11.7353 (9.63)        18.6258 (4.94)        14.5516 (8.85)      2.4514 (5.96)        14.0285 (8.89)      4.5380 (27.09)         3;0   68.7208 (0.11)          9           1
test_benchmark_query9          2.0752 (1.70)         6.4517 (1.71)         2.8203 (1.72)      1.0421 (2.53)         2.4998 (1.58)      0.1675 (1.0)           2;4  354.5747 (0.58)         18           1
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
=========================================== 30 passed in 23.96s ===========================================
```
