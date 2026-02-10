# Ladybug 

This section describes how benchmark the social network data in [Ladybug](https://github.com/LadybugDB/ladybug), a fork of Kuzu.

All timing numbers shown below are on an M3 Macbook Pro with 32 GB of RAM.

## Setup

Because Ladybug is an embedded graph database, the database is tightly coupled with the application layer -- there is no server to set up and run. Simply install the Ladybug Python library (`uv add kuzu`) and you're good to go!

## Build graph

The script `build_graph.py` contains the necessary methods to connect to the Ladybug DB and ingest the data from the CSV files, in batches for large amounts of data.

```sh
uv run build_graph.py
```

## Visualize graph

The provided `docker-compose.yml` allows you to run [Ladybug Explorer](https://github.com/ladybugdb/explorer), an open source visualization
tool for Ladybug. To run Ladybug Explorer, install Docker and run the following command:

```sh
docker compose up
```

This allows you to access to visualize the graph on the browser at `http://localhost:8000`.

## Execute queries

The query suite consists of 30 queries that test for n-hop retrievals from the graph using a combination of selectivity filters and projections.

Run the full query suite using the provided script below.

```bash
uv run query.py
```

Run a subset by passing a comma-separated list of query numbers:

```bash
uv run query.py "1,2,6"
```

### Run benchmark

The benchmark can be run using the following command. The results are output to a table that can be programmatically parsed for timing comparisons with other systems.

```bash
❯ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
============================================== test session starts ===============================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark-ldbc-snb
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 30 items                                                                                               

benchmark_query.py ..............................                                                          [100%]


-------------------------------------------------------------------------------------- benchmark: 30 tests ---------------------------------------------------------------------------------------
Name (time in ms)               Min                 Max                Mean            StdDev              Median                IQR            Outliers         OPS            Rounds  Iterations
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1        1.9117 (3.97)       2.6705 (2.92)       2.1081 (3.60)     0.1874 (3.60)       2.0716 (3.62)      0.1117 (1.77)          2;1    474.3652 (0.28)         13           1
test_benchmark_query10       1.7532 (3.64)       2.3283 (2.54)       1.9033 (3.25)     0.0817 (1.57)       1.8865 (3.30)      0.0914 (1.45)         49;6    525.4153 (0.31)        209           1
test_benchmark_query11       7.7270 (16.05)      9.0004 (9.82)       8.1187 (13.86)    0.2107 (4.05)       8.1073 (14.18)     0.2738 (4.35)         19;2    123.1724 (0.07)         75           1
test_benchmark_query12      14.1464 (29.38)     21.6140 (23.59)     16.9058 (28.86)    2.2256 (42.73)     16.5018 (28.87)     2.8348 (45.03)         4;0     59.1512 (0.03)         16           1
test_benchmark_query13      49.6842 (103.20)    53.5551 (58.46)     51.5110 (87.92)    0.9531 (18.30)     51.4873 (90.07)     1.1535 (18.32)         5;0     19.4133 (0.01)         18           1
test_benchmark_query14       1.6579 (3.44)       2.1268 (2.32)       1.8347 (3.13)     0.1053 (2.02)       1.8167 (3.18)      0.1391 (2.21)         41;4    545.0562 (0.32)        144           1
test_benchmark_query15       2.5260 (5.25)       3.4298 (3.74)       2.7253 (4.65)     0.1335 (2.56)       2.6937 (4.71)      0.1512 (2.40)        55;11    366.9327 (0.21)        269           1
test_benchmark_query16       1.8451 (3.83)       2.3163 (2.53)       1.9933 (3.40)     0.0831 (1.60)       1.9953 (3.49)      0.0946 (1.50)         21;2    501.6840 (0.29)         85           1
test_benchmark_query17       2.7762 (5.77)       3.4865 (3.81)       2.9867 (5.10)     0.1106 (2.12)       2.9715 (5.20)      0.1347 (2.14)         46;4    334.8133 (0.20)        190           1
test_benchmark_query18       1.6432 (3.41)       2.2382 (2.44)       1.8052 (3.08)     0.1070 (2.06)       1.7788 (3.11)      0.1249 (1.98)        85;17    553.9455 (0.32)        365           1
test_benchmark_query19       9.4651 (19.66)     22.8702 (24.96)     14.1154 (24.09)    2.8611 (54.93)     13.5340 (23.67)     2.4370 (38.71)        15;4     70.8448 (0.04)         56           1
test_benchmark_query2        1.2712 (2.64)       1.9603 (2.14)       1.4940 (2.55)     0.1408 (2.70)       1.5063 (2.63)      0.2294 (3.64)         24;1    669.3568 (0.39)         63           1
test_benchmark_query20      10.7147 (22.26)     11.1838 (12.21)     10.9092 (18.62)    0.1331 (2.56)      10.8868 (19.04)     0.1968 (3.13)         10;0     91.6657 (0.05)         23           1
test_benchmark_query21       0.4814 (1.0)        0.9161 (1.0)        0.5859 (1.0)      0.0645 (1.24)       0.5717 (1.0)       0.0630 (1.0)         50;13  1,706.8294 (1.0)         238           1
test_benchmark_query22      14.2664 (29.63)     34.6549 (37.83)     22.2841 (38.04)    4.2106 (80.84)     22.6807 (39.67)     5.8241 (92.51)        14;1     44.8751 (0.03)         45           1
test_benchmark_query23       1.3136 (2.73)       2.5025 (2.73)       1.5575 (2.66)     0.1299 (2.49)       1.5345 (2.68)      0.1553 (2.47)       134;12    642.0463 (0.38)        543           1
test_benchmark_query24       1.3690 (2.84)       1.7574 (1.92)       1.5081 (2.57)     0.0657 (1.26)       1.4949 (2.61)      0.0830 (1.32)       141;12    663.1070 (0.39)        492           1
test_benchmark_query25       1.4820 (3.08)       4.4241 (4.83)       1.7130 (2.92)     0.2880 (5.53)       1.6657 (2.91)      0.1128 (1.79)        11;18    583.7764 (0.34)        367           1
test_benchmark_query26       3.4514 (7.17)       4.3842 (4.79)       3.5946 (6.14)     0.0929 (1.78)       3.5871 (6.27)      0.0974 (1.55)         48;4    278.1939 (0.16)        205           1
test_benchmark_query27      10.1100 (21.00)     28.5882 (31.21)     15.4468 (26.36)    4.4585 (85.60)     15.2192 (26.62)     4.1056 (65.21)        13;6     64.7385 (0.04)         56           1
test_benchmark_query28       1.6654 (3.46)       2.0297 (2.22)       1.8098 (3.09)     0.0583 (1.12)       1.8051 (3.16)      0.0761 (1.21)        168;7    552.5401 (0.32)        510           1
test_benchmark_query29       1.0920 (2.27)       1.4700 (1.60)       1.2240 (2.09)     0.0611 (1.17)       1.2120 (2.12)      0.0751 (1.19)        96;10    816.9921 (0.48)        348           1
test_benchmark_query3        1.1448 (2.38)       1.5808 (1.73)       1.2674 (2.16)     0.0824 (1.58)       1.2460 (2.18)      0.1025 (1.63)         52;6    789.0132 (0.46)        190           1
test_benchmark_query30     144.2956 (299.73)   169.7094 (185.25)   154.3682 (263.48)   8.5969 (165.05)   153.8919 (269.20)   11.1298 (176.78)        2;0      6.4780 (0.00)          7           1
test_benchmark_query4        0.9089 (1.89)       1.2413 (1.35)       1.0283 (1.76)     0.0521 (1.0)        1.0202 (1.78)      0.0631 (1.00)         88;7    972.4397 (0.57)        312           1
test_benchmark_query5        3.4085 (7.08)       4.1252 (4.50)       3.6512 (6.23)     0.1165 (2.24)       3.6401 (6.37)      0.1328 (2.11)         21;2    273.8851 (0.16)         82           1
test_benchmark_query6        0.7381 (1.53)       1.1139 (1.22)       0.8477 (1.45)     0.0634 (1.22)       0.8386 (1.47)      0.0798 (1.27)        106;6  1,179.6415 (0.69)        336           1
test_benchmark_query7       29.5983 (61.48)     41.4377 (45.23)     31.5597 (53.87)    2.1364 (41.02)     30.9265 (54.10)     1.1964 (19.00)         2;2     31.6860 (0.02)         29           1
test_benchmark_query8        2.7169 (5.64)       3.2915 (3.59)       2.8623 (4.89)     0.0910 (1.75)       2.8420 (4.97)      0.1151 (1.83)         39;3    349.3652 (0.20)        148           1
test_benchmark_query9        1.9575 (4.07)       6.0546 (6.61)       2.2280 (3.80)     0.4422 (8.49)       2.1359 (3.74)      0.1619 (2.57)          5;7    448.8280 (0.26)        196           1
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
============================================== 30 passed in 18.23s ===============================================
```