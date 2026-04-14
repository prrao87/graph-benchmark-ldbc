# lance-graph 

This section describes how to build and query a graph of the social network data using [lance-graph](https://github.com/lance-format/lance-graph), built on top of the [Lance](https://lance.org/) format. The graph is stored as Lance datasets on disk, and lance-graph provides a Cypher engine that interprets those datasets via a GraphConfig mapping.

All timing numbers shown below are on an M3 Macbook Pro with 32 GB of RAM.

## Setup

Lance Graph runs locally and reads Lance datasets directly from disk.

```sh
uv add lance-graph lance pyarrow polars
```

## Build graph

The script `build_graph.py` converts the Parquet node/edge datasets under
`data/output` into Lance datasets under `lance_graph/graph_lance`.

```sh
uv run build_graph.py
```

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

```bash
❯ uv run pytest benchmark_query.py --benchmark-min-rounds=5 --benchmark-warmup-iterations=5 --benchmark-disable-gc --benchmark-sort=fullname
================================= test session starts =================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark-ldbc-snb
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 30 items                                                                    

benchmark_query.py ..............................                               [100%]


----------------------------------------------------------------------------------- benchmark: 30 tests -----------------------------------------------------------------------------------
Name (time in ms)              Min                Max               Mean            StdDev             Median               IQR            Outliers       OPS            Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1       2.0876 (1.87)      2.7328 (1.51)      2.2985 (1.73)     0.0940 (1.31)      2.2913 (1.78)     0.1043 (1.12)         44;5  435.0649 (0.58)        174           1
test_benchmark_query10     31.3780 (28.09)    33.2631 (18.38)    32.2531 (24.31)    0.4689 (6.53)     32.2288 (24.97)    0.6442 (6.93)         10;0   31.0048 (0.04)         30           1
test_benchmark_query11      4.5374 (4.06)      6.4273 (3.55)      5.0690 (3.82)     0.2535 (3.53)      5.0219 (3.89)     0.2623 (2.82)         41;5  197.2775 (0.26)        169           1
test_benchmark_query12     21.4533 (19.20)    23.1818 (12.81)    22.2259 (16.75)    0.4042 (5.63)     22.1473 (17.16)    0.5535 (5.95)         13;0   44.9925 (0.06)         43           1
test_benchmark_query13     10.8653 (9.73)     12.3710 (6.84)     11.4504 (8.63)     0.2582 (3.60)     11.4369 (8.86)     0.2929 (3.15)         23;2   87.3332 (0.12)         82           1
test_benchmark_query14      3.7527 (3.36)      4.4870 (2.48)      3.9594 (2.98)     0.1237 (1.72)      3.9272 (3.04)     0.1495 (1.61)         55;9  252.5616 (0.34)        227           1
test_benchmark_query15      3.2638 (2.92)      4.0103 (2.22)      3.5070 (2.64)     0.1945 (2.71)      3.4513 (2.67)     0.2302 (2.48)         80;9  285.1462 (0.38)        284           1
test_benchmark_query16      5.3734 (4.81)      6.3506 (3.51)      5.7421 (4.33)     0.1665 (2.32)      5.7120 (4.43)     0.2061 (2.22)         48;4  174.1509 (0.23)        171           1
test_benchmark_query17      3.5899 (3.21)      4.4777 (2.47)      3.8325 (2.89)     0.1075 (1.50)      3.8159 (2.96)     0.1047 (1.13)        49;10  260.9276 (0.35)        234           1
test_benchmark_query18      2.8017 (2.51)      3.4695 (1.92)      3.0091 (2.27)     0.0933 (1.30)      2.9948 (2.32)     0.1064 (1.14)        74;10  332.3300 (0.44)        310           1
test_benchmark_query19     24.1473 (21.61)    27.9887 (15.47)    25.7661 (19.42)    0.8339 (11.62)    25.7148 (19.92)    0.7185 (7.73)         11;4   38.8106 (0.05)         41           1
test_benchmark_query2       2.9749 (2.66)      5.0347 (2.78)      3.2624 (2.46)     0.2485 (3.46)      3.2246 (2.50)     0.1659 (1.78)         17;8  306.5226 (0.41)        228           1
test_benchmark_query20      3.1487 (2.82)      4.4106 (2.44)      3.5643 (2.69)     0.1951 (2.72)      3.5436 (2.75)     0.2817 (3.03)         85;2  280.5602 (0.37)        246           1
test_benchmark_query21      2.2094 (1.98)      3.3378 (1.84)      2.5601 (1.93)     0.1871 (2.61)      2.5143 (1.95)     0.1522 (1.64)        47;28  390.6079 (0.52)        246           1
test_benchmark_query22     16.0833 (14.40)    17.1881 (9.50)     16.7337 (12.61)    0.2365 (3.29)     16.7227 (12.96)    0.3397 (3.65)         16;0   59.7597 (0.08)         58           1
test_benchmark_query23      3.7026 (3.31)      4.4072 (2.44)      3.9043 (2.94)     0.0977 (1.36)      3.8987 (3.02)     0.1226 (1.32)         61;4  256.1292 (0.34)        233           1
test_benchmark_query24      2.7431 (2.46)      3.1657 (1.75)      2.9265 (2.21)     0.0718 (1.0)       2.9235 (2.27)     0.0930 (1.0)         103;5  341.7026 (0.45)        317           1
test_benchmark_query25      2.1118 (1.89)      2.8850 (1.59)      2.3600 (1.78)     0.1918 (2.67)      2.2930 (1.78)     0.2117 (2.28)       104;28  423.7358 (0.56)        402           1
test_benchmark_query26      4.2908 (3.84)      4.9572 (2.74)      4.5370 (3.42)     0.1209 (1.68)      4.5202 (3.50)     0.1552 (1.67)         59;5  220.4122 (0.29)        203           1
test_benchmark_query27     29.0500 (26.00)    34.7526 (19.21)    32.2696 (24.32)    1.1777 (16.41)    32.3490 (25.07)    1.6449 (17.69)         7;0   30.9889 (0.04)         35           1
test_benchmark_query28      3.8894 (3.48)      7.4713 (4.13)      4.1726 (3.15)     0.3235 (4.51)      4.1033 (3.18)     0.1638 (1.76)        10;12  239.6599 (0.32)        162           1
test_benchmark_query29      3.2991 (2.95)      4.1572 (2.30)      3.6671 (2.76)     0.1857 (2.59)      3.6467 (2.83)     0.2596 (2.79)         71;0  272.6924 (0.36)        244           1
test_benchmark_query3       2.8281 (2.53)      3.7213 (2.06)      3.1426 (2.37)     0.1249 (1.74)      3.1375 (2.43)     0.1491 (1.60)         73;5  318.2077 (0.42)        253           1
test_benchmark_query30     38.1170 (34.12)    40.0814 (22.15)    38.8479 (29.28)    0.5717 (7.96)     38.7160 (30.00)    0.4409 (4.74)          7;4   25.7414 (0.03)         25           1
test_benchmark_query4       3.8772 (3.47)      4.8569 (2.68)      4.1841 (3.15)     0.1464 (2.04)      4.1744 (3.23)     0.1676 (1.80)         56;4  239.0025 (0.32)        195           1
test_benchmark_query5       2.6827 (2.40)      3.4333 (1.90)      2.9738 (2.24)     0.1165 (1.62)      2.9633 (2.30)     0.1477 (1.59)         74;5  336.2664 (0.45)        270           1
test_benchmark_query6       1.1173 (1.0)       1.8095 (1.0)       1.3266 (1.0)      0.1645 (2.29)      1.2906 (1.0)      0.2235 (2.40)        202;4  753.7872 (1.0)         677           1
test_benchmark_query7      18.7448 (16.78)    20.1970 (11.16)    19.1332 (14.42)    0.2754 (3.84)     19.1193 (14.81)    0.3102 (3.34)         11;1   52.2653 (0.07)         42           1
test_benchmark_query8       1.7707 (1.58)      2.3492 (1.30)      1.9536 (1.47)     0.1060 (1.48)      1.9345 (1.50)     0.1034 (1.11)        86;32  511.8781 (0.68)        405           1
test_benchmark_query9       3.1493 (2.82)      3.8964 (2.15)      3.3595 (2.53)     0.0993 (1.38)      3.3512 (2.60)     0.1235 (1.33)         67;5  297.6604 (0.39)        273           1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
================================= 30 passed in 28.86s =================================
```