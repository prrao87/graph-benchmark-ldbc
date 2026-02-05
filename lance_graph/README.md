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
============================================ test session starts =============================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark-ldbc-snb
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 29 items                                                                                           

benchmark_query.py .............................                                                       [100%]


------------------------------------------------------------------------------------ benchmark: 29 tests ------------------------------------------------------------------------------------
Name (time in ms)              Min                Max               Mean            StdDev             Median               IQR            Outliers         OPS            Rounds  Iterations
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1       1.3767 (1.95)      1.7772 (1.68)      1.4724 (1.77)     0.0525 (1.37)      1.4636 (1.77)     0.0596 (1.18)         54;6    679.1737 (0.56)        232           1
test_benchmark_query10     24.5189 (34.65)    26.1044 (24.74)    25.2219 (30.36)    0.3681 (9.62)     25.2228 (30.42)    0.5567 (11.05)        13;0     39.6482 (0.03)         39           1
test_benchmark_query11      2.7414 (3.87)      4.0430 (3.83)      3.2230 (3.88)     0.2305 (6.02)      3.2217 (3.89)     0.2930 (5.82)         75;2    310.2685 (0.26)        229           1
test_benchmark_query12     17.1755 (24.27)    18.3755 (17.41)    17.7318 (21.35)    0.2483 (6.49)     17.6685 (21.31)    0.2672 (5.30)         14;4     56.3959 (0.05)         58           1
test_benchmark_query13      8.3278 (11.77)     9.9391 (9.42)      8.6597 (10.43)    0.2016 (5.27)      8.6350 (10.41)    0.2352 (4.67)         22;1    115.4775 (0.10)        110           1
test_benchmark_query14      2.5917 (3.66)      3.2937 (3.12)      2.7956 (3.37)     0.1052 (2.75)      2.7737 (3.34)     0.1003 (1.99)        61;17    357.7025 (0.30)        318           1
test_benchmark_query15      2.1405 (3.02)      2.8080 (2.66)      2.2776 (2.74)     0.0736 (1.92)      2.2651 (2.73)     0.0787 (1.56)        84;13    439.0494 (0.36)        403           1
test_benchmark_query16      3.9124 (5.53)      5.2454 (4.97)      4.2132 (5.07)     0.1204 (3.14)      4.2151 (5.08)     0.1292 (2.56)         45;3    237.3504 (0.20)        234           1
test_benchmark_query17      2.4369 (3.44)      2.9189 (2.77)      2.5873 (3.11)     0.0704 (1.84)      2.5801 (3.11)     0.0785 (1.56)        82;13    386.5047 (0.32)        339           1
test_benchmark_query18      1.8815 (2.66)      2.3139 (2.19)      2.0325 (2.45)     0.0625 (1.63)      2.0295 (2.45)     0.0839 (1.67)        131;7    492.0153 (0.41)        455           1
test_benchmark_query19     18.4428 (26.06)    19.6378 (18.61)    18.9444 (22.81)    0.2700 (7.05)     18.9392 (22.84)    0.3506 (6.96)         16;1     52.7861 (0.04)         52           1
test_benchmark_query2       2.0713 (2.93)      2.5650 (2.43)      2.3162 (2.79)     0.0817 (2.13)      2.3036 (2.78)     0.1143 (2.27)        121;2    431.7420 (0.36)        389           1
test_benchmark_query20      2.1316 (3.01)      2.9786 (2.82)      2.3992 (2.89)     0.1500 (3.92)      2.3583 (2.84)     0.1600 (3.18)        85;26    416.8106 (0.35)        368           1
test_benchmark_query21      1.5456 (2.18)      1.9687 (1.87)      1.6820 (2.02)     0.0697 (1.82)      1.6773 (2.02)     0.0838 (1.66)       103;14    594.5466 (0.49)        398           1
test_benchmark_query22     12.1362 (17.15)    12.8211 (12.15)    12.4648 (15.01)    0.1629 (4.26)     12.4526 (15.02)    0.2373 (4.71)         26;0     80.2257 (0.07)         78           1
test_benchmark_query23      2.4816 (3.51)      3.0308 (2.87)      2.6765 (3.22)     0.0765 (2.00)      2.6667 (3.22)     0.0908 (1.80)         96;7    373.6270 (0.31)        332           1
test_benchmark_query24      1.8614 (2.63)      2.2366 (2.12)      1.9925 (2.40)     0.0576 (1.50)      1.9885 (2.40)     0.0692 (1.37)        124;9    501.8797 (0.42)        440           1
test_benchmark_query25      1.3479 (1.90)      1.8455 (1.75)      1.4648 (1.76)     0.0465 (1.22)      1.4623 (1.76)     0.0584 (1.16)        175;4    682.6643 (0.57)        611           1
test_benchmark_query26      2.8840 (4.08)      3.4320 (3.25)      3.1116 (3.75)     0.0938 (2.45)      3.1040 (3.74)     0.1305 (2.59)         93;3    321.3729 (0.27)        316           1
test_benchmark_query27     22.4221 (31.69)    23.7178 (22.48)    23.0234 (27.72)    0.3001 (7.84)     23.0482 (27.79)    0.4085 (8.11)         14;0     43.4341 (0.04)         42           1
test_benchmark_query28      2.6678 (3.77)      3.3802 (3.20)      2.8639 (3.45)     0.1074 (2.80)      2.8417 (3.43)     0.1035 (2.05)        62;16    349.1685 (0.29)        301           1
test_benchmark_query29      2.4007 (3.39)      3.0182 (2.86)      2.6757 (3.22)     0.0928 (2.42)      2.6743 (3.23)     0.1226 (2.43)        118;5    373.7399 (0.31)        383           1
test_benchmark_query3       3.1942 (4.51)      3.8568 (3.65)      3.3852 (4.08)     0.0895 (2.34)      3.3731 (4.07)     0.1102 (2.19)         75;4    295.4012 (0.25)        255           1
test_benchmark_query4       2.8370 (4.01)      3.6791 (3.49)      3.1119 (3.75)     0.1136 (2.97)      3.1145 (3.76)     0.1467 (2.91)         96;2    321.3500 (0.27)        309           1
test_benchmark_query5       1.8461 (2.61)      2.3278 (2.21)      2.1083 (2.54)     0.0858 (2.24)      2.1098 (2.54)     0.1243 (2.47)        128;3    474.3254 (0.39)        403           1
test_benchmark_query6       0.7076 (1.0)       1.0553 (1.0)       0.8306 (1.0)      0.0383 (1.0)       0.8292 (1.0)      0.0504 (1.0)        273;10  1,203.8883 (1.0)         944           1
test_benchmark_query7      12.7920 (18.08)    13.8115 (13.09)    13.2379 (15.94)    0.2134 (5.57)     13.1735 (15.89)    0.2816 (5.59)         21;1     75.5406 (0.06)         73           1
test_benchmark_query8       1.1148 (1.58)      1.4808 (1.40)      1.2611 (1.52)     0.0517 (1.35)      1.2631 (1.52)     0.0723 (1.44)        191;4    792.9300 (0.66)        602           1
test_benchmark_query9       2.0159 (2.85)      2.5005 (2.37)      2.1874 (2.63)     0.0663 (1.73)      2.1839 (2.63)     0.0880 (1.75)        127;3    457.1621 (0.38)        423           1
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
============================================ 29 passed in 27.64s =============================================
```