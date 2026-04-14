# Ladybug 

This section describes how benchmark the social network data in [Ladybug](https://github.com/LadybugDB/ladybug), a fork of Kuzu.

All timing numbers shown below are on an M3 Macbook Pro with 32 GB of RAM.

## Setup

Because Ladybug is an embedded graph database, the database is tightly coupled with the application layer -- there is no server to set up and run. Simply install the Ladybug Python library (`uv add ladybug`) and you're good to go!

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
========================================== test session starts ===========================================
platform darwin -- Python 3.13.7, pytest-9.0.2, pluggy-1.6.0
benchmark: 5.2.3 (defaults: timer=time.perf_counter disable_gc=True min_rounds=5 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=5)
rootdir: /Users/prrao/code/graph-benchmark-ldbc-snb
configfile: pyproject.toml
plugins: anyio-4.12.1, benchmark-5.2.3, asyncio-1.3.0, Faker-40.1.2
asyncio: mode=Mode.STRICT, debug=False, asyncio_default_fixture_loop_scope=None, asyncio_default_test_loop_scope=function
collected 30 items                                                                                       

benchmark_query.py ..............................                                                  [100%]


-------------------------------------------------------------------------------------- benchmark: 30 tests --------------------------------------------------------------------------------------
Name (time in ms)               Min                 Max                Mean            StdDev              Median               IQR            Outliers         OPS            Rounds  Iterations
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1        3.0234 (4.14)       3.9659 (3.07)       3.3937 (3.85)     0.2844 (3.49)       3.3692 (3.88)     0.3192 (3.79)          2;0    294.6645 (0.26)          9           1
test_benchmark_query10       2.6595 (3.64)       6.0708 (4.69)       3.0729 (3.48)     0.4235 (5.20)       3.0043 (3.46)     0.1836 (2.18)          6;8    325.4244 (0.29)        162           1
test_benchmark_query11      13.1129 (17.94)     14.3267 (11.08)     13.5912 (15.41)    0.3273 (4.02)      13.5408 (15.59)    0.4839 (5.74)         19;0     73.5768 (0.06)         53           1
test_benchmark_query12      21.5917 (29.54)     33.3258 (25.76)     23.9139 (27.12)    3.0918 (37.96)     23.3204 (26.84)    2.5121 (29.79)         1;1     41.8167 (0.04)         13           1
test_benchmark_query13      80.1320 (109.64)    87.7738 (67.86)     82.7719 (93.87)    2.1246 (26.08)     82.3994 (94.85)    2.5054 (29.71)         3;1     12.0814 (0.01)         12           1
test_benchmark_query14       2.5647 (3.51)       3.2345 (2.50)       2.8343 (3.21)     0.1428 (1.75)       2.8255 (3.25)     0.1910 (2.26)         51;1    352.8201 (0.31)        152           1
test_benchmark_query15       4.0946 (5.60)       8.4542 (6.54)       4.4479 (5.04)     0.4277 (5.25)       4.3610 (5.02)     0.2560 (3.04)          4;4    224.8257 (0.20)        193           1
test_benchmark_query16       2.9930 (4.10)       3.5198 (2.72)       3.2053 (3.63)     0.1145 (1.41)       3.1735 (3.65)     0.1341 (1.59)         27;4    311.9852 (0.28)         96           1
test_benchmark_query17       4.5425 (6.22)       5.2249 (4.04)       4.8221 (5.47)     0.1594 (1.96)       4.8153 (5.54)     0.2308 (2.74)         59;0    207.3784 (0.18)        149           1
test_benchmark_query18       2.5818 (3.53)       3.1187 (2.41)       2.8207 (3.20)     0.1025 (1.26)       2.8135 (3.24)     0.1228 (1.46)         88;8    354.5266 (0.31)        312           1
test_benchmark_query19      13.4189 (18.36)     32.1594 (24.86)     22.5003 (25.52)    5.0237 (61.68)     22.8369 (26.29)    8.0629 (95.62)        18;0     44.4439 (0.04)         40           1
test_benchmark_query2        2.1123 (2.89)       2.6154 (2.02)       2.2932 (2.60)     0.1092 (1.34)       2.2894 (2.64)     0.1280 (1.52)         19;3    436.0720 (0.38)         56           1
test_benchmark_query20      16.3570 (22.38)     18.1098 (14.00)     16.9682 (19.24)    0.6390 (7.85)      16.6608 (19.18)    1.0797 (12.80)         6;0     58.9338 (0.05)         26           1
test_benchmark_query21       0.7309 (1.0)        1.2935 (1.0)        0.8818 (1.0)      0.0814 (1.0)        0.8687 (1.0)      0.0843 (1.0)          54;6  1,134.0637 (1.0)         227           1
test_benchmark_query22      20.6849 (28.30)     39.1292 (30.25)     26.6268 (30.20)    4.2332 (51.97)     25.4376 (29.28)    4.0505 (48.04)         9;2     37.5561 (0.03)         34           1
test_benchmark_query23       2.0010 (2.74)       3.0792 (2.38)       2.2692 (2.57)     0.1476 (1.81)       2.2322 (2.57)     0.1785 (2.12)       103;10    440.6932 (0.39)        383           1
test_benchmark_query24       2.0752 (2.84)       2.8370 (2.19)       2.3737 (2.69)     0.1212 (1.49)       2.3668 (2.72)     0.1370 (1.63)        86;10    421.2914 (0.37)        293           1
test_benchmark_query25       2.5252 (3.45)       6.1159 (4.73)       2.8515 (3.23)     0.3211 (3.94)       2.7977 (3.22)     0.2685 (3.18)         15;6    350.6910 (0.31)        227           1
test_benchmark_query26       5.6369 (7.71)       6.4632 (5.00)       5.9069 (6.70)     0.1620 (1.99)       5.8720 (6.76)     0.2163 (2.56)         45;3    169.2929 (0.15)        153           1
test_benchmark_query27      17.5315 (23.99)     34.7628 (26.87)     23.9641 (27.18)    3.8147 (46.83)     22.4596 (25.85)    5.9659 (70.75)        19;0     41.7291 (0.04)         51           1
test_benchmark_query28       2.6746 (3.66)      18.0291 (13.94)      3.4840 (3.95)     1.4935 (18.34)      2.9726 (3.42)     0.6873 (8.15)        18;29    287.0259 (0.25)        320           1
test_benchmark_query29       2.0710 (2.83)       2.8755 (2.22)       2.3416 (2.66)     0.1492 (1.83)       2.3324 (2.68)     0.1712 (2.03)       103;11    427.0496 (0.38)        340           1
test_benchmark_query3        1.7771 (2.43)       2.5128 (1.94)       1.9615 (2.22)     0.1584 (1.95)       1.8997 (2.19)     0.1517 (1.80)        36;14    509.8192 (0.45)        173           1
test_benchmark_query30     223.7446 (306.13)   243.6210 (188.34)   231.9245 (263.02)   7.3280 (89.97)    230.6737 (265.54)   7.7645 (92.08)         2;0      4.3117 (0.00)          5           1
test_benchmark_query4        1.4683 (2.01)       2.0780 (1.61)       1.6241 (1.84)     0.1256 (1.54)       1.5825 (1.82)     0.1140 (1.35)        40;22    615.7165 (0.54)        222           1
test_benchmark_query5        5.5479 (7.59)       6.8109 (5.27)       5.9142 (6.71)     0.2504 (3.07)       5.8875 (6.78)     0.3706 (4.40)         24;1    169.0858 (0.15)         79           1
test_benchmark_query6        1.1066 (1.51)       1.8732 (1.45)       1.3703 (1.55)     0.1984 (2.44)       1.2929 (1.49)     0.3002 (3.56)        106;0    729.7568 (0.64)        328           1
test_benchmark_query7       48.4568 (66.30)     51.8210 (40.06)     50.1362 (56.86)    0.8902 (10.93)     50.2581 (57.85)    1.4158 (16.79)         6;0     19.9457 (0.02)         18           1
test_benchmark_query8        4.2824 (5.86)       4.8813 (3.77)       4.5364 (5.14)     0.1439 (1.77)       4.5226 (5.21)     0.2100 (2.49)         33;0    220.4397 (0.19)        109           1
test_benchmark_query9        3.1212 (4.27)       4.1108 (3.18)       3.4515 (3.91)     0.2212 (2.72)       3.4044 (3.92)     0.3021 (3.58)         56;1    289.7257 (0.26)        160           1
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
========================================== 30 passed in 21.94s ===========================================
```
