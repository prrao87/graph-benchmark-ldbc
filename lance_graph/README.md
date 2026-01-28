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

The benchmark can be run using the following command. The results are output to a table that can be programmatically parsed for timing comparisons with other systems.

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

benchmark_query.py .............................                                             [100%]


---------------------------------------------------------------------------------- benchmark: 29 tests -----------------------------------------------------------------------------------
Name (time in ms)              Min                Max               Mean            StdDev             Median               IQR            Outliers      OPS            Rounds  Iterations
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_query1      23.4846 (1.06)     38.8126 (1.61)     25.5734 (1.12)     4.2298 (16.29)    24.5791 (1.08)     1.1770 (3.39)          1;1  39.1031 (0.90)         12           1
test_benchmark_query10     46.6241 (2.11)     48.9402 (2.03)     47.9140 (2.09)     0.7171 (2.76)     47.8626 (2.10)     1.0682 (3.08)          8;0  20.8707 (0.48)         21           1
test_benchmark_query11     23.0027 (1.04)     24.3831 (1.01)     23.4581 (1.02)     0.2849 (1.10)     23.4306 (1.03)     0.3468 (1.0)           9;2  42.6292 (0.98)         40           1
test_benchmark_query12     39.2382 (1.77)     40.6928 (1.69)     39.9012 (1.74)     0.3480 (1.34)     39.8082 (1.75)     0.5115 (1.48)          6;0  25.0619 (0.57)         24           1
test_benchmark_query13     30.6150 (1.38)     32.7346 (1.36)     31.7886 (1.39)     0.5748 (2.21)     31.8949 (1.40)     0.9641 (2.78)         12;0  31.4578 (0.72)         32           1
test_benchmark_query14     24.5252 (1.11)     39.0200 (1.62)     25.7210 (1.12)     2.2185 (8.55)     25.3502 (1.11)     0.5651 (1.63)          1;1  38.8788 (0.89)         39           1
test_benchmark_query15     24.2449 (1.10)     25.9758 (1.08)     24.8134 (1.08)     0.4411 (1.70)     24.6653 (1.08)     0.5361 (1.55)         11;1  40.3007 (0.92)         40           1
test_benchmark_query16     25.8480 (1.17)     27.3742 (1.14)     26.4944 (1.16)     0.3611 (1.39)     26.4281 (1.16)     0.3823 (1.10)          9;3  37.7438 (0.86)         37           1
test_benchmark_query17     24.3151 (1.10)     26.3104 (1.09)     24.9832 (1.09)     0.5422 (2.09)     24.7432 (1.09)     0.6777 (1.95)         10;0  40.0269 (0.92)         40           1
test_benchmark_query18     23.7343 (1.07)     26.0725 (1.08)     24.3377 (1.06)     0.5415 (2.09)     24.1614 (1.06)     0.5962 (1.72)          7;2  41.0885 (0.94)         41           1
test_benchmark_query19     40.9422 (1.85)     42.8752 (1.78)     41.6059 (1.82)     0.4927 (1.90)     41.5750 (1.83)     0.6976 (2.01)          8;0  24.0351 (0.55)         24           1
test_benchmark_query2      23.6561 (1.07)     25.3237 (1.05)     24.2468 (1.06)     0.4517 (1.74)     24.1904 (1.06)     0.5372 (1.55)         11;1  41.2425 (0.94)         40           1
test_benchmark_query20     23.9992 (1.09)     25.5825 (1.06)     24.7242 (1.08)     0.3379 (1.30)     24.7282 (1.09)     0.3843 (1.11)         10;2  40.4461 (0.93)         40           1
test_benchmark_query21     23.2746 (1.05)     25.1152 (1.04)     23.9025 (1.04)     0.4492 (1.73)     23.8426 (1.05)     0.5324 (1.54)         11;2  41.8366 (0.96)         41           1
test_benchmark_query22     34.7201 (1.57)     35.5743 (1.48)     35.0635 (1.53)     0.2596 (1.0)      34.9980 (1.54)     0.3801 (1.10)         10;0  28.5197 (0.65)         28           1
test_benchmark_query23     24.7312 (1.12)     26.7325 (1.11)     25.3844 (1.11)     0.4766 (1.84)     25.2731 (1.11)     0.7105 (2.05)         14;0  39.3943 (0.90)         40           1
test_benchmark_query24     23.7641 (1.07)     25.2807 (1.05)     24.2942 (1.06)     0.3224 (1.24)     24.2794 (1.07)     0.4023 (1.16)         13;1  41.1621 (0.94)         39           1
test_benchmark_query25     23.2307 (1.05)     25.4696 (1.06)     24.0120 (1.05)     0.4853 (1.87)     23.9899 (1.05)     0.5723 (1.65)         12;1  41.6459 (0.95)         42           1
test_benchmark_query26     25.1670 (1.14)     40.1705 (1.67)     26.1492 (1.14)     2.3056 (8.88)     25.7182 (1.13)     0.5946 (1.71)          1;1  38.2420 (0.88)         40           1
test_benchmark_query27     45.1262 (2.04)     47.4365 (1.97)     46.0750 (2.01)     0.6334 (2.44)     45.9520 (2.02)     0.7821 (2.26)          7;0  21.7037 (0.50)         22           1
test_benchmark_query28     24.7007 (1.12)     30.2612 (1.26)     25.4705 (1.11)     0.9218 (3.55)     25.3048 (1.11)     0.5957 (1.72)          3;3  39.2611 (0.90)         39           1
test_benchmark_query29     24.8784 (1.12)     26.0012 (1.08)     25.2495 (1.10)     0.2748 (1.06)     25.1882 (1.11)     0.3666 (1.06)         13;1  39.6047 (0.91)         39           1
test_benchmark_query3      24.5413 (1.11)     26.4070 (1.10)     25.1033 (1.10)     0.3933 (1.52)     24.9886 (1.10)     0.4108 (1.18)          9;2  39.8355 (0.91)         39           1
test_benchmark_query4      24.1466 (1.09)     25.3973 (1.06)     24.6867 (1.08)     0.2857 (1.10)     24.6829 (1.08)     0.4056 (1.17)         11;0  40.5077 (0.93)         40           1
test_benchmark_query5      23.1838 (1.05)     38.5288 (1.60)     24.4770 (1.07)     2.3598 (9.09)     24.0340 (1.06)     1.0793 (3.11)          1;1  40.8546 (0.94)         41           1
test_benchmark_query6      22.1156 (1.0)      24.4153 (1.01)     22.9032 (1.0)      0.5169 (1.99)     22.7616 (1.0)      0.5764 (1.66)         11;3  43.6621 (1.0)          44           1
test_benchmark_query7      34.7729 (1.57)     37.8821 (1.57)     35.5221 (1.55)     0.5948 (2.29)     35.4193 (1.56)     0.5333 (1.54)          5;1  28.1515 (0.64)         29           1
test_benchmark_query8      22.5475 (1.02)     24.0550 (1.0)      23.1256 (1.01)     0.4153 (1.60)     23.0535 (1.01)     0.5908 (1.70)         14;0  43.2422 (0.99)         41           1
test_benchmark_query9      23.3486 (1.06)     26.0487 (1.08)     24.1805 (1.06)     0.6710 (2.59)     23.9170 (1.05)     1.0247 (2.96)         13;0  41.3556 (0.95)         41           1
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
======================================= 29 passed in 31.34s ========================================
```