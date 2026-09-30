# Ladybug

This section describes how benchmark the social network data in [Ladybug](https://github.com/LadybugDB/ladybug), a fork of Kuzu.

Latest measurements (2026-09-30) are on an Apple M5 with 24 GiB RAM. See the [comparison table](../README.md#mean-query-latency) and the [0.21.1 benchmark output](../results/ladybug-0.21.1.txt).

## Setup

Because Ladybug is an embedded graph database, the database is tightly coupled with the application layer -- there is no server to set up and run. The project pins Ladybug to `0.21.1`; run `uv sync --frozen` from the repository root to install the benchmark dependencies.

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

All queries use plain `conn.execute(query)` without dummy parameters or cache-setting calls. Run from `ladybugdb/`:

```sh
uv run --frozen pytest benchmark_query.py \
  --benchmark-min-rounds=5 \
  --benchmark-min-time=0.000005 \
  --benchmark-max-time=1.0 \
  --benchmark-timer=time.perf_counter \
  --benchmark-calibration-precision=10 \
  --benchmark-warmup=off \
  --benchmark-warmup-iterations=5 \
  --benchmark-disable-gc \
  --benchmark-sort=fullname
```

Latest CLI output: [Ladybug 0.21.1](../results/ladybug-0.21.1.txt). The [raw benchmark JSON](../results/ladybug-0.21.1.json) includes per-round timings. All 30 query assertions passed. The [0.21.0 output](../results/archived/ladybug-0.21.0.txt) remains available for comparison.
