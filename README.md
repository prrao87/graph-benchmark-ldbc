# Graph benchmarks: LDBC SNB SF1

This repo contains LDBC Social Network Benchmarks (SNB) with a scale factor of 1 (SF1). The dataset is downloaded from [the official source](https://ldbcouncil.org/benchmarks/snb/datasets/).

The aim of this benchmark is to study and compare the performance of graph systems on an established benchmark dataset. The queries suite run consists of 30 queries that touch various nodes via n-hop path traversals, with very different cardinalities, filters and projections applied to get a more holistic understanding of query performance.

The following systems are compared:
- Neo4j
- Kuzu (now archived)
- Ladybug
- lance-graph

## Setup

We use [uv](https://docs.astral.sh/uv/getting-started/installation/) to manage the dependencies.

```sh
# Sync the dependencies locally
uv sync
```
All the dependencies are listed in `pyproject.toml`.

## Dataset

Download the LDBC dataset locally by running the given Python script `download_dataset.py`

```bash
uv run download_dataset.py
```

Alternatively, navigate to the [LDBC site](https://ldbcouncil.org/benchmarks/snb/datasets/) and manually download and unzip the dataset from [this URL](https://datasets.ldbcouncil.org/snb-interactive-v1/social_network-sf1-CsvComposite-StringDateFormatter.tar.zst).

The schema of the LDBC SNB graph is shown below. There are 8 node types and 23 relationship types in the graph.

![](assets/ldbc-snb-schema.png)

The individual and total number of nodes and relationships in the graph asre shown below.

```
Node counts:
- Comment: 2052169
- Forum: 90492
- Organisation: 7955
- Person: 9892
- Place: 1460
- Post: 1003605
- Tag: 16080
- Tagclass: 71

Relationship counts:
- commentHasCreator: 2052169
- commentHasTag: 2698393
- commentIsLocatedIn: 2052169
- containerOf: 1003605
- forumHasTag: 309766
- hasInterest: 229166
- hasMember: 1611869
- hasModerator: 90492
- hasType: 16080
- isPartOf: 1454
- isSubclassOf: 70
- knows: 180623
- likeComment: 1438418
- likePost: 751677
- organisationIsLocatedIn: 7955
- personIsLocatedIn: 9892
- postHasCreator: 1003605
- postHasTag: 713258
- postIsLocatedIn: 1003605
- replyOfComment: 1040749
- replyOfPost: 1011420
- studyAt: 7949
- workAt: 21654

Totals:
- nodes: 3181724
- relationships: 17256038
```

In total, there are 3.1M nodes and 17M relationships for the SF1 variant of this dataset.

## Ingest the data as a graph

Navigate to the individual directories to see the instructions on how to ingest the data into each graph system.
Once constructed, the graph is well-connected and has rich relationships between nodes of different types.

![](assets/./ldbc-snb-sf1-graph.png)

## Queries

Navigate to each directory and see the `query.py` files for
each of the 30 queries run in the benchmark.

## High-level results

| Query | neo4j-2025.12.1 (ms) | kuzu-0.11.3 (ms) | ladybug-0.14.1 (ms) | lance-graph-0.5.1 (ms) |
| --- | --- | --- | --- | --- |
| q1 | 5.7ms | 2.4ms (2.4x) | 2.1ms (2.7x) | 1.5ms (3.9x) |
| q2 | 6.4ms | 1.5ms (4.4x) | 1.5ms (4.2x) | 2.3ms (2.8x) |
| q3 | 2.8ms | 1.4ms (2.1x) | 1.2ms (2.2x) | 3.4ms (0.8x) |
| q4 | 4.8ms | 1.1ms (4.6x) | 1.1ms (4.3x) | 3.1ms (1.5x) |
| q5 | 5.1ms | 3.9ms (1.3x) | 3.6ms (1.4x) | 2.1ms (2.4x) |
| q6 | 4.1ms | 0.8ms (5.1x) | 0.9ms (4.6x) | 0.8ms (5.0x) |
| q7 | 2.2ms | 31.0ms (0.1x) | 31.1ms (0.1x) | 13.2ms (0.2x) |
| q8 | 15.6ms | 2.8ms (5.6x) | 2.8ms (5.5x) | 1.3ms (12.4x) |
| q9 | 2.6ms | 2.0ms (1.3x) | 2.2ms (1.2x) | 2.2ms (1.2x) |
| q10 | 4.9ms | 1.8ms (2.7x) | 1.9ms (2.6x) | 25.2ms (0.2x) |
| q11 | 12.9ms | 8.1ms (1.6x) | 8.4ms (1.5x) | 3.2ms (4.0x) |
| q12 | 4.7ms | 18.4ms (0.3x) | 17.9ms (0.3x) | 17.7ms (0.3x) |
| q13 | 8.8ms | 50.2ms (0.2x) | 51.6ms (0.2x) | 8.7ms (1.0x) |
| q14 | 1.6ms | 1.7ms (1.0x) | 1.8ms (0.9x) | 2.8ms (0.6x) |
| q15 | 3.2ms | 2.7ms (1.2x) | 2.7ms (1.2x) | 2.3ms (1.4x) |
| q16 | 2.0ms | 2.0ms (1.0x) | 2.0ms (1.0x) | 4.2ms (0.5x) |
| q17 | 5.3ms | 3.0ms (1.8x) | 2.9ms (1.8x) | 2.6ms (2.0x) |
| q18 | 4.1ms | 1.7ms (2.3x) | 1.8ms (2.3x) | 2.0ms (2.0x) |
| q19 | 6.1ms | 13.3ms (0.5x) | 13.6ms (0.5x) | 18.9ms (0.3x) |
| q20 | 421.8ms | 12.0ms (35.2x) | 11.1ms (38.1x) | 2.4ms (175.8x) |
| q21 | 1.6ms | 0.6ms (2.7x) | 0.6ms (2.7x) | 1.7ms (0.9x) |
| q22 | 3.7ms | 19.8ms (0.2x) | 21.2ms (0.2x) | 12.5ms (0.3x) |
| q23 | 4.0ms | 1.4ms (2.9x) | 1.4ms (2.8x) | 2.7ms (1.5x) |
| q24 | 1.5ms | 1.5ms (1.0x) | 1.6ms (1.0x) | 2.0ms (0.8x) |
| q25 | 3.1ms | 1.6ms (1.9x) | 1.7ms (1.8x) | 1.5ms (2.1x) |
| q26 | 1.8ms | 3.6ms (0.5x) | 3.8ms (0.5x) | 3.1ms (0.6x) |
| q27 | 3.4ms | 13.4ms (0.3x) | 16.0ms (0.2x) | 23.0ms (0.1x) |
| q28 | 4.3ms | 1.6ms (2.6x) | 1.9ms (2.3x) | 2.9ms (1.5x) |
| q29 | 3.1ms | 1.3ms (2.4x) | 1.3ms (2.4x) | 2.7ms (1.2x) |

> [!NOTE]
> Q30 (self-referencing pattern) is the most expensive query in this benchmark and is currently not supported in lance-graph. The lance-graph query processor is undergoing significant optimization -- follow this repo and check back in soon!

The fastest and slowest systems for each query can be visualized via the following heatmap.

![](./results/benchmark_heatmap.png)
