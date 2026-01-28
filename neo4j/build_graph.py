import argparse
import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import polars as pl
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase, AsyncManagedTransaction, AsyncSession

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[1]
CSV_ROOT = REPO_ROOT / "csv"
DYNAMIC_ROOT = CSV_ROOT / "dynamic"
STATIC_ROOT = CSV_ROOT / "static"

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

JsonBlob = dict[str, Any]


@dataclass(frozen=True)
class EdgeSpec:
    path: Path
    src_label: str
    rel_type: str
    dst_label: str


NODE_FILES: dict[str, Path] = {
    "Comment": DYNAMIC_ROOT / "comment_0_0.csv",
    "Forum": DYNAMIC_ROOT / "forum_0_0.csv",
    "Person": DYNAMIC_ROOT / "person_0_0.csv",
    "Post": DYNAMIC_ROOT / "post_0_0.csv",
    "Organisation": STATIC_ROOT / "organisation_0_0.csv",
    "Place": STATIC_ROOT / "place_0_0.csv",
    "Tag": STATIC_ROOT / "tag_0_0.csv",
    "Tagclass": STATIC_ROOT / "tagclass_0_0.csv",
}

EDGE_SPECS: list[EdgeSpec] = [
    EdgeSpec(DYNAMIC_ROOT / "forum_containerOf_post_0_0.csv", "Forum", "containerOf", "Post"),
    EdgeSpec(DYNAMIC_ROOT / "comment_hasCreator_person_0_0.csv", "Comment", "commentHasCreator", "Person"),
    EdgeSpec(DYNAMIC_ROOT / "post_hasCreator_person_0_0.csv", "Post", "postHasCreator", "Person"),
    EdgeSpec(DYNAMIC_ROOT / "person_hasInterest_tag_0_0.csv", "Person", "hasInterest", "Tag"),
    EdgeSpec(DYNAMIC_ROOT / "forum_hasMember_person_0_0.csv", "Forum", "hasMember", "Person"),
    EdgeSpec(DYNAMIC_ROOT / "forum_hasModerator_person_0_0.csv", "Forum", "hasModerator", "Person"),
    EdgeSpec(DYNAMIC_ROOT / "comment_hasTag_tag_0_0.csv", "Comment", "commentHasTag", "Tag"),
    EdgeSpec(DYNAMIC_ROOT / "forum_hasTag_tag_0_0.csv", "Forum", "forumHasTag", "Tag"),
    EdgeSpec(DYNAMIC_ROOT / "post_hasTag_tag_0_0.csv", "Post", "postHasTag", "Tag"),
    EdgeSpec(STATIC_ROOT / "tag_hasType_tagclass_0_0.csv", "Tag", "hasType", "Tagclass"),
    EdgeSpec(DYNAMIC_ROOT / "comment_isLocatedIn_place_0_0.csv", "Comment", "commentIsLocatedIn", "Place"),
    EdgeSpec(STATIC_ROOT / "organisation_isLocatedIn_place_0_0.csv", "Organisation", "organisationIsLocatedIn", "Place"),
    EdgeSpec(DYNAMIC_ROOT / "person_isLocatedIn_place_0_0.csv", "Person", "personIsLocatedIn", "Place"),
    EdgeSpec(DYNAMIC_ROOT / "post_isLocatedIn_place_0_0.csv", "Post", "postIsLocatedIn", "Place"),
    EdgeSpec(STATIC_ROOT / "place_isPartOf_place_0_0.csv", "Place", "isPartOf", "Place"),
    EdgeSpec(STATIC_ROOT / "tagclass_isSubclassOf_tagclass_0_0.csv", "Tagclass", "isSubclassOf", "Tagclass"),
    EdgeSpec(DYNAMIC_ROOT / "person_knows_person_0_0.csv", "Person", "knows", "Person"),
    EdgeSpec(DYNAMIC_ROOT / "person_likes_comment_0_0.csv", "Person", "likeComment", "Comment"),
    EdgeSpec(DYNAMIC_ROOT / "person_likes_post_0_0.csv", "Person", "likePost", "Post"),
    EdgeSpec(DYNAMIC_ROOT / "comment_replyOf_comment_0_0.csv", "Comment", "replyOfComment", "Comment"),
    EdgeSpec(DYNAMIC_ROOT / "comment_replyOf_post_0_0.csv", "Comment", "replyOfPost", "Post"),
    EdgeSpec(DYNAMIC_ROOT / "person_studyAt_organisation_0_0.csv", "Person", "studyAt", "Organisation"),
    EdgeSpec(DYNAMIC_ROOT / "person_workAt_organisation_0_0.csv", "Person", "workAt", "Organisation"),
]


def _iter_batches(rows: list[JsonBlob], batch_size: int) -> Iterable[list[JsonBlob]]:
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]


def _load_csv(path: Path) -> pl.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing CSV: {path}")
    return pl.read_csv(path, separator="|")


def _normalize_node_rows(rows: list[JsonBlob]) -> list[JsonBlob]:
    normalized = []
    for row in rows:
        if "id" not in row:
            raise ValueError("Expected 'id' column for node row")
        row = dict(row)
        row["ID"] = row.pop("id")
        normalized.append(row)
    return normalized


def _normalize_edge_rows(rows: list[JsonBlob]) -> list[JsonBlob]:
    normalized = []
    for row in rows:
        row = dict(row)
        keys = list(row.keys())
        if len(keys) < 2:
            raise ValueError("Edge row must have at least 2 columns")
        src_key, dst_key = keys[0], keys[1]
        src = row.pop(src_key)
        dst = row.pop(dst_key)
        normalized.append({"src": src, "dst": dst, "props": row})
    return normalized


async def _merge_nodes(
    tx: AsyncManagedTransaction, label: str, rows: list[JsonBlob]
) -> None:
    query = f"""
        UNWIND $rows AS row
        MERGE (n:{label} {{ID: row.ID}})
            SET n += row
    """
    await tx.run(query, rows=rows)


async def _merge_edges(
    tx: AsyncManagedTransaction,
    src_label: str,
    rel_type: str,
    dst_label: str,
    rows: list[JsonBlob],
) -> None:
    query = f"""
        UNWIND $rows AS row
        MERGE (src:{src_label} {{ID: row.src}})
        MERGE (dst:{dst_label} {{ID: row.dst}})
        MERGE (src)-[r:{rel_type}]->(dst)
            SET r += row.props
    """
    await tx.run(query, rows=rows)


async def create_constraints(session: AsyncSession) -> None:
    labels = list(NODE_FILES.keys())
    for label in labels:
        query = f"CREATE CONSTRAINT {label}_ID IF NOT EXISTS FOR (n:{label}) REQUIRE n.ID IS UNIQUE"
        await session.run(query)


async def write_nodes(session: AsyncSession, batch_size: int) -> None:
    for label, path in NODE_FILES.items():
        df = _load_csv(path)
        rows = _normalize_node_rows(df.to_dicts())
        for batch in _iter_batches(rows, batch_size):
            await session.execute_write(_merge_nodes, label, batch)
        print(f"Loaded {len(rows)} nodes for label {label}")


async def write_edges(session: AsyncSession, batch_size: int) -> None:
    for spec in EDGE_SPECS:
        df = _load_csv(spec.path)
        rows = _normalize_edge_rows(df.to_dicts())
        for batch in _iter_batches(rows, batch_size):
            await session.execute_write(
                _merge_edges, spec.src_label, spec.rel_type, spec.dst_label, batch
            )
        print(f"Loaded {len(rows)} edges for {spec.rel_type}")


async def main(batch_size: int) -> None:
    if NEO4J_USER is None or NEO4J_PASSWORD is None:
        raise EnvironmentError("NEO4J_USER and NEO4J_PASSWORD must be set")

    async with AsyncGraphDatabase.driver(URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        async with driver.session(database=NEO4J_DATABASE) as session:
            await create_constraints(session)

            nodes_start = time.perf_counter()
            await write_nodes(session, batch_size)
            nodes_elapsed = time.perf_counter() - nodes_start
            print(f"Nodes loaded in {nodes_elapsed:.4f}s")

            edges_start = time.perf_counter()
            await write_edges(session, batch_size)
            edges_elapsed = time.perf_counter() - edges_start
            print(f"Edges loaded in {edges_elapsed:.4f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser("Build Neo4j graph from files")
    parser.add_argument(
        "--batch_size",
        "-b",
        type=int,
        default=50_000,
        help="Batch size of rows to ingest at a time",
    )
    args = parser.parse_args()

    asyncio.run(main(args.batch_size))
