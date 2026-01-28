"""
Script to count total number of node/relationship tables,
and the total number of nodes and relationships PER table,
in the Ladybug database.
"""
from __future__ import annotations

from pathlib import Path

import real_ladybug as lb
from real_ladybug import Connection

DB_NAME = "ldbc_snb_sf1.lbdb"
DB_PATH = Path(__file__).with_name(DB_NAME)


def _execute(conn: Connection, query: str):
    response = conn.execute(query)
    return response.get_as_pl()  # type: ignore


def _show_tables(conn: Connection):
    last_exc: Exception | None = None
    for query in ("CALL show_tables() RETURN *;", "CALL show_tables()", "SHOW TABLES"):
        try:
            return _execute(conn, query)
        except Exception as exc:  # noqa: BLE001 - keep fallback flexible
            last_exc = exc
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Unable to fetch table metadata.")


def _column_for_keyword(df, keyword: str) -> str:
    matches = [col for col in df.columns if keyword in col.lower()]
    if not matches:
        raise KeyError(f"Missing column containing '{keyword}' in {df.columns}")
    return matches[0]


def _table_names_by_type(conn: Connection) -> tuple[list[str], list[str]]:
    tables = _show_tables(conn)
    name_col = _column_for_keyword(tables, "name")
    type_col = _column_for_keyword(tables, "type")
    node_tables: list[str] = []
    rel_tables: list[str] = []
    for row in tables.to_dicts():
        table_name = row[name_col]
        table_type = str(row[type_col]).upper()
        if table_type.startswith("NODE"):
            node_tables.append(table_name)
        elif table_type.startswith("REL"):
            rel_tables.append(table_name)
    return node_tables, rel_tables


def _count_nodes(conn: Connection, label: str) -> int:
    query = f"""
        MATCH (n:{label})
        RETURN COUNT(n) AS count
    """
    df = _execute(conn, query)
    return int(df["count"][0])


def _count_relationships(conn: Connection, rel_type: str) -> int:
    query = f"""
        MATCH ()-[r:{rel_type}]->()
        RETURN COUNT(r) AS count
    """
    df = _execute(conn, query)
    return int(df["count"][0])


def main() -> None:
    db = lb.Database(str(DB_PATH))
    conn = lb.Connection(db)

    node_tables, rel_tables = _table_names_by_type(conn)
    print(f"Node tables: {len(node_tables)}")
    print(f"Relationship tables: {len(rel_tables)}")

    total_nodes = 0
    print("\nNode counts:")
    for label in sorted(node_tables):
        count = _count_nodes(conn, label)
        total_nodes += count
        print(f"- {label}: {count}")

    total_rels = 0
    print("\nRelationship counts:")
    for rel_type in sorted(rel_tables):
        count = _count_relationships(conn, rel_type)
        total_rels += count
        print(f"- {rel_type}: {count}")

    print("\nTotals:")
    print(f"- nodes: {total_nodes}")
    print(f"- relationships: {total_rels}")


if __name__ == "__main__":
    main()
