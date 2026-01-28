"""
Builds Lance datasets for the LDBC SNB SF1 graph from CSV inputs.

This script reads CSV files under `csv/static` and `csv/dynamic`, converts them
into PyArrow tables, and writes one Lance dataset per node/edge CSV into
`lance_graph/graph_lance`.

Node CSVs are detected by a leading `id` column. Edge CSVs are detected by
having the first two columns in the form `Label.id|Label.id`. Edge endpoints
are normalized to `src`/`dst` and cast to the referenced node id types.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import lance
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv

SCRIPT_ROOT = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_ROOT.parent
CSV_ROOT = REPO_ROOT / "csv"
GRAPH_ROOT = SCRIPT_ROOT / "graph_lance"


def _normalize_label(label: str) -> str:
    label = label.strip()
    if not label:
        raise ValueError("Empty label")
    lower = label.lower()
    return lower[0].upper() + lower[1:]


def _normalize_column(name: str) -> str:
    return name.strip().lower().replace(".", "_")


def _dedupe_names(names: Iterable[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        count = seen.get(name, 0)
        if count == 0:
            out.append(name)
        else:
            out.append(f"{name}_{count}")
        seen[name] = count + 1
    return out


def _read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8") as f:
        header = f.readline().rstrip("\n")
    return header.split("|")


def _read_csv(path: Path, column_names: list[str]) -> pa.Table:
    read_opts = csv.ReadOptions(column_names=column_names, skip_rows=1)
    parse_opts = csv.ParseOptions(delimiter="|")
    return csv.read_csv(str(path), read_options=read_opts, parse_options=parse_opts)


def _assert_no_nulls(arr: pa.Array, where: str) -> None:
    if pc.any(pc.is_null(arr)).as_py():
        raise ValueError(f"Nulls found in {where}")


def _cast_column(table: pa.Table, name: str, typ: pa.DataType) -> pa.Array:
    arr = table[name]
    if arr.type == typ:
        return arr
    return pc.cast(arr, typ)


def _write_lance(table: pa.Table, name: str) -> str:
    GRAPH_ROOT.mkdir(parents=True, exist_ok=True)
    path = GRAPH_ROOT / f"{name}.lance"
    lance.write_dataset(table, str(path), mode="overwrite")
    return str(path)


def _strip_numeric_suffix(stem: str) -> str:
    parts = stem.split("_")
    while parts and parts[-1].isdigit():
        parts.pop()
    return "_".join(parts)


def _node_label_from_stem(stem: str) -> str:
    base = _strip_numeric_suffix(stem)
    return _normalize_label(base)


def _edge_name_from_stem(stem: str) -> str:
    return _strip_numeric_suffix(stem)


def _parse_edge_labels(header: list[str]) -> tuple[str, str] | None:
    if len(header) < 2:
        return None
    left, right = header[0], header[1]
    if "." not in left or "." not in right:
        return None
    left_label = left.split(".", 1)[0]
    right_label = right.split(".", 1)[0]
    if not left_label or not right_label:
        return None
    return _normalize_label(left_label), _normalize_label(right_label)


def _load_node(path: Path, label: str) -> tuple[pa.Table, pa.DataType]:
    header = _read_header(path)
    names = _dedupe_names([_normalize_column(n) for n in header])
    table = _read_csv(path, names)
    if "id" not in table.column_names:
        raise ValueError(f"Missing id column in {path}. Found: {table.column_names}")
    _assert_no_nulls(table["id"], f"{path}:id")
    return table, table.schema.field("id").type


def _load_edge(path: Path, src_type: pa.DataType, dst_type: pa.DataType) -> pa.Table:
    header = _read_header(path)
    normalized = [_normalize_column(n) for n in header]
    if len(normalized) < 2:
        raise ValueError(f"Edge file {path} must have at least 2 columns")
    normalized[0] = "src"
    normalized[1] = "dst"
    names = _dedupe_names(normalized)
    table = _read_csv(path, names)

    src = _cast_column(table, "src", src_type)
    dst = _cast_column(table, "dst", dst_type)
    _assert_no_nulls(src, f"{path}:src")
    _assert_no_nulls(dst, f"{path}:dst")

    cols = []
    for name in table.column_names:
        if name == "src":
            cols.append(src)
        elif name == "dst":
            cols.append(dst)
        else:
            cols.append(table[name])
    return pa.table(cols, names=table.column_names)


def main() -> None:
    if not CSV_ROOT.exists():
        raise FileNotFoundError(f"CSV root not found: {CSV_ROOT}")

    csv_files = sorted(CSV_ROOT.rglob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found under: {CSV_ROOT}")

    node_id_types: dict[str, pa.DataType] = {}
    edge_files: list[Path] = []

    for path in csv_files:
        header = _read_header(path)
        if header and header[0].strip().lower() == "id":
            label = _node_label_from_stem(path.stem)
            table, id_type = _load_node(path, label)
            node_id_types[label] = id_type
            _write_lance(table, label)
        else:
            edge_files.append(path)

    skipped: list[Path] = []
    for path in edge_files:
        header = _read_header(path)
        labels = _parse_edge_labels(header)
        if labels is None:
            skipped.append(path)
            continue
        src_label, dst_label = labels
        if src_label not in node_id_types or dst_label not in node_id_types:
            skipped.append(path)
            continue
        table = _load_edge(path, node_id_types[src_label], node_id_types[dst_label])
        _write_lance(table, _edge_name_from_stem(path.stem))

    if skipped:
        print("Skipped non-graph CSVs:")
        for path in skipped:
            print(f"  - {path.relative_to(REPO_ROOT)}")

    print(f"Wrote Lance datasets to: {GRAPH_ROOT.resolve()}")


if __name__ == "__main__":
    main()
