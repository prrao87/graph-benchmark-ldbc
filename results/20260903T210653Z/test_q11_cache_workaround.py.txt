"""Regression checks for Q11's scoped cache workaround."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "ladybug_queries", ROOT / "ladybugdb/query.py"
)
queries = importlib.util.module_from_spec(spec)
spec.loader.exec_module(queries)


@pytest.mark.parametrize("query_fails", [False, True])
def test_q11_restores_cache(monkeypatch, query_fails):
    calls = []
    expected = [{"num_e": 190, "o.name": "MDLR_Airlines"}]
    connection = object()

    def execute(conn, idx, query):
        assert conn is connection
        assert idx == 11
        calls.append(query)
        if "MATCH" in query:
            if query_fails:
                raise RuntimeError("query failed")
            return expected
        return None

    monkeypatch.setattr(queries, "_execute", execute)
    if query_fails:
        with pytest.raises(RuntimeError, match="query failed"):
            queries.run_query11(connection)
    else:
        assert queries.run_query11(connection) is expected
    assert len(calls) == 3
    assert calls[0] == "CALL enable_cached_prepared_statement='none';"
    assert "COUNT(DISTINCT p.ID)" in calls[1]
    assert calls[2] == "CALL enable_cached_prepared_statement='both';"


def test_other_queries_do_not_toggle_cache(monkeypatch):
    calls = []
    monkeypatch.setattr(
        queries, "_execute", lambda conn, idx, query: calls.append(query)
    )
    queries.run_query10(object())
    assert len(calls) == 1
    assert "enable_cached_prepared_statement" not in calls[0]
