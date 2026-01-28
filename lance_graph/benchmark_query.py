from __future__ import annotations

from typing import Any, Iterable

import pytest

import query


@pytest.fixture(scope="session")
def graph():
    cfg = query.build_config()
    datasets = query.load_datasets(query.GRAPH_ROOT)
    return cfg, datasets


def _rows(result: Any) -> list[dict[str, Any]]:
    if hasattr(result, "to_dicts"):
        return result.to_dicts()
    return result


def _normalize_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append({str(key).lower(): value for key, value in row.items()})
    return normalized


def _row_sort_key(row: dict[str, Any]) -> tuple:
    return tuple(sorted(row.items()))


def _assert_rows(
    result: Any,
    expected_rows: Iterable[dict[str, Any]],
    *,
    order_sensitive: bool = False,
) -> None:
    rows = _normalize_rows(_rows(result))
    expected = _normalize_rows(expected_rows)
    if order_sensitive:
        assert rows == expected
        return
    assert sorted(rows, key=_row_sort_key) == sorted(expected, key=_row_sort_key)


def _assert_single_value(result: Any, key: str, expected_value: Any) -> None:
    rows = _normalize_rows(_rows(result))
    assert rows == [{key.lower(): expected_value}]


def test_benchmark_query1(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query1, cfg, datasets)
    _assert_rows(
        result,
        [{"p.firstname": "Thomas", "p.lastname": "Brown"}],
        order_sensitive=True,
    )


def test_benchmark_query2(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query2, cfg, datasets)
    _assert_rows(
        result,
        [
            {"post.id": 2061586474857},
            {"post.id": 2061586474860},
        ],
    )


def test_benchmark_query3(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query3, cfg, datasets)
    _assert_rows(
        result,
        [
            {
                "person.firstname": "Mads",
                "person.lastname": "Haugland",
                "org.name": "Norwegian_School_of_Sport_Sciences",
            }
        ],
        order_sensitive=True,
    )


def test_benchmark_query4(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query4, cfg, datasets)
    _assert_rows(result, [{"c.id": 1924145496676}], order_sensitive=True)


def test_benchmark_query5(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query5, cfg, datasets)
    _assert_rows(
        result,
        [{"p.firstname": "Akihiko", "p.lastname": "Choi"}],
        order_sensitive=True,
    )


def test_benchmark_query6(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query6, cfg, datasets)
    _assert_rows(
        result,
        [
            {"p.id": 28587302332692},
            {"p.id": 17592186046501},
            {"p.id": 19791209310595},
            {"p.id": 15393162799645},
            {"p.id": 15393162791641},
        ],
    )


def test_benchmark_query7(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query7, cfg, datasets)
    _assert_rows(result, [])


def test_benchmark_query8(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query8, cfg, datasets)
    _assert_rows(result, [{"p.id": 13194139534410}], order_sensitive=True)


def test_benchmark_query9(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query9, cfg, datasets)
    _assert_rows(
        result,
        [{"p.id": 1242, "p.firstname": "Hans", "p.lastname": "Johansson"}],
        order_sensitive=True,
    )


def test_benchmark_query10(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query10, cfg, datasets)
    _assert_rows(
        result,
        [
            {"p.id": 6597069775789},
            {"p.id": 8796093029267},
            {"p.id": 24189255819727},
        ],
    )


def test_benchmark_query11(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query11, cfg, datasets)
    _assert_rows(result, [{"num_e": 190, "o.name": "MDLR_Airlines"}])


def test_benchmark_query12(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query12, cfg, datasets)
    _assert_single_value(result, "num_comments", 3229)


def test_benchmark_query13(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query13, cfg, datasets)
    _assert_single_value(result, "num_persons", 2293)


def test_benchmark_query14(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query14, cfg, datasets)
    _assert_single_value(result, "num_forums", 37)


def test_benchmark_query15(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query15, cfg, datasets)
    _assert_single_value(result, "num_forums", 278)


def test_benchmark_query16(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query16, cfg, datasets)
    _assert_single_value(result, "num_posts", 3)


def test_benchmark_query17(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query17, cfg, datasets)
    _assert_rows(result, [{"t.name": "Hamid_Karzai", "tag_count": 32}])


def test_benchmark_query18(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query18, cfg, datasets)
    _assert_single_value(result, "num_p", 20)


def test_benchmark_query19(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query19, cfg, datasets)
    _assert_rows(result, [{"l.name": "India", "comment_count": 242}])


def test_benchmark_query20(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query20, cfg, datasets)
    _assert_single_value(result, "long_comment_count", 3)


def test_benchmark_query21(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query21, cfg, datasets)
    _assert_single_value(result, "liked", True)


def test_benchmark_query22(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query22, cfg, datasets)
    _assert_single_value(result, "has_reply_comment", True)


def test_benchmark_query23(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query23, cfg, datasets)
    _assert_single_value(result, "has_moderator", True)


def test_benchmark_query24(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query24, cfg, datasets)
    _assert_single_value(result, "has_person", False)


def test_benchmark_query25(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query25, cfg, datasets)
    _assert_single_value(result, "knows_someone", False)


def test_benchmark_query26(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query26, cfg, datasets)
    _assert_single_value(result, "has_forum", False)


def test_benchmark_query27(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query27, cfg, datasets)
    _assert_single_value(result, "has_comment", True)


def test_benchmark_query28(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query28, cfg, datasets)
    _assert_single_value(result, "has_people", True)


def test_benchmark_query29(benchmark, graph):
    cfg, datasets = graph
    result = benchmark(query.run_query29, cfg, datasets)
    _assert_single_value(result, "has_written_post_with_safari", False)


# def test_benchmark_query30(benchmark, graph):
#     cfg, datasets = graph
#     result = benchmark(query.run_query30, cfg, datasets)
#     _assert_single_value(result, "has_self_reply", True)
