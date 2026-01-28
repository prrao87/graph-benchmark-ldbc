from __future__ import annotations

import asyncio
import os
from typing import Any, Iterable

import pytest
from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase, AsyncSession

import query

load_dotenv()

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def session(event_loop) -> AsyncSession:
    if NEO4J_USER is None or NEO4J_PASSWORD is None:
        raise EnvironmentError("NEO4J_USER and NEO4J_PASSWORD must be set")
    driver = AsyncGraphDatabase.driver(URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    session = driver.session(database=NEO4J_DATABASE)

    def _close():
        event_loop.run_until_complete(session.close())
        event_loop.run_until_complete(driver.close())

    yield session
    _close()


def _run(event_loop, coro):
    return event_loop.run_until_complete(coro)


def _rows(result: Any) -> list[dict[str, Any]]:
    if hasattr(result, "to_dicts"):
        return result.to_dicts()
    return result


def _row_sort_key(row: dict[str, Any]) -> tuple:
    return tuple(sorted(row.items()))


def _assert_rows(
    result: Any,
    expected_rows: Iterable[dict[str, Any]],
    *,
    order_sensitive: bool = False,
) -> None:
    rows = _rows(result)
    expected = list(expected_rows)
    if order_sensitive:
        assert rows == expected
        return
    assert sorted(rows, key=_row_sort_key) == sorted(expected, key=_row_sort_key)


def _assert_single_value(result: Any, key: str, expected_value: Any) -> None:
    rows = _rows(result)
    assert rows == [{key: expected_value}]


def test_benchmark_query1(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query1(session)))
    _assert_rows(
        result,
        [{"p.firstName": "Thomas", "p.lastName": "Brown"}],
        order_sensitive=True,
    )


def test_benchmark_query2(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query2(session)))
    _assert_rows(
        result,
        [
            {"post.ID": 2061586474857},
            {"post.ID": 2061586474860},
        ],
    )


def test_benchmark_query3(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query3(session)))
    _assert_rows(
        result,
        [
            {
                "person.firstName": "Mads",
                "person.lastName": "Haugland",
                "org.name": "Norwegian_School_of_Sport_Sciences",
            }
        ],
        order_sensitive=True,
    )


def test_benchmark_query4(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query4(session)))
    _assert_rows(result, [{"c.ID": 1924145496676}], order_sensitive=True)


def test_benchmark_query5(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query5(session)))
    _assert_rows(
        result,
        [{"p.firstName": "Akihiko", "p.lastName": "Choi"}],
        order_sensitive=True,
    )


def test_benchmark_query6(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query6(session)))
    _assert_rows(
        result,
        [
            {"p.ID": 28587302332692},
            {"p.ID": 17592186046501},
            {"p.ID": 19791209310595},
            {"p.ID": 15393162799645},
            {"p.ID": 15393162791641},
        ],
    )


def test_benchmark_query7(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query7(session)))
    _assert_rows(result, [])


def test_benchmark_query8(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query8(session)))
    _assert_rows(result, [{"p.ID": 13194139534410}], order_sensitive=True)


def test_benchmark_query9(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query9(session)))
    _assert_rows(
        result,
        [{"p.ID": 1242, "p.firstName": "Hans", "p.lastName": "Johansson"}],
        order_sensitive=True,
    )


def test_benchmark_query10(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query10(session)))
    _assert_rows(
        result,
        [
            {"p.ID": 6597069775789},
            {"p.ID": 8796093029267},
            {"p.ID": 24189255819727},
        ],
    )


def test_benchmark_query11(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query11(session)))
    _assert_rows(result, [{"num_e": 190, "o.name": "MDLR_Airlines"}])


def test_benchmark_query12(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query12(session)))
    _assert_single_value(result, "num_comments", 3229)


def test_benchmark_query13(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query13(session)))
    _assert_single_value(result, "num_persons", 2293)


def test_benchmark_query14(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query14(session)))
    _assert_single_value(result, "num_forums", 37)


def test_benchmark_query15(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query15(session)))
    _assert_single_value(result, "num_forums", 278)


def test_benchmark_query16(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query16(session)))
    _assert_single_value(result, "num_posts", 3)


def test_benchmark_query17(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query17(session)))
    _assert_rows(result, [{"t.name": "Hamid_Karzai", "tag_count": 32}])


def test_benchmark_query18(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query18(session)))
    _assert_single_value(result, "num_p", 20)


def test_benchmark_query19(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query19(session)))
    _assert_rows(result, [{"l.name": "India", "comment_count": 242}])


def test_benchmark_query20(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query20(session)))
    _assert_single_value(result, "long_comment_count", 3)


def test_benchmark_query21(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query21(session)))
    _assert_single_value(result, "liked", True)


def test_benchmark_query22(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query22(session)))
    _assert_single_value(result, "has_reply_comment", True)


def test_benchmark_query23(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query23(session)))
    _assert_single_value(result, "has_moderator", True)


def test_benchmark_query24(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query24(session)))
    _assert_single_value(result, "has_person", False)


def test_benchmark_query25(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query25(session)))
    _assert_single_value(result, "knows_someone", False)


def test_benchmark_query26(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query26(session)))
    _assert_single_value(result, "has_forum", False)


def test_benchmark_query27(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query27(session)))
    _assert_single_value(result, "has_comment", True)


def test_benchmark_query28(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query28(session)))
    _assert_single_value(result, "has_people", True)


def test_benchmark_query29(benchmark, session, event_loop):
    result = benchmark(lambda: _run(event_loop, query.run_query29(session)))
    _assert_single_value(result, "has_written_post_with_safari", False)


# def test_benchmark_query30(benchmark, session, event_loop):
#     result = benchmark(lambda: _run(event_loop, query.run_query30(session)))
#     _assert_single_value(result, "has_self_reply", True)
