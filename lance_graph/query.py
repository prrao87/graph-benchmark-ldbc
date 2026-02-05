import sys
import time
from pathlib import Path
from typing import Any, Callable

import lance
import polars as pl
import pyarrow as pa
from lance_graph import CypherEngine, GraphConfig

SCRIPT_ROOT = Path(__file__).resolve().parent
GRAPH_ROOT = SCRIPT_ROOT / "graph_lance"

NODE_LABELS = (
    "Comment",
    "Forum",
    "Organisation",
    "Person",
    "Place",
    "Post",
    "Tag",
    "Tagclass",
)

REL_DATASETS = {
    "containerOf": "forum_containerOf_post",
    "commentHasCreator": "comment_hasCreator_person",
    "postHasCreator": "post_hasCreator_person",
    "hasInterest": "person_hasInterest_tag",
    "hasMember": "forum_hasMember_person",
    "hasModerator": "forum_hasModerator_person",
    "commentHasTag": "comment_hasTag_tag",
    "forumHasTag": "forum_hasTag_tag",
    "postHasTag": "post_hasTag_tag",
    "hasType": "tag_hasType_tagclass",
    "commentIsLocatedIn": "comment_isLocatedIn_place",
    "organisationIsLocatedIn": "organisation_isLocatedIn_place",
    "personIsLocatedIn": "person_isLocatedIn_place",
    "postIsLocatedIn": "post_isLocatedIn_place",
    "isPartOf": "place_isPartOf_place",
    "isSubclassOf": "tagclass_isSubclassOf_tagclass",
    "knows": "person_knows_person",
    "likeComment": "person_likes_comment",
    "likePost": "person_likes_post",
    "replyOfComment": "comment_replyOf_comment",
    "replyOfPost": "comment_replyOf_post",
    "studyAt": "person_studyAt_organisation",
    "workAt": "person_workAt_organisation",
}


def build_config() -> GraphConfig:
    builder = GraphConfig.builder()
    for label in NODE_LABELS:
        builder = builder.with_node_label(label, "id")
    for rel_type in REL_DATASETS:
        builder = builder.with_relationship(rel_type, "src", "dst")
    return builder.build()


def load_datasets(root: Path) -> dict[str, pa.Table]:
    datasets: dict[str, pa.Table] = {}
    for label in NODE_LABELS:
        datasets[label] = lance.dataset(str(root / f"{label}.lance")).to_table()
    for rel_type, dataset_name in REL_DATASETS.items():
        datasets[rel_type] = lance.dataset(str(root / f"{dataset_name}.lance")).to_table()
    return datasets


def to_polars(result: Any) -> pl.DataFrame:
    if isinstance(result, pl.DataFrame):
        return result
    if isinstance(result, pa.Table):
        return pl.from_arrow(result)
    if isinstance(result, pa.RecordBatch):
        return pl.from_arrow(pa.Table.from_batches([result]))
    if hasattr(result, "to_pydict"):
        return pl.DataFrame(result.to_pydict())
    raise TypeError(f"Unsupported result type: {type(result)}")


def format_cypher_value(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value)


def apply_params(query: str, params: dict[str, Any]) -> str:
    for key, value in params.items():
        query = query.replace(f"${key}", format_cypher_value(value))
    return query


def execute_query(
    engine: CypherEngine,
    query: str,
    params: dict[str, Any] | None = None,
) -> pl.DataFrame:
    if params:
        query = apply_params(query, params)
    result = engine.execute(query)
    return to_polars(result)


def _execute(
    engine: CypherEngine,
    idx: int,
    query: str,
) -> pl.DataFrame:
    print(f"\nQuery {idx}:\n{query}")
    result = execute_query(engine, query)
    print(result)
    return result


def _execute_count_as_bool(
    engine: CypherEngine,
    idx: int,
    query: str,
    *,
    count_col: str,
    output_col: str,
) -> pl.DataFrame:
    print(f"\nQuery {idx}:\n{query}")
    result = execute_query(engine, query)
    df = to_polars(result)
    if df.is_empty():
        value = False
    else:
        count = df.select(pl.col(count_col)).item()
        value = bool(count)
    out = pl.DataFrame({output_col: [value]})
    print(out)
    return out


def run_query1(engine: CypherEngine) -> pl.DataFrame:
    "Who are the names of people who live in Glasgow and are interested in Napoleon?"
    query = """
        MATCH (t:Tag)<-[:hasInterest]-(p:Person)-[:personIsLocatedIn]->(pl:Place)
        WHERE pl.name = "Glasgow" AND t.name = "Napoleon"
        RETURN p.firstname, p.lastname
    """
    return _execute(engine, 1, query)


def run_query2(engine: CypherEngine) -> pl.DataFrame:
    "IDs of posts by Lei Zhang whose content contains Zulu."
    query = """
        MATCH (p:Person)<-[:postHasCreator]-(post:Post)
        WHERE p.firstname = "Lei" AND p.lastname = "Zhang"
          AND post.content CONTAINS "Zulu"
        RETURN post.id
    """
    return _execute(engine, 2, query)


def run_query3(engine: CypherEngine) -> pl.DataFrame:
    "Creator of post ID 962077547172 and where they studied."
    query = """
        MATCH (post:Post {id: 962077547172})-[:postHasCreator]->(person:Person),
              (person)-[:studyAt]->(org:Organisation)
        RETURN person.firstname, person.lastname, org.name
    """
    return _execute(engine, 3, query)


def run_query4(engine: CypherEngine) -> pl.DataFrame:
    "Comment IDs by Alfredo Gomez with length > 100."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)
        WHERE p.firstname = "Alfredo"
          AND p.lastname = "Gomez"
          AND c.length > 100
        RETURN c.id
    """
    return _execute(engine, 4, query)


def run_query5(engine: CypherEngine) -> pl.DataFrame:
    "Full names of persons with last name Choi who are members of forums containing John Brown."
    query = """
        MATCH (f:Forum)-[:hasMember]->(p:Person)
        WHERE f.title CONTAINS "John Brown"
          AND p.lastname CONTAINS "Choi"
        RETURN DISTINCT p.firstname, p.lastname
        LIMIT 10
    """
    return _execute(engine, 5, query)


def run_query6(engine: CypherEngine) -> pl.DataFrame:
    "IDs of employees who work at Nova_Air and whose last name contains Bravo."
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation)
        WHERE o.name = "Nova_Air" AND p.lastname CONTAINS "Bravo"
        RETURN p.id
    """
    return _execute(engine, 6, query)


def run_query7(engine: CypherEngine) -> pl.DataFrame:
    "Places where person 1786706544494 commented on posts tagged Jamaica."
    query = """
        MATCH (p:Person {id: 1786706544494})<-[:commentHasCreator]-(c:Comment)
              -[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag),
              (c)-[:commentIsLocatedIn]->(place:Place)
        WHERE t.name = "Jamaica"
        RETURN DISTINCT place.name
    """
    return _execute(engine, 7, query)


def run_query8(engine: CypherEngine) -> pl.DataFrame:
    "Distinct IDs of persons born after 1990 who moderate forums containing Emilio Fernandez."
    query = """
        MATCH (p:Person)<-[:hasModerator]-(f:Forum)
        WHERE p.birthday > "1990-01-01"
          AND f.title CONTAINS "Emilio Fernandez"
        RETURN DISTINCT p.id
    """
    return _execute(engine, 8, query)


def run_query9(engine: CypherEngine) -> pl.DataFrame:
    "Persons with last name Johansson who know someone who studied in Tallinn."
    query = """
        MATCH (p:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)
              -[:organisationIsLocatedIn]->(l:Place)
        WHERE l.name = "Tallinn" AND p.lastname = "Johansson"
        RETURN p.id, p.firstname, p.lastname
    """
    return _execute(engine, 9, query)


def run_query10(engine: CypherEngine) -> pl.DataFrame:
    "Unique IDs of persons who commented on posts tagged Cate_Blanchett."
    query = """
        MATCH (c:Comment)-[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag),
              (c)-[:commentHasCreator]->(p:Person)
        WHERE t.name = "Cate_Blanchett"
        RETURN DISTINCT p.id
    """
    return _execute(engine, 10, query)


def run_query11(engine: CypherEngine) -> pl.DataFrame:
    "Non-university organization with most employees."
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation)
        WHERE o.type <> "university"
        RETURN COUNT(DISTINCT p.id) AS num_e, o.name
        ORDER BY num_e DESC
        LIMIT 1
    """
    return _execute(engine, 11, query)


def run_query12(engine: CypherEngine) -> pl.DataFrame:
    "Total number of comments with non-null content created by people in Berlin."
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person)-[:personIsLocatedIn]->(l:Place)
        WHERE c.content IS NOT NULL AND l.name = "Berlin"
        RETURN COUNT(DISTINCT c.id) AS num_comments
    """
    return _execute(engine, 12, query)


def run_query13(engine: CypherEngine) -> pl.DataFrame:
    "Total number of persons who liked comments created by Rafael Alonso."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)<-[:likeComment]-(p2:Person)
        WHERE p.firstname = "Rafael" AND p.lastname = "Alonso"
        RETURN COUNT(DISTINCT p2.id) AS num_persons
    """
    return _execute(engine, 13, query)


def run_query14(engine: CypherEngine) -> pl.DataFrame:
    "Number of forums with tags belonging to the Athlete tagclass."
    query = """
        MATCH (f:Forum)-[:forumHasTag]->(:Tag)-[:hasType]->(:Tagclass {name: "Athlete"})
        RETURN COUNT(DISTINCT f.id) AS num_forums
    """
    return _execute(engine, 14, query)


def run_query15(engine: CypherEngine) -> pl.DataFrame:
    "Total number of forums moderated by employees of Air_Tanzania."
    query = """
        MATCH (f:Forum)-[:hasModerator]->(p:Person)-[:workAt]->(o:Organisation)
        WHERE o.name = "Air_Tanzania"
        RETURN COUNT(DISTINCT f.id) AS num_forums
    """
    return _execute(engine, 15, query)


def run_query16(engine: CypherEngine) -> pl.DataFrame:
    "Number of posts containing Copernicus created by persons located in Mumbai."
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(l:Place),
              (p)<-[:postHasCreator]-(post:Post)
        WHERE l.name = "Mumbai" AND post.content CONTAINS "Copernicus"
        RETURN COUNT(post.id) AS num_posts
    """
    return _execute(engine, 16, query)


def run_query17(engine: CypherEngine) -> pl.DataFrame:
    "Most common interest tag among people who studied at Indian_Institute_of_Science."
    query = """
        MATCH (p:Person)-[:studyAt]->(o:Organisation), (p)-[:hasInterest]->(t:Tag)
        WHERE o.name = "Indian_Institute_of_Science"
        RETURN t.name, COUNT(*) AS tag_count
        ORDER BY tag_count DESC
        LIMIT 1
    """
    return _execute(engine, 17, query)


def run_query18(engine: CypherEngine) -> pl.DataFrame:
    "People studying at The_Oxford_Educational_Institutions with interest in William_Shakespeare."
    query = """
        MATCH (p:Person)-[:studyAt]->(o:Organisation), (p)-[:hasInterest]->(t:Tag)
        WHERE o.name = "The_Oxford_Educational_Institutions"
          AND t.name = "William_Shakespeare"
        RETURN COUNT(DISTINCT p.id) AS num_p
    """
    return _execute(engine, 18, query)


def run_query19(engine: CypherEngine) -> pl.DataFrame:
    "Place with most comments whose tag contains Copernicus."
    query = """
        MATCH (c:Comment)-[:commentHasTag]->(t:Tag), (c)-[:commentIsLocatedIn]->(l:Place)
        WHERE t.name CONTAINS "Copernicus"
        RETURN l.name, COUNT(c.id) AS comment_count
        ORDER BY comment_count DESC
        LIMIT 1
    """
    return _execute(engine, 19, query)


def run_query20(engine: CypherEngine) -> pl.DataFrame:
    "Number of comments containing World War II with length > 1000."
    query = """
        MATCH (c:Comment)
        WHERE c.content CONTAINS "World War II" AND c.length > 1000
        RETURN COUNT(c.id) AS long_comment_count
    """
    return _execute(engine, 20, query)


def run_query21(engine: CypherEngine) -> pl.DataFrame:
    "Has Bill Moore liked the post with ID 1649268446863?"
    query = """
        MATCH (p:Post)<-[:likePost]-(p2:Person)
        WHERE p2.firstname = "Bill" AND p2.lastname = "Moore"
          AND p.id = 1649268446863
        RETURN COUNT(DISTINCT p.id) AS liked
    """
    return _execute_count_as_bool(
        engine,
        21,
        query,
        count_col="liked",
        output_col="liked",
    )


def run_query22(engine: CypherEngine) -> pl.DataFrame:
    "Did anyone who works at Linxair create a comment that replied to a post?"
    query = """
        MATCH (o:Organisation {name: "Linxair"})<-[:workAt]-(p:Person)<-[:commentHasCreator]-(c:Comment)-[:replyOfPost]->(post:Post)
        RETURN COUNT(DISTINCT c.id) AS has_reply_comment
    """
    return _execute_count_as_bool(
        engine,
        22,
        query,
        count_col="has_reply_comment",
        output_col="has_reply_comment",
    )


def run_query23(engine: CypherEngine) -> pl.DataFrame:
    "Is there a person with last name Gurung who is a moderator of a forum tagged Norah_Jones?"
    query = """
        MATCH (p:Person)<-[:hasModerator]-(f:Forum)-[:forumHasTag]->(t:Tag)
        WHERE t.name = "Norah_Jones" AND p.lastname = "Gurung"
        RETURN COUNT(DISTINCT p.id) AS has_moderator
    """
    return _execute_count_as_bool(
        engine,
        23,
        query,
        count_col="has_moderator",
        output_col="has_moderator",
    )


def run_query24(engine: CypherEngine) -> pl.DataFrame:
    "Is there a person who lives in Paris and is interested in Cate_Blanchett?"
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(l:Place), (p)-[:hasInterest]->(t:Tag)
        WHERE l.name = "Paris" AND t.name = "Cate_Blanchett"
        RETURN COUNT(DISTINCT p.id) AS has_person
    """
    return _execute_count_as_bool(
        engine,
        24,
        query,
        count_col="has_person",
        output_col="has_person",
    )


def run_query25(engine: CypherEngine) -> pl.DataFrame:
    "Does Amit Singh know anyone who studied at MIT_School_of_Engineering?"
    query = """
        MATCH (amit:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)
        WHERE amit.firstname = "Amit" AND amit.lastname = "Singh"
          AND o.name = "MIT_School_of_Engineering"
        RETURN COUNT(DISTINCT p2.id) AS knows_someone
    """
    return _execute_count_as_bool(
        engine,
        25,
        query,
        count_col="knows_someone",
        output_col="knows_someone",
    )


def run_query26(engine: CypherEngine) -> pl.DataFrame:
    "Are there any forums with tag Benjamin_Franklin that person 10995116287854 is a member of?"
    query = """
        MATCH (f:Forum)-[:hasMember]->(p:Person), (f)-[:forumHasTag]->(t:Tag)
        WHERE p.id = 10995116287854 AND t.name = "Benjamin_Franklin"
        RETURN COUNT(DISTINCT f.id) AS has_forum
    """
    return _execute_count_as_bool(
        engine,
        26,
        query,
        count_col="has_forum",
        output_col="has_forum",
    )


def run_query27(engine: CypherEngine) -> pl.DataFrame:
    "Did any person from Toronto create a comment with tag Winston_Churchill?"
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person),
              (p)-[:personIsLocatedIn]->(l:Place),
              (c)-[:commentHasTag]->(t:Tag)
        WHERE l.name = "Toronto" AND t.name = "Winston_Churchill"
        RETURN COUNT(DISTINCT c.id) AS has_comment
    """
    return _execute_count_as_bool(
        engine,
        27,
        query,
        count_col="has_comment",
        output_col="has_comment",
    )


def run_query28(engine: CypherEngine) -> pl.DataFrame:
    "Are there people in Manila interested in tags of type BritishRoyalty?"
    query = """
        MATCH (p:Person)-[:hasInterest]->(t:Tag)-[:hasType]->(tc:Tagclass {name: "BritishRoyalty"}),
            (p)-[:personIsLocatedIn]->(l:Place {name: "Manila"})
        RETURN COUNT(DISTINCT p.id) AS has_people
    """
    return _execute_count_as_bool(
        engine,
        28,
        query,
        count_col="has_people",
        output_col="has_people",
    )


def run_query29(engine: CypherEngine) -> pl.DataFrame:
    "Has Justine Fenter written a post using Safari?"
    query = """
        MATCH (p:Person)<-[:postHasCreator]-(post:Post)
        WHERE p.firstname = "Justine" AND p.lastname = "Fenter"
          AND post.browserused CONTAINS "Safari"
        RETURN COUNT(DISTINCT post.id) AS has_written_post_with_safari
    """
    return _execute_count_as_bool(
        engine,
        29,
        query,
        count_col="has_written_post_with_safari",
        output_col="has_written_post_with_safari",
    )


def run_query30(engine: CypherEngine) -> pl.DataFrame:
    "Are there comments replying to posts created by the same person?"
    query = """
        MATCH (c1:Comment)-[:commentHasCreator]->(p:Person)
              <-[:postHasCreator]-(post:Post)<-[:replyOfPost]-(c2:Comment)
        WHERE c1.id = c2.id
        RETURN COUNT(DISTINCT c1.id) AS has_self_reply
    """
    return _execute_count_as_bool(
        engine,
        30,
        query,
        count_col="has_self_reply",
        output_col="has_self_reply",
    )


QUERY_FUNCTIONS: dict[int, Callable[[CypherEngine], pl.DataFrame]] = {
    1: run_query1,
    2: run_query2,
    3: run_query3,
    4: run_query4,
    5: run_query5,
    6: run_query6,
    7: run_query7,
    8: run_query8,
    9: run_query9,
    10: run_query10,
    11: run_query11,
    12: run_query12,
    13: run_query13,
    14: run_query14,
    15: run_query15,
    16: run_query16,
    17: run_query17,
    18: run_query18,
    19: run_query19,
    20: run_query20,
    21: run_query21,
    22: run_query22,
    23: run_query23,
    24: run_query24,
    25: run_query25,
    26: run_query26,
    27: run_query27,
    28: run_query28,
    29: run_query29,
    # 30: run_query30,  # Disabled: self-reply query
}


def _parse_selection(argv: list[str]) -> list[int] | None:
    if not argv:
        return None
    selection = argv[0].strip()
    if selection in {"run_all", "all"}:
        return None
    parts = [p.strip() for p in selection.split(",") if p.strip()]
    indices: list[int] = []
    for part in parts:
        try:
            indices.append(int(part))
        except ValueError:
            raise ValueError(f"Invalid query index: {part}")
    return indices


def main(selected: list[int] | None = None) -> None:
    cfg = build_config()
    datasets = load_datasets(GRAPH_ROOT)
    engine = CypherEngine(cfg, datasets)
    start = time.perf_counter()
    if selected is None:
        selected = list(QUERY_FUNCTIONS.keys())
    for idx in selected:
        func = QUERY_FUNCTIONS.get(idx)
        if func is None:
            print(f"Skipping unknown query index: {idx}")
            continue
        func(engine)
    elapsed = time.perf_counter() - start
    print(f"\nCompleted {len(selected)} query(ies) in {elapsed:.2f}s")


if __name__ == "__main__":
    selected_queries = _parse_selection(sys.argv[1:])
    main(selected_queries)
