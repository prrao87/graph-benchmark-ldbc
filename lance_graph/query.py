import sys
import time
from pathlib import Path
from typing import Any, Callable

import lance
import polars as pl
import pyarrow as pa
from lance_graph import CypherQuery, GraphConfig

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
    query: str,
    cfg: GraphConfig,
    datasets: dict[str, pa.Table],
    params: dict[str, Any] | None = None,
) -> pl.DataFrame:
    if params:
        query = apply_params(query, params)
    cypher = CypherQuery(query)
    result = cypher.with_config(cfg).execute(datasets)
    return to_polars(result)


def _execute(
    cfg: GraphConfig,
    datasets: dict[str, pa.Table],
    idx: int,
    query: str,
) -> pl.DataFrame:
    print(f"\nQuery {idx}:\n{query}")
    result = execute_query(query, cfg, datasets)
    print(result)
    return result


def _execute_distinct_count(
    cfg: GraphConfig,
    datasets: dict[str, pa.Table],
    idx: int,
    query: str,
    *,
    distinct_col: str,
    output_col: str,
    as_bool: bool = False,
) -> pl.DataFrame:
    print(f"\nQuery {idx}:\n{query}")
    result = execute_query(query, cfg, datasets)
    df = to_polars(result)
    count = df.select(pl.col(distinct_col).n_unique()).item()
    value = bool(count) if as_bool else int(count)
    out = pl.DataFrame({output_col: [value]})
    print(out)
    return out


def _execute_group_distinct_count(
    cfg: GraphConfig,
    datasets: dict[str, pa.Table],
    idx: int,
    query: str,
    *,
    group_col: str,
    distinct_col: str,
    output_count_col: str,
    output_group_col: str,
    limit: int = 1,
) -> pl.DataFrame:
    print(f"\nQuery {idx}:\n{query}")
    result = execute_query(query, cfg, datasets)
    df = to_polars(result)
    if df.is_empty():
        out = pl.DataFrame({output_group_col: [], output_count_col: []})
        print(out)
        return out
    agg = df.group_by(group_col).agg(
        pl.col(distinct_col).n_unique().alias(output_count_col)
    )
    agg = agg.sort(output_count_col, descending=True)
    if limit:
        agg = agg.head(limit)
    if output_group_col != group_col:
        agg = agg.rename({group_col: output_group_col})
    print(agg)
    return agg


def run_query1(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Who are the names of people who live in Glasgow and are interested in Napoleon?"
    query = """
        MATCH (t:Tag)<-[:hasInterest]-(p:Person)-[:personIsLocatedIn]->(pl:Place)
        WHERE pl.name = "Glasgow" AND t.name = "Napoleon"
        RETURN p.firstname, p.lastname
    """
    return _execute(cfg, datasets, 1, query)


def run_query2(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "IDs of posts by Lei Zhang whose content contains Zulu."
    query = """
        MATCH (p:Person)<-[:postHasCreator]-(post:Post)
        WHERE p.firstname = "Lei" AND p.lastname = "Zhang"
          AND post.content CONTAINS "Zulu"
        RETURN post.id
    """
    return _execute(cfg, datasets, 2, query)


def run_query3(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Creator of post ID 962077547172 and where they studied."
    query = """
        MATCH (post:Post {id: 962077547172})-[:postHasCreator]->(person:Person),
              (person)-[:studyAt]->(org:Organisation)
        RETURN person.firstname, person.lastname, org.name
    """
    return _execute(cfg, datasets, 3, query)


def run_query4(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Comment IDs by Alfredo Gomez with length > 100."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)
        WHERE p.firstname = "Alfredo"
          AND p.lastname = "Gomez"
          AND c.length > 100
        RETURN c.id
    """
    return _execute(cfg, datasets, 4, query)


def run_query5(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Full names of persons with last name Choi who are members of forums containing John Brown."
    query = """
        MATCH (f:Forum)-[:hasMember]->(p:Person)
        WHERE f.title CONTAINS "John Brown"
          AND p.lastname CONTAINS "Choi"
        RETURN DISTINCT p.firstname, p.lastname
        LIMIT 10
    """
    return _execute(cfg, datasets, 5, query)


def run_query6(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "IDs of employees who work at Nova_Air and whose last name contains Bravo."
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation)
        WHERE o.name = "Nova_Air" AND p.lastname CONTAINS "Bravo"
        RETURN p.id
    """
    return _execute(cfg, datasets, 6, query)


def run_query7(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Places where person 1786706544494 commented on posts tagged Jamaica."
    query = """
        MATCH (p:Person {id: 1786706544494})<-[:commentHasCreator]-(c:Comment)
              -[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag),
              (c)-[:commentIsLocatedIn]->(place:Place)
        WHERE t.name = "Jamaica"
        RETURN DISTINCT place.name
    """
    return _execute(cfg, datasets, 7, query)


def run_query8(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Distinct IDs of persons born after 1990 who moderate forums containing Emilio Fernandez."
    query = """
        MATCH (p:Person)<-[:hasModerator]-(f:Forum)
        WHERE p.birthday > "1990-01-01"
          AND f.title CONTAINS "Emilio Fernandez"
        RETURN DISTINCT p.id
    """
    return _execute(cfg, datasets, 8, query)


def run_query9(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Persons with last name Johansson who know someone who studied in Tallinn."
    query = """
        MATCH (p:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)
              -[:organisationIsLocatedIn]->(l:Place)
        WHERE l.name = "Tallinn" AND p.lastname = "Johansson"
        RETURN p.id, p.firstname, p.lastname
    """
    return _execute(cfg, datasets, 9, query)


def run_query10(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Unique IDs of persons who commented on posts tagged Cate_Blanchett."
    query = """
        MATCH (c:Comment)-[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag),
              (c)-[:commentHasCreator]->(p:Person)
        WHERE t.name = "Cate_Blanchett"
        RETURN DISTINCT p.id
    """
    return _execute(cfg, datasets, 10, query)


def run_query11(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Non-university organization with most employees."
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation)
        WHERE o.type <> "university"
        RETURN o.name AS org_name, p.id AS person_id
    """
    return _execute_group_distinct_count(
        cfg,
        datasets,
        11,
        query,
        group_col="org_name",
        distinct_col="person_id",
        output_count_col="num_e",
        output_group_col="o.name",
    )


def run_query12(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Total number of comments with non-null content created by people in Berlin."
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person)-[:personIsLocatedIn]->(l:Place)
        WHERE c.content IS NOT NULL AND l.name = "Berlin"
        RETURN DISTINCT c.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        12,
        query,
        distinct_col="id",
        output_col="num_comments",
    )


def run_query13(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Total number of persons who liked comments created by Rafael Alonso."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)<-[:likeComment]-(p2:Person)
        WHERE p.firstname = "Rafael" AND p.lastname = "Alonso"
        RETURN DISTINCT p2.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        13,
        query,
        distinct_col="id",
        output_col="num_persons",
    )


def run_query14(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Number of forums with tags belonging to the Athlete tagclass."
    query = """
        MATCH (f:Forum)-[:forumHasTag]->(:Tag)-[:hasType]->(:Tagclass {name: "Athlete"})
        RETURN DISTINCT f.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        14,
        query,
        distinct_col="id",
        output_col="num_forums",
    )


def run_query15(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Total number of forums moderated by employees of Air_Tanzania."
    query = """
        MATCH (f:Forum)-[:hasModerator]->(p:Person)-[:workAt]->(o:Organisation)
        WHERE o.name = "Air_Tanzania"
        RETURN DISTINCT f.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        15,
        query,
        distinct_col="id",
        output_col="num_forums",
    )


def run_query16(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Number of posts containing Copernicus created by persons located in Mumbai."
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(l:Place),
              (p)<-[:postHasCreator]-(post:Post)
        WHERE l.name = "Mumbai" AND post.content CONTAINS "Copernicus"
        RETURN COUNT(post.id) AS num_posts
    """
    return _execute(cfg, datasets, 16, query)


def run_query17(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Most common interest tag among people who studied at Indian_Institute_of_Science."
    query = """
        MATCH (p:Person)-[:studyAt]->(o:Organisation), (p)-[:hasInterest]->(t:Tag)
        WHERE o.name = "Indian_Institute_of_Science"
        RETURN t.name, COUNT(*) AS tag_count
        ORDER BY tag_count DESC
        LIMIT 1
    """
    return _execute(cfg, datasets, 17, query)


def run_query18(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "People studying at The_Oxford_Educational_Institutions with interest in William_Shakespeare."
    query = """
        MATCH (p:Person)-[:studyAt]->(o:Organisation), (p)-[:hasInterest]->(t:Tag)
        WHERE o.name = "The_Oxford_Educational_Institutions"
          AND t.name = "William_Shakespeare"
        RETURN DISTINCT p.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        18,
        query,
        distinct_col="id",
        output_col="num_p",
    )


def run_query19(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Place with most comments whose tag contains Copernicus."
    query = """
        MATCH (c:Comment)-[:commentHasTag]->(t:Tag), (c)-[:commentIsLocatedIn]->(l:Place)
        WHERE t.name CONTAINS "Copernicus"
        RETURN l.name, COUNT(c.id) AS comment_count
        ORDER BY comment_count DESC
        LIMIT 1
    """
    return _execute(cfg, datasets, 19, query)


def run_query20(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Number of comments containing World War II with length > 1000."
    query = """
        MATCH (c:Comment)
        WHERE c.content CONTAINS "World War II" AND c.length > 1000
        RETURN COUNT(c.id) AS long_comment_count
    """
    return _execute(cfg, datasets, 20, query)


def run_query21(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Has Bill Moore liked the post with ID 1649268446863?"
    query = """
        MATCH (p:Post)<-[:likePost]-(p2:Person)
        WHERE p2.firstname = "Bill" AND p2.lastname = "Moore"
          AND p.id = 1649268446863
        RETURN DISTINCT p.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        21,
        query,
        distinct_col="id",
        output_col="liked",
        as_bool=True,
    )


def run_query22(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Did anyone who works at Linxair create a comment that replied to a post?"
    query = """
        MATCH (o:Organisation {name: "Linxair"})<-[:workAt]-(p:Person)<-[:commentHasCreator]-(c:Comment)-[:replyOfPost]->(post:Post)
        RETURN DISTINCT c.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        22,
        query,
        distinct_col="id",
        output_col="has_reply_comment",
        as_bool=True,
    )


def run_query23(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Is there a person with last name Gurung who is a moderator of a forum tagged Norah_Jones?"
    query = """
        MATCH (p:Person)<-[:hasModerator]-(f:Forum)-[:forumHasTag]->(t:Tag)
        WHERE t.name = "Norah_Jones" AND p.lastname = "Gurung"
        RETURN DISTINCT p.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        23,
        query,
        distinct_col="id",
        output_col="has_moderator",
        as_bool=True,
    )


def run_query24(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Is there a person who lives in Paris and is interested in Cate_Blanchett?"
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(l:Place), (p)-[:hasInterest]->(t:Tag)
        WHERE l.name = "Paris" AND t.name = "Cate_Blanchett"
        RETURN DISTINCT p.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        24,
        query,
        distinct_col="id",
        output_col="has_person",
        as_bool=True,
    )


def run_query25(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Does Amit Singh know anyone who studied at MIT_School_of_Engineering?"
    query = """
        MATCH (amit:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)
        WHERE amit.firstname = "Amit" AND amit.lastname = "Singh"
          AND o.name = "MIT_School_of_Engineering"
        RETURN DISTINCT p2.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        25,
        query,
        distinct_col="id",
        output_col="knows_someone",
        as_bool=True,
    )


def run_query26(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Are there any forums with tag Benjamin_Franklin that person 10995116287854 is a member of?"
    query = """
        MATCH (f:Forum)-[:hasMember]->(p:Person), (f)-[:forumHasTag]->(t:Tag)
        WHERE p.id = 10995116287854 AND t.name = "Benjamin_Franklin"
        RETURN DISTINCT f.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        26,
        query,
        distinct_col="id",
        output_col="has_forum",
        as_bool=True,
    )


def run_query27(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Did any person from Toronto create a comment with tag Winston_Churchill?"
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person),
              (p)-[:personIsLocatedIn]->(l:Place),
              (c)-[:commentHasTag]->(t:Tag)
        WHERE l.name = "Toronto" AND t.name = "Winston_Churchill"
        RETURN DISTINCT c.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        27,
        query,
        distinct_col="id",
        output_col="has_comment",
        as_bool=True,
    )


def run_query28(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Are there people in Manila interested in tags of type BritishRoyalty?"
    query = """
        MATCH (p:Person)-[:hasInterest]->(t:Tag)-[:hasType]->(tc:Tagclass {name: "BritishRoyalty"}),
            (p)-[:personIsLocatedIn]->(l:Place {name: "Manila"})
        RETURN DISTINCT p.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        28,
        query,
        distinct_col="id",
        output_col="has_people",
        as_bool=True,
    )


def run_query29(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Has Justine Fenter written a post using Safari?"
    query = """
        MATCH (p:Person)<-[:postHasCreator]-(post:Post)
        WHERE p.firstname = "Justine" AND p.lastname = "Fenter"
          AND post.browserused CONTAINS "Safari"
        RETURN DISTINCT post.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        29,
        query,
        distinct_col="id",
        output_col="has_written_post_with_safari",
        as_bool=True,
    )


def run_query30(cfg: GraphConfig, datasets: dict[str, pa.Table]) -> pl.DataFrame:
    "Are there comments replying to posts created by the same person?"
    query = """
        MATCH (c1:Comment)-[:commentHasCreator]->(p:Person)
              <-[:postHasCreator]-(post:Post)<-[:replyOfPost]-(c2:Comment)
        WHERE c1.id = c2.id
        RETURN DISTINCT c1.id AS id
    """
    return _execute_distinct_count(
        cfg,
        datasets,
        30,
        query,
        distinct_col="id",
        output_col="has_self_reply",
        as_bool=True,
    )


QUERY_FUNCTIONS: dict[int, Callable[[GraphConfig, dict[str, pa.Table]], pl.DataFrame]] = {
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
    start = time.perf_counter()
    if selected is None:
        selected = list(QUERY_FUNCTIONS.keys())
    for idx in selected:
        func = QUERY_FUNCTIONS.get(idx)
        if func is None:
            print(f"Skipping unknown query index: {idx}")
            continue
        func(cfg, datasets)
    elapsed = time.perf_counter() - start
    print(f"\nCompleted {len(selected)} query(ies) in {elapsed:.2f}s")


if __name__ == "__main__":
    selected_queries = _parse_selection(sys.argv[1:])
    main(selected_queries)
