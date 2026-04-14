import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

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

GraphDatasets = dict[str, pa.Table]
QueryContext = tuple[GraphConfig, GraphDatasets]


def build_config() -> GraphConfig:
    builder = GraphConfig.builder()
    for label in NODE_LABELS:
        builder = builder.with_node_label(label, "id")
    for rel_type in REL_DATASETS:
        builder = builder.with_relationship(rel_type, "src", "dst")
    return builder.build()


def load_datasets(root: Path) -> GraphDatasets:
    datasets: GraphDatasets = {}
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


def format_cypher_literal(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    return str(value)


def inline_unsupported_params(
    query: str,
    params: Mapping[str, Any] | None = None,
    *,
    inline_keys: tuple[str, ...] = (),
) -> tuple[str, dict[str, Any]]:
    # lance-graph 0.5.4 supports bound parameters for comparisons, but not for
    # the right-hand side of CONTAINS, so only those explicit keys are inlined.
    remaining = dict(params or {})
    for key in inline_keys:
        if key not in remaining:
            raise KeyError(f"Missing query parameter: {key}")
        query = query.replace(f"${key}", format_cypher_literal(remaining.pop(key)))
    return query, remaining


def build_query(
    config: GraphConfig,
    query: str,
    params: Mapping[str, Any] | None = None,
    *,
    inline_keys: tuple[str, ...] = (),
) -> CypherQuery:
    query_text, bound_params = inline_unsupported_params(
        query,
        params,
        inline_keys=inline_keys,
    )
    statement = CypherQuery(query_text).with_config(config)
    for key, value in bound_params.items():
        statement = statement.with_parameter(key, value)
    return statement


def execute_query(
    config: GraphConfig,
    datasets: GraphDatasets,
    query: str,
    params: Mapping[str, Any] | None = None,
    *,
    inline_keys: tuple[str, ...] = (),
) -> pl.DataFrame:
    result = build_query(
        config,
        query,
        params,
        inline_keys=inline_keys,
    ).execute(datasets)
    return to_polars(result)


def _execute(
    ctx: QueryContext,
    idx: int,
    query: str,
    params: Mapping[str, Any] | None = None,
    *,
    inline_keys: tuple[str, ...] = (),
) -> pl.DataFrame:
    config, datasets = ctx
    print(f"\nQuery {idx}:\n{query}")
    if params:
        print(f"Parameters: {dict(params)}")
    result = execute_query(
        config,
        datasets,
        query,
        params,
        inline_keys=inline_keys,
    )
    print(result)
    return result


def _execute_count_as_bool(
    ctx: QueryContext,
    idx: int,
    query: str,
    *,
    count_col: str,
    output_col: str,
    params: Mapping[str, Any] | None = None,
    inline_keys: tuple[str, ...] = (),
) -> pl.DataFrame:
    config, datasets = ctx
    print(f"\nQuery {idx}:\n{query}")
    if params:
        print(f"Parameters: {dict(params)}")
    df = execute_query(
        config,
        datasets,
        query,
        params,
        inline_keys=inline_keys,
    )
    if df.is_empty():
        value = False
    else:
        value = bool(df.select(pl.col(count_col)).item())
    out = pl.DataFrame({output_col: [value]})
    print(out)
    return out


def run_query1(ctx: QueryContext) -> pl.DataFrame:
    "Who are the names of people who live in Glasgow and are interested in Napoleon?"
    query = """
        MATCH (t:Tag)<-[:hasInterest]-(p:Person)-[:personIsLocatedIn]->(pl:Place)
        WHERE pl.name = $place_name AND t.name = $tag_name
        RETURN p.firstname, p.lastname
    """
    return _execute(ctx, 1, query, {"place_name": "Glasgow", "tag_name": "Napoleon"})


def run_query2(ctx: QueryContext) -> pl.DataFrame:
    "IDs of posts by Lei Zhang whose content contains Zulu."
    query = """
        MATCH (p:Person)<-[:postHasCreator]-(post:Post)
        WHERE p.firstname = $first_name AND p.lastname = $last_name
          AND post.content CONTAINS $content_fragment
        RETURN post.id
    """
    return _execute(
        ctx,
        2,
        query,
        {
            "first_name": "Lei",
            "last_name": "Zhang",
            "content_fragment": "Zulu",
        },
        inline_keys=("content_fragment",),
    )


def run_query3(ctx: QueryContext) -> pl.DataFrame:
    "Creator of post ID 962077547172 and where they studied."
    query = """
        MATCH (post:Post)-[:postHasCreator]->(person:Person),
              (person)-[:studyAt]->(org:Organisation)
        WHERE post.id = $post_id
        RETURN person.firstname, person.lastname, org.name
    """
    return _execute(ctx, 3, query, {"post_id": 962077547172})


def run_query4(ctx: QueryContext) -> pl.DataFrame:
    "Comment IDs by Alfredo Gomez with length > 100."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)
        WHERE p.firstname = $first_name
          AND p.lastname = $last_name
          AND c.length > $min_length
        RETURN c.id
    """
    return _execute(
        ctx,
        4,
        query,
        {
            "first_name": "Alfredo",
            "last_name": "Gomez",
            "min_length": 100,
        },
    )


def run_query5(ctx: QueryContext) -> pl.DataFrame:
    "Full names of persons with last name Choi who are members of forums containing John Brown."
    query = """
        MATCH (f:Forum)-[:hasMember]->(p:Person)
        WHERE f.title CONTAINS $forum_title_fragment
          AND p.lastname CONTAINS $last_name_fragment
        RETURN DISTINCT p.firstname, p.lastname
        LIMIT 10
    """
    return _execute(
        ctx,
        5,
        query,
        {
            "forum_title_fragment": "John Brown",
            "last_name_fragment": "Choi",
        },
        inline_keys=("forum_title_fragment", "last_name_fragment"),
    )


def run_query6(ctx: QueryContext) -> pl.DataFrame:
    "IDs of employees who work at Nova_Air and whose last name contains Bravo."
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation)
        WHERE o.name = $organization_name AND p.lastname CONTAINS $last_name_fragment
        RETURN p.id
    """
    return _execute(
        ctx,
        6,
        query,
        {
            "organization_name": "Nova_Air",
            "last_name_fragment": "Bravo",
        },
        inline_keys=("last_name_fragment",),
    )


def run_query7(ctx: QueryContext) -> pl.DataFrame:
    "Places where person 1786706544494 commented on posts tagged Jamaica."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)
              -[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag),
              (c)-[:commentIsLocatedIn]->(place:Place)
        WHERE p.id = $person_id AND t.name = $tag_name
        RETURN DISTINCT place.name
    """
    return _execute(
        ctx,
        7,
        query,
        {
            "person_id": 1786706544494,
            "tag_name": "Jamaica",
        },
    )


def run_query8(ctx: QueryContext) -> pl.DataFrame:
    "Distinct IDs of persons born after 1990 who moderate forums containing Emilio Fernandez."
    query = """
        MATCH (p:Person)<-[:hasModerator]-(f:Forum)
        WHERE p.birthday > $min_birthday
          AND f.title CONTAINS $forum_title_fragment
        RETURN DISTINCT p.id
    """
    return _execute(
        ctx,
        8,
        query,
        {
            "min_birthday": "1990-01-01",
            "forum_title_fragment": "Emilio Fernandez",
        },
        inline_keys=("forum_title_fragment",),
    )


def run_query9(ctx: QueryContext) -> pl.DataFrame:
    "Persons with last name Johansson who know someone who studied in Tallinn."
    query = """
        MATCH (p:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)
              -[:organisationIsLocatedIn]->(l:Place)
        WHERE l.name = $place_name AND p.lastname = $last_name
        RETURN p.id, p.firstname, p.lastname
    """
    return _execute(
        ctx,
        9,
        query,
        {
            "place_name": "Tallinn",
            "last_name": "Johansson",
        },
    )


def run_query10(ctx: QueryContext) -> pl.DataFrame:
    "Unique IDs of persons who commented on posts tagged Cate_Blanchett."
    query = """
        MATCH (c:Comment)-[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag),
              (c)-[:commentHasCreator]->(p:Person)
        WHERE t.name = $tag_name
        RETURN DISTINCT p.id
    """
    return _execute(ctx, 10, query, {"tag_name": "Cate_Blanchett"})


def run_query11(ctx: QueryContext) -> pl.DataFrame:
    "Non-university organization with most employees."
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation)
        WHERE o.type <> $organization_type
        RETURN COUNT(DISTINCT p.id) AS num_e, o.name
        ORDER BY num_e DESC
        LIMIT 1
    """
    return _execute(ctx, 11, query, {"organization_type": "university"})


def run_query12(ctx: QueryContext) -> pl.DataFrame:
    "Total number of comments with non-null content created by people in Berlin."
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person)-[:personIsLocatedIn]->(l:Place)
        WHERE c.content IS NOT NULL AND l.name = $place_name
        RETURN COUNT(DISTINCT c.id) AS num_comments
    """
    return _execute(ctx, 12, query, {"place_name": "Berlin"})


def run_query13(ctx: QueryContext) -> pl.DataFrame:
    "Total number of persons who liked comments created by Rafael Alonso."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)<-[:likeComment]-(p2:Person)
        WHERE p.firstname = $first_name AND p.lastname = $last_name
        RETURN COUNT(DISTINCT p2.id) AS num_persons
    """
    return _execute(
        ctx,
        13,
        query,
        {
            "first_name": "Rafael",
            "last_name": "Alonso",
        },
    )


def run_query14(ctx: QueryContext) -> pl.DataFrame:
    "Number of forums with tags belonging to the Athlete tagclass."
    query = """
        MATCH (f:Forum)-[:forumHasTag]->(:Tag)-[:hasType]->(tc:Tagclass)
        WHERE tc.name = $tagclass_name
        RETURN COUNT(DISTINCT f.id) AS num_forums
    """
    return _execute(ctx, 14, query, {"tagclass_name": "Athlete"})


def run_query15(ctx: QueryContext) -> pl.DataFrame:
    "Total number of forums moderated by employees of Air_Tanzania."
    query = """
        MATCH (f:Forum)-[:hasModerator]->(p:Person)-[:workAt]->(o:Organisation)
        WHERE o.name = $organization_name
        RETURN COUNT(DISTINCT f.id) AS num_forums
    """
    return _execute(ctx, 15, query, {"organization_name": "Air_Tanzania"})


def run_query16(ctx: QueryContext) -> pl.DataFrame:
    "Number of posts containing Copernicus created by persons located in Mumbai."
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(l:Place),
              (p)<-[:postHasCreator]-(post:Post)
        WHERE l.name = $place_name AND post.content CONTAINS $content_fragment
        RETURN COUNT(post.id) AS num_posts
    """
    return _execute(
        ctx,
        16,
        query,
        {
            "place_name": "Mumbai",
            "content_fragment": "Copernicus",
        },
        inline_keys=("content_fragment",),
    )


def run_query17(ctx: QueryContext) -> pl.DataFrame:
    "Most common interest tag among people who studied at Indian_Institute_of_Science."
    query = """
        MATCH (p:Person)-[:studyAt]->(o:Organisation), (p)-[:hasInterest]->(t:Tag)
        WHERE o.name = $organization_name
        RETURN t.name, COUNT(*) AS tag_count
        ORDER BY tag_count DESC
        LIMIT 1
    """
    return _execute(
        ctx,
        17,
        query,
        {"organization_name": "Indian_Institute_of_Science"},
    )


def run_query18(ctx: QueryContext) -> pl.DataFrame:
    "People studying at The_Oxford_Educational_Institutions with interest in William_Shakespeare."
    query = """
        MATCH (p:Person)-[:studyAt]->(o:Organisation), (p)-[:hasInterest]->(t:Tag)
        WHERE o.name = $organization_name
          AND t.name = $tag_name
        RETURN COUNT(DISTINCT p.id) AS num_p
    """
    return _execute(
        ctx,
        18,
        query,
        {
            "organization_name": "The_Oxford_Educational_Institutions",
            "tag_name": "William_Shakespeare",
        },
    )


def run_query19(ctx: QueryContext) -> pl.DataFrame:
    "Place with most comments whose tag contains Copernicus."
    query = """
        MATCH (c:Comment)-[:commentHasTag]->(t:Tag), (c)-[:commentIsLocatedIn]->(l:Place)
        WHERE t.name CONTAINS $tag_name_fragment
        RETURN l.name, COUNT(c.id) AS comment_count
        ORDER BY comment_count DESC
        LIMIT 1
    """
    return _execute(
        ctx,
        19,
        query,
        {"tag_name_fragment": "Copernicus"},
        inline_keys=("tag_name_fragment",),
    )


def run_query20(ctx: QueryContext) -> pl.DataFrame:
    "Number of comments containing World War II with length > 1000."
    query = """
        MATCH (c:Comment)
        WHERE c.content CONTAINS $content_fragment AND c.length > $min_length
        RETURN COUNT(c.id) AS long_comment_count
    """
    return _execute(
        ctx,
        20,
        query,
        {
            "content_fragment": "World War II",
            "min_length": 1000,
        },
        inline_keys=("content_fragment",),
    )


def run_query21(ctx: QueryContext) -> pl.DataFrame:
    "Has Bill Moore liked the post with ID 1649268446863?"
    query = """
        MATCH (p:Post)<-[:likePost]-(p2:Person)
        WHERE p2.firstname = $first_name AND p2.lastname = $last_name
          AND p.id = $post_id
        RETURN COUNT(DISTINCT p.id) AS liked
    """
    return _execute_count_as_bool(
        ctx,
        21,
        query,
        count_col="liked",
        output_col="liked",
        params={
            "first_name": "Bill",
            "last_name": "Moore",
            "post_id": 1649268446863,
        },
    )


def run_query22(ctx: QueryContext) -> pl.DataFrame:
    "Did anyone who works at Linxair create a comment that replied to a post?"
    query = """
        MATCH (o:Organisation)<-[:workAt]-(p:Person)<-[:commentHasCreator]-(c:Comment)-[:replyOfPost]->(post:Post)
        WHERE o.name = $organization_name
        RETURN COUNT(DISTINCT c.id) AS has_reply_comment
    """
    return _execute_count_as_bool(
        ctx,
        22,
        query,
        count_col="has_reply_comment",
        output_col="has_reply_comment",
        params={"organization_name": "Linxair"},
    )


def run_query23(ctx: QueryContext) -> pl.DataFrame:
    "Is there a person with last name Gurung who is a moderator of a forum tagged Norah_Jones?"
    query = """
        MATCH (p:Person)<-[:hasModerator]-(f:Forum)-[:forumHasTag]->(t:Tag)
        WHERE t.name = $tag_name AND p.lastname = $last_name
        RETURN COUNT(DISTINCT p.id) AS has_moderator
    """
    return _execute_count_as_bool(
        ctx,
        23,
        query,
        count_col="has_moderator",
        output_col="has_moderator",
        params={
            "tag_name": "Norah_Jones",
            "last_name": "Gurung",
        },
    )


def run_query24(ctx: QueryContext) -> pl.DataFrame:
    "Is there a person who lives in Paris and is interested in Cate_Blanchett?"
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(l:Place), (p)-[:hasInterest]->(t:Tag)
        WHERE l.name = $place_name AND t.name = $tag_name
        RETURN COUNT(DISTINCT p.id) AS has_person
    """
    return _execute_count_as_bool(
        ctx,
        24,
        query,
        count_col="has_person",
        output_col="has_person",
        params={
            "place_name": "Paris",
            "tag_name": "Cate_Blanchett",
        },
    )


def run_query25(ctx: QueryContext) -> pl.DataFrame:
    "Does Amit Singh know anyone who studied at MIT_School_of_Engineering?"
    query = """
        MATCH (amit:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)
        WHERE amit.firstname = $first_name AND amit.lastname = $last_name
          AND o.name = $organization_name
        RETURN COUNT(DISTINCT p2.id) AS knows_someone
    """
    return _execute_count_as_bool(
        ctx,
        25,
        query,
        count_col="knows_someone",
        output_col="knows_someone",
        params={
            "first_name": "Amit",
            "last_name": "Singh",
            "organization_name": "MIT_School_of_Engineering",
        },
    )


def run_query26(ctx: QueryContext) -> pl.DataFrame:
    "Are there any forums with tag Benjamin_Franklin that person 10995116287854 is a member of?"
    query = """
        MATCH (f:Forum)-[:hasMember]->(p:Person), (f)-[:forumHasTag]->(t:Tag)
        WHERE p.id = $person_id AND t.name = $tag_name
        RETURN COUNT(DISTINCT f.id) AS has_forum
    """
    return _execute_count_as_bool(
        ctx,
        26,
        query,
        count_col="has_forum",
        output_col="has_forum",
        params={
            "person_id": 10995116287854,
            "tag_name": "Benjamin_Franklin",
        },
    )


def run_query27(ctx: QueryContext) -> pl.DataFrame:
    "Did any person from Toronto create a comment with tag Winston_Churchill?"
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person),
              (p)-[:personIsLocatedIn]->(l:Place),
              (c)-[:commentHasTag]->(t:Tag)
        WHERE l.name = $place_name AND t.name = $tag_name
        RETURN COUNT(DISTINCT c.id) AS has_comment
    """
    return _execute_count_as_bool(
        ctx,
        27,
        query,
        count_col="has_comment",
        output_col="has_comment",
        params={
            "place_name": "Toronto",
            "tag_name": "Winston_Churchill",
        },
    )


def run_query28(ctx: QueryContext) -> pl.DataFrame:
    "Are there people in Manila interested in tags of type BritishRoyalty?"
    query = """
        MATCH (p:Person)-[:hasInterest]->(t:Tag)-[:hasType]->(tc:Tagclass),
              (p)-[:personIsLocatedIn]->(l:Place)
        WHERE tc.name = $tagclass_name AND l.name = $place_name
        RETURN COUNT(DISTINCT p.id) AS has_people
    """
    return _execute_count_as_bool(
        ctx,
        28,
        query,
        count_col="has_people",
        output_col="has_people",
        params={
            "tagclass_name": "BritishRoyalty",
            "place_name": "Manila",
        },
    )


def run_query29(ctx: QueryContext) -> pl.DataFrame:
    "Has Justine Fenter written a post using Safari?"
    query = """
        MATCH (p:Person)<-[:postHasCreator]-(post:Post)
        WHERE p.firstname = $first_name AND p.lastname = $last_name
          AND post.browserused CONTAINS $browser_name
        RETURN COUNT(DISTINCT post.id) AS has_written_post_with_safari
    """
    return _execute_count_as_bool(
        ctx,
        29,
        query,
        count_col="has_written_post_with_safari",
        output_col="has_written_post_with_safari",
        params={
            "first_name": "Justine",
            "last_name": "Fenter",
            "browser_name": "Safari",
        },
        inline_keys=("browser_name",),
    )


def run_query30(ctx: QueryContext) -> pl.DataFrame:
    "Are there comments replying to posts created by the same person?"
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(creator:Person),
              (c)-[:replyOfPost]->(post:Post)-[:postHasCreator]->(creator)
        RETURN COUNT(DISTINCT c.id) AS has_self_reply
    """
    return _execute_count_as_bool(
        ctx,
        30,
        query,
        count_col="has_self_reply",
        output_col="has_self_reply",
    )


QUERY_FUNCTIONS: dict[int, Callable[[QueryContext], pl.DataFrame]] = {
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
    30: run_query30,
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
    ctx = (build_config(), load_datasets(GRAPH_ROOT))
    start = time.perf_counter()
    if selected is None:
        selected = list(QUERY_FUNCTIONS.keys())
    for idx in selected:
        func = QUERY_FUNCTIONS.get(idx)
        if func is None:
            print(f"Skipping unknown query index: {idx}")
            continue
        func(ctx)
    elapsed = time.perf_counter() - start
    print(f"\nCompleted {len(selected)} query(ies) in {elapsed:.2f}s")


if __name__ == "__main__":
    selected_queries = _parse_selection(sys.argv[1:])
    main(selected_queries)
