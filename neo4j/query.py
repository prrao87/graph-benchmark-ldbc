import asyncio
import os
import sys
import time
from typing import Awaitable, Callable

from dotenv import load_dotenv
from neo4j import AsyncGraphDatabase, AsyncSession

load_dotenv()

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")


async def _execute(session: AsyncSession, idx: int, query: str):
    print(f"\nQuery {idx}:\n{query}")
    result = await session.run(query)
    records = await result.data()
    print(records)
    return records


async def run_query1(session: AsyncSession):
    "Who are the names of people who live in Glasgow and are interested in Napoleon?"
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(pl:Place),
              (p)-[:hasInterest]->(t:Tag)
        WHERE pl.name = "Glasgow" AND t.name = "Napoleon"
        RETURN p.firstName, p.lastName;
    """
    return await _execute(session, 1, query)


async def run_query2(session: AsyncSession):
    "IDs of posts by Lei Zhang whose content contains Zulu."
    query = """
        MATCH (p:Person)<-[:postHasCreator]-(post:Post)
        WHERE p.firstName = "Lei" AND p.lastName = "Zhang"
          AND post.content CONTAINS "Zulu"
        RETURN post.ID;
    """
    return await _execute(session, 2, query)


async def run_query3(session: AsyncSession):
    "Creator of post ID 962077547172 and where they studied."
    query = """
        MATCH (post:Post {ID: 962077547172})-[:postHasCreator]->(person:Person),
              (person)-[:studyAt]->(org:Organisation)
        RETURN person.firstName, person.lastName, org.name;
    """
    return await _execute(session, 3, query)


async def run_query4(session: AsyncSession):
    "Comment IDs by Alfredo Gomez with length > 100."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)
        WHERE p.firstName = "Alfredo"
          AND p.lastName = "Gomez"
          AND c.length > 100
        RETURN c.ID;
    """
    return await _execute(session, 4, query)


async def run_query5(session: AsyncSession):
    "Full names of persons with last name Choi who are members of forums containing John Brown."
    query = """
        MATCH (f:Forum)-[:hasMember]->(p:Person)
        WHERE f.title CONTAINS "John Brown"
          AND p.lastName CONTAINS "Choi"
        RETURN DISTINCT p.firstName, p.lastName
        LIMIT 10;
    """
    return await _execute(session, 5, query)


async def run_query6(session: AsyncSession):
    "IDs of employees who work at Nova_Air and whose last name contains Bravo."
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation)
        WHERE o.name = "Nova_Air" AND p.lastName CONTAINS "Bravo"
        RETURN p.ID;
    """
    return await _execute(session, 6, query)


async def run_query7(session: AsyncSession):
    "Places where person 1786706544494 commented on posts tagged Jamaica."
    query = """
        MATCH (p:Person {ID: 1786706544494})<-[:commentHasCreator]-(c:Comment)
              -[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag),
              (c)-[:commentIsLocatedIn]->(place:Place)
        WHERE t.name = "Jamaica"
        RETURN DISTINCT place.name;
    """
    return await _execute(session, 7, query)


async def run_query8(session: AsyncSession):
    "Distinct IDs of persons born after 1990 who moderate forums containing Emilio Fernandez."
    query = """
        MATCH (p:Person)<-[:hasModerator]-(f:Forum)
        WHERE date(p.birthday) > date("1990-01-01")
          AND f.title CONTAINS "Emilio Fernandez"
        RETURN DISTINCT p.ID;
    """
    return await _execute(session, 8, query)


async def run_query9(session: AsyncSession):
    "Persons with last name Johansson who know someone who studied in Tallinn."
    query = """
        MATCH (p:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)
              -[:organisationIsLocatedIn]->(l:Place)
        WHERE l.name = "Tallinn" AND p.lastName = "Johansson"
        RETURN p.ID, p.firstName, p.lastName;
    """
    return await _execute(session, 9, query)


async def run_query10(session: AsyncSession):
    "Unique IDs of persons who commented on posts tagged Cate_Blanchett."
    query = """
        MATCH (c:Comment)-[:replyOfPost]->(post:Post)-[:postHasTag]->(t:Tag),
              (c)-[:commentHasCreator]->(p:Person)
        WHERE t.name = "Cate_Blanchett"
        RETURN DISTINCT p.ID;
    """
    return await _execute(session, 10, query)


async def run_query11(session: AsyncSession):
    "Non-university organization with most employees."
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation)
        WHERE o.type <> "university"
        RETURN COUNT(DISTINCT p.ID) AS num_e, o.name
        ORDER BY num_e DESC
        LIMIT 1;
    """
    return await _execute(session, 11, query)


async def run_query12(session: AsyncSession):
    "Total number of comments with non-null content created by people in Berlin."
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person)-[:personIsLocatedIn]->(l:Place)
        WHERE c.content IS NOT NULL AND l.name = "Berlin"
        RETURN COUNT(DISTINCT c.ID) AS num_comments;
    """
    return await _execute(session, 12, query)


async def run_query13(session: AsyncSession):
    "Total number of persons who liked comments created by Rafael Alonso."
    query = """
        MATCH (p:Person)<-[:commentHasCreator]-(c:Comment)<-[:likeComment]-(p2:Person)
        WHERE p.firstName = "Rafael" AND p.lastName = "Alonso"
        RETURN COUNT(DISTINCT p2.ID) AS num_persons;
    """
    return await _execute(session, 13, query)


async def run_query14(session: AsyncSession):
    "Number of forums with tags belonging to the Athlete tagclass."
    query = """
        MATCH (f:Forum)-[:forumHasTag]->(:Tag)-[:hasType]->(:Tagclass {name: "Athlete"})
        RETURN COUNT(DISTINCT f.ID) AS num_forums;
    """
    return await _execute(session, 14, query)


async def run_query15(session: AsyncSession):
    "Total number of forums moderated by employees of Air_Tanzania."
    query = """
        MATCH (f:Forum)-[:hasModerator]->(p:Person)-[:workAt]->(o:Organisation)
        WHERE o.name = "Air_Tanzania"
        RETURN COUNT(DISTINCT f.ID) AS num_forums;
    """
    return await _execute(session, 15, query)


async def run_query16(session: AsyncSession):
    "Number of posts containing Copernicus created by persons located in Mumbai."
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(l:Place),
              (p)<-[:postHasCreator]-(post:Post)
        WHERE l.name = "Mumbai" AND post.content CONTAINS "Copernicus"
        RETURN COUNT(post.ID) AS num_posts;
    """
    return await _execute(session, 16, query)


async def run_query17(session: AsyncSession):
    "Most common interest tag among people who studied at Indian_Institute_of_Science."
    query = """
        MATCH (p:Person)-[:studyAt]->(o:Organisation), (p)-[:hasInterest]->(t:Tag)
        WHERE o.name = "Indian_Institute_of_Science"
        RETURN t.name, COUNT(*) AS tag_count
        ORDER BY tag_count DESC
        LIMIT 1;
    """
    return await _execute(session, 17, query)


async def run_query18(session: AsyncSession):
    "People studying at The_Oxford_Educational_Institutions with interest in William_Shakespeare."
    query = """
        MATCH (p:Person)-[:studyAt]->(o:Organisation), (p)-[:hasInterest]->(t:Tag)
        WHERE o.name = "The_Oxford_Educational_Institutions"
          AND t.name = "William_Shakespeare"
        RETURN COUNT(DISTINCT p.ID) AS num_p;
    """
    return await _execute(session, 18, query)


async def run_query19(session: AsyncSession):
    "Place with most comments whose tag contains Copernicus."
    query = """
        MATCH (c:Comment)-[:commentHasTag]->(t:Tag), (c)-[:commentIsLocatedIn]->(l:Place)
        WHERE t.name CONTAINS "Copernicus"
        RETURN l.name, COUNT(c.ID) AS comment_count
        ORDER BY comment_count DESC
        LIMIT 1;
    """
    return await _execute(session, 19, query)


async def run_query20(session: AsyncSession):
    "Number of comments containing World War II with length > 1000."
    query = """
        MATCH (c:Comment)
        WHERE c.content CONTAINS "World War II" AND c.length > 1000
        RETURN COUNT(c.ID) AS long_comment_count;
    """
    return await _execute(session, 20, query)


async def run_query21(session: AsyncSession):
    "Has Bill Moore liked the post with ID 1649268446863?"
    query = """
        MATCH (p:Post)<-[:likePost]-(p2:Person)
        WHERE p2.firstName = "Bill" AND p2.lastName = "Moore"
          AND p.ID = 1649268446863
        RETURN COUNT(p.ID) > 0 AS liked;
    """
    return await _execute(session, 21, query)


async def run_query22(session: AsyncSession):
    "Did anyone who works at Linxair create a comment that replied to a post?"
    query = """
        MATCH (p:Person)-[:workAt]->(o:Organisation),
              (c:Comment)-[:replyOfPost]->(post:Post),
              (c)-[:commentHasCreator]->(p)
        WHERE o.name = "Linxair"
        RETURN COUNT(DISTINCT c.ID) > 0 AS has_reply_comment;
    """
    return await _execute(session, 22, query)


async def run_query23(session: AsyncSession):
    "Is there a person with last name Gurung who is a moderator of a forum tagged Norah_Jones?"
    query = """
        MATCH (p:Person)<-[:hasModerator]-(f:Forum)-[:forumHasTag]->(t:Tag)
        WHERE t.name = "Norah_Jones" AND p.lastName = "Gurung"
        RETURN COUNT(DISTINCT p.ID) > 0 AS has_moderator;
    """
    return await _execute(session, 23, query)


async def run_query24(session: AsyncSession):
    "Is there a person who lives in Paris and is interested in Cate_Blanchett?"
    query = """
        MATCH (p:Person)-[:personIsLocatedIn]->(l:Place), (p)-[:hasInterest]->(t:Tag)
        WHERE l.name = "Paris" AND t.name = "Cate_Blanchett"
        RETURN COUNT(DISTINCT p.ID) > 0 AS has_person;
    """
    return await _execute(session, 24, query)


async def run_query25(session: AsyncSession):
    "Does Amit Singh know anyone who studied at MIT_School_of_Engineering?"
    query = """
        MATCH (amit:Person)-[:knows]->(p2:Person)-[:studyAt]->(o:Organisation)
        WHERE amit.firstName = "Amit" AND amit.lastName = "Singh"
          AND o.name = "MIT_School_of_Engineering"
        RETURN COUNT(DISTINCT p2.ID) > 0 AS knows_someone;
    """
    return await _execute(session, 25, query)


async def run_query26(session: AsyncSession):
    "Are there any forums with tag Benjamin_Franklin that person 10995116287854 is a member of?"
    query = """
        MATCH (f:Forum)-[:hasMember]->(p:Person), (f)-[:forumHasTag]->(t:Tag)
        WHERE p.ID = 10995116287854 AND t.name = "Benjamin_Franklin"
        RETURN COUNT(DISTINCT f.ID) > 0 AS has_forum;
    """
    return await _execute(session, 26, query)


async def run_query27(session: AsyncSession):
    "Did any person from Toronto create a comment with tag Winston_Churchill?"
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person),
              (p)-[:personIsLocatedIn]->(l:Place),
              (c)-[:commentHasTag]->(t:Tag)
        WHERE l.name = "Toronto" AND t.name = "Winston_Churchill"
        RETURN COUNT(DISTINCT c.ID) > 0 AS has_comment;
    """
    return await _execute(session, 27, query)


async def run_query28(session: AsyncSession):
    "Are there people in Manila interested in tags of type BritishRoyalty?"
    query = """
        MATCH (t:Tag)-[:hasType]->(tc:Tagclass),
              (p:Person)-[:hasInterest]->(t),
              (p)-[:personIsLocatedIn]->(l:Place)
        WHERE tc.name = "BritishRoyalty" AND l.name = "Manila"
        RETURN COUNT(DISTINCT p.ID) > 0 AS has_people;
    """
    return await _execute(session, 28, query)


async def run_query29(session: AsyncSession):
    "Has Justine Fenter written a post using Safari?"
    query = """
        MATCH (p:Person)<-[:postHasCreator]-(post:Post)
        WHERE p.firstName = "Justine" AND p.lastName = "Fenter"
          AND post.browserUsed CONTAINS "Safari"
        RETURN COUNT(post.ID) > 0 AS has_written_post_with_safari;
    """
    return await _execute(session, 29, query)


async def run_query30(session: AsyncSession):
    "Are there comments replying to posts created by the same person?"
    query = """
        MATCH (c:Comment)-[:commentHasCreator]->(p:Person)
              <-[:postHasCreator]-(post:Post)<-[:replyOfPost]-(c)
        RETURN COUNT(DISTINCT c.ID) > 0 AS has_self_reply;
    """
    return await _execute(session, 30, query)


QUERY_FUNCTIONS: dict[int, Callable[[AsyncSession], Awaitable[object]]] = {
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


async def main(selected: list[int] | None = None) -> None:
    if NEO4J_USER is None or NEO4J_PASSWORD is None:
        raise EnvironmentError("NEO4J_USER and NEO4J_PASSWORD must be set")

    start = time.perf_counter()
    if selected is None:
        selected = list(QUERY_FUNCTIONS.keys())

    async with AsyncGraphDatabase.driver(URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as driver:
        async with driver.session(database=NEO4J_DATABASE) as session:
            for idx in selected:
                func = QUERY_FUNCTIONS.get(idx)
                if func is None:
                    print(f"Skipping unknown query index: {idx}")
                    continue
                await func(session)

    elapsed = time.perf_counter() - start
    print(f"\nCompleted {len(selected)} query(ies) in {elapsed:.2f}s")


if __name__ == "__main__":
    selected_queries = _parse_selection(sys.argv[1:])
    asyncio.run(main(selected_queries))
