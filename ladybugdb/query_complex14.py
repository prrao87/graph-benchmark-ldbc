"""Official LDBC SNB Interactive v1 query suite (complex Q1-Q14) for Ladybug.

Faithful port of::

    /data/ldbc_snb_interactive_v1_impls/cypher/queries/
        interactive-complex-{1..14}.cypher

to the Ladybug schema of ``ldbc_snb_sf1.lbdb``. Each query is ONE Cypher
statement with the official clause structure (same MATCH / OPTIONAL MATCH /
WITH / aggregation / ORDER BY / LIMIT shape). Only the following adaptations
are applied:

Schema adaptations (data-model differences, unavoidable):
- ``Person.id`` -> ``Person.ID``; ``KNOWS`` -> ``knows`` (stored directed,
  hence traversed undirected ``-[:knows]-`` everywhere).
- ``Message`` (Post|Comment superclass) -> ``Post``/``Comment`` branches
  combined with ``UNION ALL`` (Q2/Q8/Q9), or two ``OPTIONAL MATCH`` branches
  where a post-``UNION`` aggregation is required (Q3) or a post-``UNION``
  ``WITH`` (head-per-group) is required (Q7), since Ladybug parses
  ``UNION ALL`` followed only by ``ORDER BY``/``SKIP``/``LIMIT``.
- ``HAS_CREATOR`` -> ``postHasCreator`` / ``commentHasCreator``.
- ``REPLY_OF`` -> ``replyOfPost`` / ``replyOfComment``.
- ``LIKES`` (with ``like.creationDate``) -> ``likePost`` / ``likeComment``.
- ``HAS_TAG`` -> ``postHasTag`` / ``commentHasTag``; ``HAS_TYPE`` -> ``hasType``;
  ``IS_SUBCLASS_OF`` -> ``isSubclassOf``; ``TagClass`` -> ``Tagclass``.
- ``IS_LOCATED_IN`` -> ``personIsLocatedIn`` / ``postIsLocatedIn`` /
  ``commentIsLocatedIn`` / ``organisationIsLocatedIn``.
- ``IS_PART_OF`` -> ``isPartOf``; ``STUDY_AT`` -> ``studyAt``;
  ``WORK_AT`` -> ``workAt``; ``HAS_MEMBER`` -> ``hasMember``;
  ``CONTAINER_OF`` -> ``containerOf``; ``HAS_INTEREST`` -> ``hasInterest``.
- ``City``/``Country``/``Company``/``University`` labels -> ``Place.type``
  (``city``/``country``) / ``Organisation.type`` (``university``/``company``)
  inline label emulation (verified no-ops on this snapshot).
- Epoch-millis date params -> ``"YYYY-MM-DD HH:MM:SS"`` strings bound via
  ``TIMESTAMP($param)``.
- ``Person.email`` / ``Person.speaks`` do not exist in this snapshot, so Q1
  omits ``friendEmails``/``friendLanguages``; the ``[name, year, place]``
  university/company lists are returned as structs (Ladybug lists must be
  homogeneous), with the official ``CASE ... WHEN null`` null-row shape kept.
- ``Comment`` has no ``imageFile`` property, so Q7 resolves the message text
  per kind before the unified pipeline (``COALESCE(content, imageFile)`` for
  posts, ``content`` for comments); comments always carry content here.

Dialect adaptations (Ladybug openCypher subset, same clause shape):
- D1 ``shortestPath()`` function -> ``* SHORTEST`` pattern quantifier.
- D2 ``datetime({epochMillis: ...})`` -> ``date_part()`` on the ``DATE``
  ``birthday`` (same zodiac predicate on ``$month``).
- D3 ``head(collect(...))`` -> ordered ``WITH ... LIMIT 1e9`` +
  ``COLLECT(...)[1]`` (1-based indexing; ``HEAD()`` does not exist).
- D4 list comprehension ``size([p IN posts WHERE ...])`` -> ``UNWIND`` +
  ``OPTIONAL MATCH`` + ``COUNT(DISTINCT CASE WHEN ...)`` in the same single
  statement (list comprehensions do not exist).
- D5 ``allShortestPaths()`` / ``reduce()`` do not exist -> Q14 runs two
  statements: (1) shortest length via ``OPTIONAL MATCH ... SHORTEST`` +
  ``CASE WHEN ... IS NULL`` (official null shape kept); (2) ONE
  enumeration-plus-weighting statement (unrolled L-hop chain, per-edge reply
  counts via ``COUNT(DISTINCT ...)`` inside the DB, no per-edge round trips).
- D6 ``WITH ... ORDER BY`` requires a ``LIMIT`` -> ``LIMIT 1000000000``
  (no-op) where the official query orders without limiting.
- D7 ``CASE path IS NULL`` -> ``OPTIONAL MATCH`` + ``CASE WHEN e IS NULL``.

Parameters reuse the ``PARAMS`` table (official examples wherever they yield
results in SF1; official person IDs other than 143 do not exist here, so
verified members are used: Samir = highest degree; Rafael = his direct
friend).
"""

import sys
import time
from typing import Any, Callable

import ladybug as lb
from ladybug import Connection

SAMIR = 2199023262543  # Samir Al-Fayez: highest-degree person (814 friends)
RAFAEL = 2783  # Rafael Alonso, a direct friend of Samir

PARAMS: dict[int, dict[str, Any]] = {
    1: {"personId": SAMIR, "firstName": "Jose"},
    2: {"personId": SAMIR, "maxDate": "2010-10-16 12:00:00"},
    3: {
        "personId": SAMIR,
        "countryXName": "Angola",
        "countryYName": "Colombia",
        "startDate": "2010-06-01 12:00:00",
        "endDate": "2010-06-29 12:00:00",
        "durationDays": None,
    },
    4: {"personId": SAMIR, "startDate": "2010-06-01 00:00:00", "endDate": "2010-06-30 00:00:00"},
    5: {"personId": SAMIR, "minDate": "2010-11-01 12:00:00"},
    6: {"personId": SAMIR, "tagName": "Carl_Gustaf_Emil_Mannerheim"},
    7: {"personId": SAMIR},
    8: {"personId": 143},
    9: {"personId": SAMIR, "maxDate": "2010-11-16 12:00:00"},
    10: {"personId": SAMIR, "month": 5},
    11: {"personId": SAMIR, "countryName": "Hungary", "workFromYear": 2011},
    12: {"personId": SAMIR, "tagClassName": "Monarch"},
    13: {"person1Id": SAMIR, "person2Id": RAFAEL},
    14: {"person1Id": SAMIR, "person2Id": RAFAEL},
}


def _execute(conn: Connection, idx: int, query: str, params: dict[str, Any] | None = None):
    bound = dict(params or {})
    bound.setdefault("dummy", 0)
    print(f"\nQuery {idx}  parameters: " + ", ".join(f"${k}={v!r}" for k, v in bound.items() if k != "dummy"))
    print(f"Query {idx}:\n{query}")
    response = conn.execute(query, bound)
    result = response.get_as_pl()  # type: ignore
    print(result)
    response.close()
    return result


def _get(conn: Connection, query: str, params: dict[str, Any]):
    """Lightweight fetch (list of rows) for the Q14 length probe."""
    response = conn.execute(query, params)
    try:
        return response.get_all()
    finally:
        response.close()


# ---------------------------------------------------------------------------
# Q1. Transitive friends with certain name (official: complex-1).
# ---------------------------------------------------------------------------
def run_query1(conn: Connection, personId: int | None = None, firstName: str | None = None):
    """Q1. Transitive (1-3 hop) friends of $personId named $firstName.

    Official shape: cartesian (person, friend) + shortestPath + min(length),
    then MATCH city + 2x OPTIONAL MATCH (uni/company) + RETURN, ORDER BY/LIMIT.
    """
    p = PARAMS[1].copy()
    if personId is not None:
        p["personId"] = personId
    if firstName is not None:
        p["firstName"] = firstName
    query = """
        MATCH (p:Person {ID: $personId}), (friend:Person {firstName: $firstName})
        WHERE p.ID <> friend.ID
        WITH p, friend
        MATCH (p)-[path:knows* SHORTEST 1..3]-(friend)
        WITH MIN(LENGTH(path)) AS distance, friend
        ORDER BY distance ASC, friend.lastName ASC, friend.ID ASC
        LIMIT 20
        MATCH (friend)-[:personIsLocatedIn]->(friendCity:Place {type: 'city'})
        OPTIONAL MATCH (friend)-[studyAt:studyAt]->(uni:Organisation {type: 'university'})
                           -[:organisationIsLocatedIn]->(uniCity:Place {type: 'city'})
        WITH friend, friendCity, distance,
             COLLECT(CASE WHEN uni IS NULL THEN NULL
                          ELSE {universityName: uni.name, classYear: studyAt.classYear,
                                cityName: uniCity.name} END) AS unis
        OPTIONAL MATCH (friend)-[workAt:workAt]->(company:Organisation {type: 'company'})
                           -[:organisationIsLocatedIn]->(companyCountry:Place {type: 'country'})
        WITH friend, friendCity, distance, unis,
             COLLECT(CASE WHEN company IS NULL THEN NULL
                          ELSE {companyName: company.name, workFrom: workAt.workFrom,
                                countryName: companyCountry.name} END) AS companies
        RETURN friend.ID AS friendId, friend.lastName AS friendLastName,
               distance AS distanceFromPerson, friend.birthday AS friendBirthday,
               friend.creationDate AS friendCreationDate, friend.gender AS friendGender,
               friend.browserUsed AS friendBrowserUsed, friend.locationIP AS friendLocationIp,
               friendCity.name AS friendCityName, unis AS friendUniversities,
               companies AS friendCompanies
        ORDER BY distanceFromPerson ASC, friendLastName ASC, friendId ASC
        LIMIT 20;
    """
    return _execute(conn, 1, query, p)


# ---------------------------------------------------------------------------
# Q2. Recent messages by your friends (official: complex-2).
# ---------------------------------------------------------------------------
def run_query2(conn: Connection, personId: int | None = None, maxDate: str | None = None):
    """Q2. Recent posts+comments of friends of $personId, created <= $maxDate.

    Official shape: single MATCH/WHERE/RETURN + ORDER BY/LIMIT; Message split
    into Post/Comment UNION ALL branches (schema), DB-level ORDER BY/LIMIT.
    """
    p = PARAMS[2].copy()
    if personId is not None:
        p["personId"] = personId
    if maxDate is not None:
        p["maxDate"] = maxDate
    query = """
        MATCH (:Person {ID: $personId})-[:knows]-(friend:Person)<-[:postHasCreator]-(message:Post)
        WHERE message.creationDate <= TIMESTAMP($maxDate)
        RETURN friend.ID AS personId, friend.firstName AS personFirstName,
               friend.lastName AS personLastName, message.ID AS postOrCommentId,
               COALESCE(message.content, message.imageFile) AS postOrCommentContent,
               message.creationDate AS postOrCommentCreationDate
        UNION ALL
        MATCH (:Person {ID: $personId})-[:knows]-(friend:Person)<-[:commentHasCreator]-(message:Comment)
        WHERE message.creationDate <= TIMESTAMP($maxDate)
        RETURN friend.ID AS personId, friend.firstName AS personFirstName,
               friend.lastName AS personLastName, message.ID AS postOrCommentId,
               message.content AS postOrCommentContent,
               message.creationDate AS postOrCommentCreationDate
        ORDER BY postOrCommentCreationDate DESC, postOrCommentId ASC
        LIMIT 20;
    """
    return _execute(conn, 2, query, p)


# ---------------------------------------------------------------------------
# Q3. Friends/FoF that have been to given countries (official: complex-3).
# ---------------------------------------------------------------------------
def run_query3(
    conn: Connection,
    personId: int | None = None,
    countryXName: str | None = None,
    countryYName: str | None = None,
    startDate: str | None = None,
    endDate: str | None = None,
    durationDays: int | None = None,
):
    """Q3. Friends/FoF of $personId with messages in $countryXName AND $countryYName.

    Official shape: country/city preamble + knows neighbourhood with home-city
    exclusion + message match + grouped counts + ORDER BY/LIMIT, one statement.
    Post/Comment kinds are two OPTIONAL MATCH branches (UNION cannot feed an
    aggregation in this dialect), aggregated in successive WITH steps.
    """
    from datetime import datetime, timedelta

    p = PARAMS[3].copy()
    if personId is not None:
        p["personId"] = personId
    if countryXName is not None:
        p["countryXName"] = countryXName
    if countryYName is not None:
        p["countryYName"] = countryYName
    if startDate is not None:
        p["startDate"] = startDate
    if endDate is not None:
        p["endDate"] = endDate
        p["durationDays"] = None
    if durationDays is not None:
        p["durationDays"] = durationDays
    if p.get("durationDays"):
        computed = (datetime.strptime(p["startDate"], "%Y-%m-%d %H:%M:%S") + timedelta(days=p["durationDays"])).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        print(f"[Q3] window: startDate={p['startDate']} + {p['durationDays']} days -> endDate={computed}")
        p["endDate"] = computed
    bound = {k: p[k] for k in ("personId", "countryXName", "countryYName", "startDate", "endDate")}
    query = """
        MATCH (countryX:Place {name: $countryXName}), (countryY:Place {name: $countryYName}),
              (person:Person {ID: $personId})
        WITH person, countryX, countryY
        LIMIT 1
        MATCH (city:Place)-[:isPartOf]->(country:Place)
        WHERE country IN [countryX, countryY]
        WITH person, countryX, countryY, COLLECT(city) AS cities
        MATCH (person)-[:knows*1..2]-(friend:Person)-[:personIsLocatedIn]->(fcity:Place)
        WHERE friend.ID <> person.ID AND NOT fcity IN cities
        WITH DISTINCT friend, countryX, countryY
        OPTIONAL MATCH (friend)<-[:postHasCreator]-(ppost:Post)-[:postIsLocatedIn]->(pcountry:Place)
        WHERE ppost.creationDate >= TIMESTAMP($startDate)
          AND ppost.creationDate < TIMESTAMP($endDate)
          AND pcountry IN [countryX, countryY]
        WITH friend, countryX, countryY,
             SUM(CASE WHEN pcountry = countryX THEN 1 ELSE 0 END) AS postX,
             SUM(CASE WHEN pcountry = countryY THEN 1 ELSE 0 END) AS postY
        OPTIONAL MATCH (friend)<-[:commentHasCreator]-(cmt:Comment)-[:commentIsLocatedIn]->(ccountry:Place)
        WHERE cmt.creationDate >= TIMESTAMP($startDate)
          AND cmt.creationDate < TIMESTAMP($endDate)
          AND ccountry IN [countryX, countryY]
        WITH friend, postX, postY,
             SUM(CASE WHEN ccountry = countryX THEN 1 ELSE 0 END) AS cmtX,
             SUM(CASE WHEN ccountry = countryY THEN 1 ELSE 0 END) AS cmtY
        WITH friend, postX + cmtX AS xCount, postY + cmtY AS yCount
        WHERE xCount > 0 AND yCount > 0
        RETURN friend.ID AS friendId, friend.firstName AS friendFirstName,
               friend.lastName AS friendLastName, xCount, yCount,
               xCount + yCount AS xyCount
        ORDER BY xyCount DESC, friendId ASC
        LIMIT 20;
    """
    return _execute(conn, 3, query, bound)


# ---------------------------------------------------------------------------
# Q4. New topics (official: complex-4).
# ---------------------------------------------------------------------------
def run_query4(
    conn: Connection,
    personId: int | None = None,
    startDate: str | None = None,
    endDate: str | None = None,
):
    """Q4. Tags used on friends' posts only within [$startDate, $endDate).

    Official shape: DISTINCT tag/post + valid/inValid CASE + SUM/GROUP +
    HAVING + ORDER BY/LIMIT.
    """
    p = PARAMS[4].copy()
    if personId is not None:
        p["personId"] = personId
    if startDate is not None:
        p["startDate"] = startDate
    if endDate is not None:
        p["endDate"] = endDate
    query = """
        MATCH (person:Person {ID: $personId})-[:knows]-(friend:Person),
              (friend)<-[:postHasCreator]-(post:Post)-[:postHasTag]->(tag:Tag)
        WITH DISTINCT tag, post
        WITH tag,
             CASE WHEN post.creationDate >= TIMESTAMP($startDate)
                   AND post.creationDate < TIMESTAMP($endDate) THEN 1 ELSE 0 END AS valid,
             CASE WHEN post.creationDate < TIMESTAMP($startDate) THEN 1 ELSE 0 END AS inValid
        WITH tag, SUM(valid) AS postCount, SUM(inValid) AS inValidPostCount
        WHERE postCount > 0 AND inValidPostCount = 0
        RETURN tag.name AS tagName, postCount
        ORDER BY postCount DESC, tagName ASC
        LIMIT 10;
    """
    return _execute(conn, 4, query, p)


# ---------------------------------------------------------------------------
# Q5. New groups (official: complex-5).
# ---------------------------------------------------------------------------
def run_query5(conn: Connection, personId: int | None = None, minDate: str | None = None):
    """Q5. Forums friends/FoF joined after $minDate, ranked by their posts.

    Official shape: neighbourhood + member match + collect + OPTIONAL
    post match with IN-friends filter + COUNT/GROUP + ORDER BY/LIMIT.
    """
    p = PARAMS[5].copy()
    if personId is not None:
        p["personId"] = personId
    if minDate is not None:
        p["minDate"] = minDate
    query = """
        MATCH (person:Person {ID: $personId})-[:knows*1..2]-(friend:Person)
        WHERE NOT person = friend
        WITH DISTINCT friend
        MATCH (friend)<-[membership:hasMember]-(forum:Forum)
        WHERE membership.joinDate > TIMESTAMP($minDate)
        WITH forum, COLLECT(friend) AS friends
        OPTIONAL MATCH (author:Person)<-[:postHasCreator]-(post:Post)<-[:containerOf]-(forum)
        WHERE author IN friends
        WITH forum, COUNT(post) AS postCount
        RETURN forum.title AS forumName, postCount
        ORDER BY postCount DESC, forum.ID ASC
        LIMIT 20;
    """
    return _execute(conn, 5, query, p)


# ---------------------------------------------------------------------------
# Q6. Tag co-occurrence (official: complex-6).
# ---------------------------------------------------------------------------
def run_query6(conn: Connection, personId: int | None = None, tagName: str | None = None):
    """Q6. Tags co-occurring with $tagName on friends/FoF posts.

    Official shape: anchor tag id + neighbourhood collect + UNWIND + MATCH +
    GROUP + ORDER BY/LIMIT.
    """
    p = PARAMS[6].copy()
    if personId is not None:
        p["personId"] = personId
    if tagName is not None:
        p["tagName"] = tagName
    query = """
        MATCH (knownTag:Tag {name: $tagName})
        WITH knownTag.ID AS knownTagId
        MATCH (person:Person {ID: $personId})-[:knows*1..2]-(friend:Person)
        WHERE NOT person = friend
        WITH knownTagId, COLLECT(DISTINCT friend) AS friends
        UNWIND friends AS f
        MATCH (f)<-[:postHasCreator]-(post:Post),
              (post)-[:postHasTag]->(t:Tag {ID: knownTagId}),
              (post)-[:postHasTag]->(tag:Tag)
        WHERE NOT t = tag
        WITH tag.name AS tagName, COUNT(post) AS postCount
        RETURN tagName, postCount
        ORDER BY postCount DESC, tagName ASC
        LIMIT 10;
    """
    return _execute(conn, 6, query, p)


# ---------------------------------------------------------------------------
# Q7. Recent likers (official: complex-7).
# ---------------------------------------------------------------------------
def run_query7(conn: Connection, personId: int | None = None):
    """Q7. Most recent likers of $personId's posts+comments (one row per liker).

    Official shape: MATCH/ORDER BY/head(collect)/RETURN/ORDER BY/LIMIT.
    Post/Comment like kinds are collected per kind then concatenated and
    unnested (UNION cannot feed the head-per-group WITH in this dialect);
    head() is COLLECT(...)[1]; minutes come from the like-message interval.
    """
    p = PARAMS[7].copy()
    if personId is not None:
        p["personId"] = personId
    query = """
        MATCH (person:Person {ID: $personId})
        OPTIONAL MATCH (person)<-[:postHasCreator]-(pmsg:Post)<-[plike:likePost]-(pliker:Person)
        WITH person, COLLECT({likerId: pliker.ID, msgId: pmsg.ID,
                              msgContent: COALESCE(pmsg.content, pmsg.imageFile),
                              msgTime: pmsg.creationDate,
                              likeTime: plike.creationDate}) AS prows
        OPTIONAL MATCH (person)<-[:commentHasCreator]-(cmsg:Comment)<-[clike:likeComment]-(cliker:Person)
        WITH person, prows, COLLECT({likerId: cliker.ID, msgId: cmsg.ID,
                                     msgContent: cmsg.content,
                                     msgTime: cmsg.creationDate,
                                     likeTime: clike.creationDate}) AS crows
        WITH person, prows + crows AS rows
        UNWIND rows AS r
        WITH person, r WHERE r.likerId IS NOT NULL
        WITH r.likerId AS likerId, r.msgId AS msgId, r.msgContent AS msgContent,
             r.msgTime AS msgTime, r.likeTime AS likeTime, person
        MATCH (liker:Person {ID: likerId})
        OPTIONAL MATCH (liker)-[k:knows]-(person)
        WITH liker, msgId, msgContent, msgTime, likeTime, person, (k IS NULL) AS isNew0
        ORDER BY likeTime DESC, msgId ASC
        LIMIT 1000000000
        WITH liker, COLLECT({msgId: msgId, msgContent: msgContent, msgTime: msgTime,
                             likeTime: likeTime, isNew: isNew0})[1] AS latestLike, person
        RETURN liker.ID AS personId, liker.firstName AS personFirstName,
               liker.lastName AS personLastName,
               latestLike.likeTime AS likeCreationDate,
               latestLike.msgId AS commentOrPostId,
               latestLike.msgContent AS commentOrPostContent,
               (date_part('day', latestLike.likeTime - latestLike.msgTime) * 1440
                + date_part('hour', latestLike.likeTime - latestLike.msgTime) * 60
                + date_part('minute', latestLike.likeTime - latestLike.msgTime)) AS minutesLatency,
               latestLike.isNew AS isNew
        ORDER BY likeCreationDate DESC, personId ASC
        LIMIT 20;
    """
    return _execute(conn, 7, query, {"personId": p["personId"]})


# ---------------------------------------------------------------------------
# Q8. Recent replies (official: complex-8).
# ---------------------------------------------------------------------------
def run_query8(conn: Connection, personId: int | None = None):
    """Q8. Most recent reply comments to $personId's posts+comments.

    Official shape: single MATCH/RETURN/ORDER BY/LIMIT over REPLY_OF;
    replyOfPost/replyOfComment are UNION ALL branches (schema).
    """
    p = PARAMS[8].copy()
    if personId is not None:
        p["personId"] = personId
    query = """
        MATCH (start:Person {ID: $personId})<-[:postHasCreator]-(post:Post)
              <-[:replyOfPost]-(comment:Comment)-[:commentHasCreator]->(person:Person)
        RETURN person.ID AS personId, person.firstName AS personFirstName,
               person.lastName AS personLastName,
               comment.creationDate AS commentCreationDate, comment.ID AS commentId,
               comment.content AS commentContent
        UNION ALL
        MATCH (start:Person {ID: $personId})<-[:commentHasCreator]-(parent:Comment)
              <-[:replyOfComment]-(comment:Comment)-[:commentHasCreator]->(person:Person)
        RETURN person.ID AS personId, person.firstName AS personFirstName,
               person.lastName AS personLastName,
               comment.creationDate AS commentCreationDate, comment.ID AS commentId,
               comment.content AS commentContent
        ORDER BY commentCreationDate DESC, commentId ASC
        LIMIT 20;
    """
    return _execute(conn, 8, query, p)


# ---------------------------------------------------------------------------
# Q9. Recent messages by friends/FoF (official: complex-9).
# ---------------------------------------------------------------------------
def run_query9(conn: Connection, personId: int | None = None, maxDate: str | None = None):
    """Q9. Recent posts+comments of friends/FoF of $personId, before $maxDate.

    Official shape: neighbourhood collect + UNWIND + message MATCH +
    RETURN/ORDER BY/LIMIT; Post/Comment are UNION ALL branches (schema).
    """
    p = PARAMS[9].copy()
    if personId is not None:
        p["personId"] = personId
    if maxDate is not None:
        p["maxDate"] = maxDate
    query = """
        MATCH (root:Person {ID: $personId})-[:knows*1..2]-(friend:Person)
        WHERE NOT friend = root
        WITH COLLECT(DISTINCT friend) AS friends
        UNWIND friends AS f
        MATCH (f)<-[:postHasCreator]-(message:Post)
        WHERE message.creationDate < TIMESTAMP($maxDate)
        RETURN f.ID AS personId, f.firstName AS personFirstName,
               f.lastName AS personLastName, message.ID AS commentOrPostId,
               COALESCE(message.content, message.imageFile) AS commentOrPostContent,
               message.creationDate AS commentOrPostCreationDate
        UNION ALL
        MATCH (root:Person {ID: $personId})-[:knows*1..2]-(friend:Person)
        WHERE NOT friend = root
        WITH COLLECT(DISTINCT friend) AS friends
        UNWIND friends AS f
        MATCH (f)<-[:commentHasCreator]-(message:Comment)
        WHERE message.creationDate < TIMESTAMP($maxDate)
        RETURN f.ID AS personId, f.firstName AS personFirstName,
               f.lastName AS personLastName, message.ID AS commentOrPostId,
               message.content AS commentOrPostContent,
               message.creationDate AS commentOrPostCreationDate
        ORDER BY commentOrPostCreationDate DESC, commentOrPostId ASC
        LIMIT 20;
    """
    return _execute(conn, 9, query, p)


# ---------------------------------------------------------------------------
# Q10. Friend recommendation (official: complex-10).
# ---------------------------------------------------------------------------
def run_query10(conn: Connection, personId: int | None = None, month: int | None = None):
    """Q10. Friend-of-friend recommender for $personId.

    Official shape: 2-hop + city MATCH, zodiac filter, DISTINCT,
    OPTIONAL post match, collect/size/common counts, RETURN/ORDER BY/LIMIT.
    The post-list size filter is a second OPTIONAL MATCH + COUNT DISTINCT
    in the same statement (list comprehensions do not exist here).
    """
    p = PARAMS[10].copy()
    if personId is not None:
        p["personId"] = personId
    if month is not None:
        p["month"] = month
    query = """
        MATCH (person:Person {ID: $personId})-[:hasInterest]->(i:Tag)
        WITH person, COLLECT(i) AS interests
        MATCH (person)-[:knows*2..2]-(friend:Person),
              (friend)-[:personIsLocatedIn]->(city:Place {type: 'city'})
        WHERE NOT friend = person AND NOT (friend)-[:knows]-(person)
        WITH person, city, friend, interests, friend.birthday AS birthday
        WHERE (date_part('month', birthday) = $month AND date_part('day', birthday) >= 21)
           OR (date_part('month', birthday) = ($month % 12) + 1 AND date_part('day', birthday) < 22)
        WITH DISTINCT friend, city, person, interests
        OPTIONAL MATCH (friend)<-[:postHasCreator]-(post:Post)
        OPTIONAL MATCH (post)-[:postHasTag]->(t:Tag) WHERE t IN interests
        WITH friend, city, COUNT(DISTINCT post) AS postCount,
             COUNT(DISTINCT CASE WHEN t IS NOT NULL THEN post END) AS commonPostCount
        RETURN friend.ID AS personId, friend.firstName AS personFirstName,
               friend.lastName AS personLastName,
               commonPostCount - (postCount - commonPostCount) AS commonInterestScore,
               friend.gender AS personGender, city.name AS personCityName
        ORDER BY commonInterestScore DESC, personId ASC
        LIMIT 10;
    """
    return _execute(conn, 10, query, p)


# ---------------------------------------------------------------------------
# Q11. Job referral (official: complex-11).
# ---------------------------------------------------------------------------
def run_query11(
    conn: Connection,
    personId: int | None = None,
    countryName: str | None = None,
    workFromYear: int | None = None,
):
    """Q11. Friends/FoF who started at a $countryName company before $workFromYear.

    Official shape: neighbourhood DISTINCT + work/company/country MATCH +
    RETURN/ORDER BY/LIMIT (:Company emulated via Organisation type).
    """
    p = PARAMS[11].copy()
    if personId is not None:
        p["personId"] = personId
    if countryName is not None:
        p["countryName"] = countryName
    if workFromYear is not None:
        p["workFromYear"] = workFromYear
    query = """
        MATCH (person:Person {ID: $personId})-[:knows*1..2]-(friend:Person)
        WHERE NOT person = friend
        WITH DISTINCT friend
        MATCH (friend)-[workAt:workAt]->(company:Organisation {type: 'company'})
                  -[:organisationIsLocatedIn]->(country:Place {name: $countryName, type: 'country'})
        WHERE workAt.workFrom < $workFromYear
        RETURN friend.ID AS personId, friend.firstName AS personFirstName,
               friend.lastName AS personLastName, company.name AS organizationName,
               workAt.workFrom AS organizationWorkFromYear
        ORDER BY organizationWorkFromYear ASC, personId ASC, organizationName DESC
        LIMIT 10;
    """
    return _execute(conn, 11, query, p)


# ---------------------------------------------------------------------------
# Q12. Expert search (official: complex-12).
# ---------------------------------------------------------------------------
def run_query12(conn: Connection, personId: int | None = None, tagClassName: str | None = None):
    """Q12. Friends of $personId replying most to $tagClassName posts.

    Official shape: single statement; tag hierarchy via the
    HAS_TYPE|IS_SUBCLASS_OF alternation + collect, then friend/reply/post/tag
    MATCH with IN-tags filter + GROUP/ORDER BY/LIMIT.
    """
    p = PARAMS[12].copy()
    if personId is not None:
        p["personId"] = personId
    if tagClassName is not None:
        p["tagClassName"] = tagClassName
    query = """
        MATCH (tag:Tag)-[:hasType|isSubclassOf*0..]->(baseTagClass:Tagclass)
        WHERE tag.name = $tagClassName OR baseTagClass.name = $tagClassName
        WITH COLLECT(tag.ID) AS tags
        MATCH (:Person {ID: $personId})-[:knows]-(friend:Person)
              <-[:commentHasCreator]-(comment:Comment)-[:replyOfPost]->(:Post)-[:postHasTag]->(tag:Tag)
        WHERE tag.ID IN tags
        RETURN friend.ID AS personId, friend.firstName AS personFirstName,
               friend.lastName AS personLastName,
               COLLECT(DISTINCT tag.name) AS tagNames,
               COUNT(DISTINCT comment) AS replyCount
        ORDER BY replyCount DESC, personId ASC
        LIMIT 20;
    """
    return _execute(conn, 12, query, p)


# ---------------------------------------------------------------------------
# Q13. Single shortest path (official: complex-13).
# ---------------------------------------------------------------------------
def run_query13(conn: Connection, person1Id: int | None = None, person2Id: int | None = None):
    """Q13. Shortest knows-path length between $person1Id and $person2Id.

    Official shape: single statement with null-path CASE (unbounded hop
    range; SHORTEST is the dialect spelling of shortestPath).
    """
    p = PARAMS[13].copy()
    if person1Id is not None:
        p["person1Id"] = person1Id
    if person2Id is not None:
        p["person2Id"] = person2Id
    query = """
        OPTIONAL MATCH (person1:Person {ID: $person1Id})-[path:knows* SHORTEST]-(person2:Person {ID: $person2Id})
        RETURN CASE WHEN path IS NULL THEN -1 ELSE LENGTH(path) END AS shortestPathLength;
    """
    return _execute(conn, 13, query, p)


# ---------------------------------------------------------------------------
# Q14. Trusted connection paths (official: complex-14).
# ---------------------------------------------------------------------------
def run_query14(conn: Connection, person1Id: int | None = None, person2Id: int | None = None):
    """Q14. All shortest knows-paths between $person1Id and $person2Id, weighted.

    Official shape: allShortestPaths + per-edge reply weights (1.0 post,
    0.5 comment, both directions) + ORDER BY weight DESC. allShortestPaths /
    reduce() do not exist in this dialect, so: (1) one length probe (same
    null-shape as Q13); (2) ONE enumeration-plus-weighting statement with an
    unrolled L-hop chain and DB-side COUNT(DISTINCT ...) weights.
    """
    p = PARAMS[14].copy()
    if person1Id is not None:
        p["person1Id"] = person1Id
    if person2Id is not None:
        p["person2Id"] = person2Id
    x, y = p["person1Id"], p["person2Id"]

    rows = _get(
        conn,
        "OPTIONAL MATCH (a:Person {ID: $person1Id})-[e:knows* SHORTEST]-(b:Person {ID: $person2Id})"
        " RETURN CASE WHEN e IS NULL THEN -1 ELSE LENGTH(e) END AS shortestPathLength;",
        {"person1Id": x, "person2Id": y, "dummy": 0},
    )
    length = rows[0][0] if rows else -1
    print(f"\nQuery 14  parameters: $person1Id={x!r}, $person2Id={y!r}")
    if length < 0:
        print("Query 14: no knows-path between the two persons.")
        return rows
    print(f"Query 14: shortest length = {length}; enumerating + weighting in one statement ...")

    nodes = ["a"] + [f"n{i}" for i in range(1, length)] + ["b"]
    chain = "(a:Person {ID: $person1Id})"
    for i in range(length):
        nxt = nodes[i + 1]
        # intermediate nodes are plain :Person; endpoint carries the ID filter
        if i == length - 1:
            chain += f"-[:knows]-({nxt}:Person {{ID: $person2Id}})"
        else:
            chain += f"-[:knows]-({nxt}:Person)"
    conds = []
    if length > 1:
        mids = nodes[1:-1]
        for m in mids:
            conds.append(f"{m}.ID <> a.ID AND {m}.ID <> b.ID")
        for i, m1 in enumerate(mids):
            for m2 in mids[i + 1:]:
                conds.append(f"{m1}.ID <> {m2}.ID")
    match = f"MATCH {chain}"
    if conds:
        match += " WHERE " + " AND ".join(conds)

    optionals = []
    counts = []
    for i in range(length):
        u, v = nodes[i], nodes[i + 1]
        optionals.append(
            f"OPTIONAL MATCH (cpa{i}:Comment)-[:commentHasCreator]->({u}),"
            f" (cpa{i})-[:replyOfPost]->(ppa{i}:Post)-[:postHasCreator]->({v})"
        )
        optionals.append(
            f"OPTIONAL MATCH (cpb{i}:Comment)-[:commentHasCreator]->({v}),"
            f" (cpb{i})-[:replyOfPost]->(ppb{i}:Post)-[:postHasCreator]->({u})"
        )
        optionals.append(
            f"OPTIONAL MATCH (cca{i}:Comment)-[:commentHasCreator]->({u}),"
            f" (cca{i})-[:replyOfComment]->(pca{i}:Comment)-[:commentHasCreator]->({v})"
        )
        optionals.append(
            f"OPTIONAL MATCH (ccb{i}:Comment)-[:commentHasCreator]->({v}),"
            f" (ccb{i})-[:replyOfComment]->(pcb{i}:Comment)-[:commentHasCreator]->({u})"
        )
        counts.append(f"COUNT(DISTINCT cpa{i}) + COUNT(DISTINCT cpb{i})")
        counts.append(f"0.5 * (COUNT(DISTINCT cca{i}) + COUNT(DISTINCT ccb{i}))")
    path_ids = "[" + ", ".join(f"{n}.ID" for n in nodes) + "]"
    weight = " + ".join(counts) if counts else "0.0"
    query = (
        f"{match}\n"
        + "\n".join(optionals)
        + f"\nWITH {path_ids} AS personIdsInPath, ({weight}) AS pathWeight"
        + "\nRETURN personIdsInPath, pathWeight ORDER BY pathWeight DESC;"
    )
    return _execute(conn, 14, query, {"person1Id": x, "person2Id": y})


QUERY_FUNCTIONS: dict[int, Callable[..., object]] = {
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


def main(conn: Connection, selected: list[int] | None = None) -> None:
    start = time.perf_counter()
    if selected is None:
        selected = list(QUERY_FUNCTIONS.keys())
    for idx in selected:
        func = QUERY_FUNCTIONS.get(idx)
        if func is None:
            print(f"Skipping unknown query index: {idx}")
            continue
        func(conn)
    elapsed = time.perf_counter() - start
    print(f"\nCompleted {len(selected)} query(ies) in {elapsed:.2f}s")


if __name__ == "__main__":
    DB_NAME = "ldbc_snb_sf1.lbdb"
    db = lb.Database(f"./{DB_NAME}")
    conn = lb.Connection(db)
    selected_queries = _parse_selection(sys.argv[1:])
    main(conn, selected_queries)
