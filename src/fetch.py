"""Get Reddit submissions that match the search criteria."""

from dataclasses import dataclass, fields
from typing import Any, Dict, Iterator, List, Optional
import argparse
import logging
import requests
import sqlite3
import sys


logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s:%(module)s:%(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


MAX_LIMIT = 100
TIMEOUT = 60
SUBREDDIT = "goodyearwelt"
USER_AGENT = (
    "N/A:"                                                # Platform.
    "goodyearwelt-reviews:"                               # Name.
    "0.1.0 "                                              # Version.
    "(by https://github.com/dslaw/goodyearwelt-reviews)"  # Author.
)


@dataclass(frozen=True)
class Submission:
    id: str
    subreddit: str
    title: str
    author_fullname: str
    url: str
    created_utc: int
    selftext_html: str
    num_comments: int
    gilded: int
    downs: int
    ups: int
    score: int

    @classmethod
    def from_json(cls, **data):
        keys = [field.name for field in fields(cls)]
        kwargs = {key: data.get(key) for key in keys}
        return cls(**kwargs)

def search(subreddit: str, query: str, after: Optional[str] = None) -> requests.Response:  # noqa: E501
    url = f"https://reddit.com/r/{subreddit}/search.json"
    headers = {"User-Agent": USER_AGENT}

    params = {
        "q": query,
        "limit": MAX_LIMIT,
        "sort": "new",
        "restrict_sr": "true",
        "sr_detail": "false",
    }
    if after is not None:
        params.update({"after": after})

    response = requests.request(
        "GET",
        url,
        params=params,
        headers=headers,
        timeout=TIMEOUT
    )
    return response

def paginated_search(subreddit: str, query: str, after: Optional[str]) -> Iterator[requests.Response]:  # noqa: E501
    while True:
        response = search(subreddit, query, after=after)
        if not response.ok:
            logging.error(
                "Request failed with status %s and reason %s",
                response.status_code,
                response.reason
            )
            break

        yield response

        after = response.json()["data"]["after"]
        if after is None:
            break

    return

def extract_submissions(listing: Dict[str, Any]) -> List[Submission]:
    children = listing["data"].get("children", [])
    submissions = [Submission.from_json(**child["data"]) for child in children]
    return submissions

def insert_submission(cursor: sqlite3.Cursor, submission: Submission, query: str) -> None:
    facts_sql = \
        """
        insert or ignore into submission_facts
        (id, title, author_fullname, url, created_utc, search_query)
        values
        (?, ?, ?, ?, ?, ?)
        """
    cursor.execute(
        facts_sql, (
            submission.id,
            submission.title,
            submission.author_fullname,
            submission.url,
            submission.created_utc,
            query,
        )
    )

    sql =  \
        """
        insert or ignore into submissions
        (id, selftext_html, comments, gilded, downs, ups, score)
        values
        (?, ?, ?, ?, ?, ?, ?)
        """
    cursor.execute(
        sql, (
            submission.id,
            submission.selftext_html,
            submission.num_comments,
            submission.gilded,
            submission.downs,
            submission.ups,
            submission.score,
        )
    )

    return

def get_oldest_submission(cursor: sqlite3.Cursor, query: str) -> Optional[str]:
    # Search results are sorted by "new", so the least recent
    # saved result is where to resume from.
    sql = \
        """
        select id from submission_facts
        where search_query = ?
        order by created_utc asc
        limit 1
        """
    cursor.execute(sql, (query,))
    result = cursor.fetchone()
    submission_id: Optional[str] = result[0] if result is not None else None
    return submission_id

def ingest(cursor: sqlite3.Cursor, query: str, resume: bool) -> None:
    after = None
    if resume:
        submission_id = get_oldest_submission(cursor, query)
        if submission_id is not None:
            after = f"t3_{submission_id}"
            logging.info("Starting search from %s", after)
        else:
            logging.warning("No previous results to resume search from")

    responses = paginated_search(SUBREDDIT, query, after=after)
    for response in responses:
        listing = response.json()
        submissions = extract_submissions(listing)
        for submission in submissions:
            insert_submission(cursor, submission, query)

    return

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-q", "--query", type=str, help="Query string.")
    parser.add_argument("-c", "--conn", type=str, help="Database connection string.")
    parser.add_argument("-r", "--resume", action="store_true", help="Resume from saved.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.conn)
    cursor = conn.cursor()
    logging.info("Established database connection")

    status = 0
    try:
        logging.info("Starting ingest for %s", args.query)
        ingest(cursor, args.query, args.resume)
    except sqlite3.Error as e:
        logging.error("Encountered error, aborting: %s", e)
        conn.rollback()
        status = 1
    else:
        conn.commit()
    finally:
        conn.close()
        logging.info("Finished ingest for %s", args.query)

    return status


if __name__ == "__main__":
    sys.exit(main())
