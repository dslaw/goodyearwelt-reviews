"""Get Reddit submissions that match the search criteria."""

from typing import Any, Dict, Iterator, List, Tuple, Optional
import argparse
import logging
import requests
import sqlite3
import sys

from src.scrape.common import (
    from_json,
    insert_or_ignore,
    is_media_url,
)
from src.scrape.models import Media, Submission


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


def search(subreddit: str, query: str, after: Optional[str] = None) -> requests.Response:
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

def extract_submissions(listing: Dict[str, Any], subreddit: str, query: str) -> List[Tuple[Submission, Optional[Media]]]:  # noqa: E501
    children = listing["data"].get("children", [])
    modeled = []
    for child in children:
        data = {**child["data"], "search_query": query, "subreddit": subreddit}
        submission = from_json(Submission, **data)
        media: Optional[Media] = None
        if is_media_url(data["url"]):
            media = Media(
                submission_id=submission.id,
                url=data["url"],
                is_direct=True,
                txt=None
            )
        modeled.append((submission, media))

    return modeled

def ingest(cursor: sqlite3.Cursor, query: str, subreddit: str) -> None:
    responses = paginated_search(subreddit, query, after=None)
    for response in responses:
        listing = response.json()
        extracted = extract_submissions(listing, subreddit, query)
        for submission, media in extracted:
            insert_or_ignore(cursor, "submissions", submission)
            if media is not None:
                insert_or_ignore(cursor, "medias", media)

    return

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-c", "--conn", type=str, help="Database connection string.")
    parser.add_argument("-q", "--query", type=str, help="Query string.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.conn)
    cursor = conn.cursor()
    logging.info("Established database connection")

    status = 0
    try:
        logging.info("Starting ingest for %s", args.query)
        ingest(cursor, args.query, SUBREDDIT)
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
