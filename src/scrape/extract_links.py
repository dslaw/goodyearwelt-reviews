"""Extract image links from submission bodies."""

from bs4 import BeautifulSoup
from itertools import chain, starmap
from typing import List, Tuple
import html
import logging
import sqlite3
import sys

from src.scrape.common import base_parser, insert_or_ignore, is_media_url, setup_logging
from src.scrape.models import Media


def get_submission_contents(cursor: sqlite3.Cursor) -> List[Tuple[str, str]]:
    cursor.execute(
        """
        select
            id,
            selftext_html
        from submissions
        where selftext_html is not null
        """
    )
    return cursor.fetchall()

def extract(submission_id: str, selftext_html: str) -> List[Media]:
    raw = html.unescape(selftext_html)
    soup = BeautifulSoup(raw, "html.parser")
    links = soup.find_all("a")

    medias = [
        Media(
            submission_id=submission_id,
            url=link.attrs["href"],
            # Link was found in post body, not metadata, so it is "indirect".
            is_direct=False,
            txt=link.text
        )
        for link in links
        if is_media_url(link.attrs["href"])
    ]
    return medias

def main() -> int:
    setup_logging()
    parser = base_parser(description=__doc__)
    args = parser.parse_args()

    conn = sqlite3.connect(args.conn)
    cursor = conn.cursor()
    logging.info("Established database connection")

    status = 0
    try:
        records = get_submission_contents(cursor)
        g = chain.from_iterable(starmap(extract, records))
        medias = list(g)
        for media in medias:
            insert_or_ignore(cursor, "medias", media)
    except sqlite3.Error as e:
        logging.error("Encountered error, aborting: %s", e)
        conn.rollback()
        status = 1
    else:
        conn.commit()
    finally:
        conn.close()
        logging.info("Finished extracting links")

    return status


if __name__ == "__main__":
    sys.exit(main())
