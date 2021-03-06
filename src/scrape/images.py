"""Get linked images and album metadata."""

from pathlib import Path
from typing import Any, Dict, List, MutableMapping, Optional, Tuple
from urllib.parse import urlparse, urlunparse
import logging
import requests
import sqlite3
import sys

from src.scrape.common import base_parser, from_json, insert_or_ignore, setup_logging
from src.scrape.models import Album, Image


IMGUR_API_VERSION = 3


class RateLimitError(Exception):
    pass

class Client(object):
    fail_on_statuses = (401, 403)
    timeout = 60

    @property
    def headers(self) -> Optional[Dict[str, str]]:
        return None

    def on_failure(self, response: requests.Response) -> None:
        if response.status_code in self.fail_on_statuses:
            response.raise_for_status()

        logging.error(
            "Unable to get %s with status %s and reason %s",
            response.url,
            response.status_code,
            response.reason
        )

    def get_image(self, url: str, **metadata) -> Image:
        response = requests.request(
            "GET",
            url,
            headers=self.headers,
            timeout=self.timeout
        )
        if not response.ok:
            self.on_failure(response)
            logging.warning("Returning image for %s as only metadata", url)

        content = response.content if response.ok else None
        mimetype = response.headers["Content-Type"] if response.ok else None

        updated_data = {
            **metadata,
            "type": mimetype,
            "img": content,
            "link": url,
        }
        return from_json(Image, **updated_data)

class ImgurClient(Client):
    min_stopping_credits = 3

    def __init__(self, client_id: str) -> None:
        self.client_id = client_id

    @property
    def headers(self) -> Dict[str, str]:
        return {"Authorization": f"Client-ID {self.client_id}"}

    def near_rate_limit(self, headers: MutableMapping[str, str]) -> bool:
        def hit_threshold(who: str) -> bool:
            credits = headers.get(f"X-RateLimit-{who}Remaining")
            if credits is None:
                return False
            return int(credits) <= self.min_stopping_credits

        return hit_threshold("User") or hit_threshold("Client")

    def on_failure(self, response: requests.Response) -> None:
        if response.status_code == 429:
            raise RateLimitError("Rate limited by Imgur")

        super().on_failure(response)
        return None

    def get_image(self, url: str, **metadata) -> Image:
        # Certain subdomain(s) reject requests with the authorization header
        # set (specifically, `i.imgur.com`). Removing the authorization
        # header and keeping the subdomain also works.
        #
        # XXX: `url` cannot be a request to the API (eg
        # `api.imgur.com/3/image/{hash}`) or this will fail.
        url = strip_imgur_subdomain(url)
        return super().get_image(url, **metadata)

    def get_json(self, url: str) -> Optional[Any]:
        headers = {**self.headers, "Accept": "application/json"}
        response = requests.request("GET", url, headers=headers, timeout=self.timeout)
        if not response.ok:
            self.on_failure(response)
            return None

        # Check if we're near to the rate limit - if it is hit too many
        # times, Imgur will penalize the client account.
        if self.near_rate_limit(response.headers):
            raise RateLimitError(f"Stopping before rate limit reached")

        return response.json()

    def get_album(self, url: str, media_id: int) -> Optional[Tuple[Album, List[Image]]]:
        wrapped: Optional[Dict[str, Any]] = self.get_json(url)
        if wrapped is None:
            return None

        data = wrapped["data"]
        album = from_json(Album, **data, media_id=media_id)
        images = []
        for img in data["images"]:
            img_url: str = img["link"]
            metadata = {**img, "album_id": album.id, "media_id": media_id}
            image = self.get_image(img_url, **metadata)
            images.append(image)
        return album, images

def sniff_imgur_resource(url: str) -> Optional[str]:
    parsed = urlparse(url)
    # Filter in case there is a trailing slash.
    slugs = [slug for slug in parsed.path.split("/") if slug]

    if len(slugs) == 1:
        # If the path has only one component, it should be a direct
        # link to an image, either the HTML page (no extension) or
        # the image (extension). In either case, the hash/id can be
        # extracted from the path.
        return "image"

    resource_type, *_ = slugs
    known_resource_types = {
        "a": "album",
        "gallery": "gallery",
        "image": "image",
    }
    return known_resource_types.get(resource_type)

def get_id(url: str) -> Optional[str]:
    # NB: This only holds for the urls we are interested in, not in the general
    #     case.
    parsed = urlparse(url)
    *_, end = [slug for slug in parsed.path.split("/") if slug]
    id_, *_ = end.split("#")  # Split off anchor link if it exists.
    return Path(id_).stem  # Remove extension if it exists.

def make_imgur_url(resource_type: str, hash_: str) -> str:
    api_url = f"https://api.imgur.com/{IMGUR_API_VERSION}/{resource_type}/{hash_}"
    return api_url

def strip_imgur_subdomain(url: str) -> str:
    parsed = urlparse(url)
    parts = parsed.netloc.split(".")
    if len(parts) >= 2 and parts[-2] == "imgur" and parts[-1] == "com":
        replaced = parsed._replace(netloc="imgur.com")
        return urlunparse(replaced)
    return url

def is_imgur(url: str) -> bool:
    parsed = urlparse(url)
    return "imgur" in parsed.netloc

def is_album(url: str) -> bool:
    if not is_imgur(url):
        # Reddit does not host albums afaik.
        return False

    # It appears that treating a gallery as an album is okay.
    resource_type = sniff_imgur_resource(url)
    return resource_type in ("album", "gallery")

def get_links(cursor: sqlite3.Cursor) -> List[Tuple[int, str]]:
    cursor.execute(
        """
        select id, url from medias
        where id not in (select media_id from images)
        """
    )
    return cursor.fetchall()

def ingest_albums(cursor: sqlite3.Cursor, client: ImgurClient, medias: List[Tuple[int, str]]) -> None:  # noqa: E501
    for media_id, url in medias:
        hash_ = get_id(url)
        if hash_ is None:
            logging.warning("Unable to get hash from %s, skipping", url)
            continue

        api_url = make_imgur_url("album", hash_)
        ret = client.get_album(api_url, media_id)
        if ret is None:
            continue

        album, images = ret
        insert_or_ignore(cursor, "albums", album)
        for image in images:
            insert_or_ignore(cursor, "images", image)

        logging.info("Processed %s", url)

    return

def ingest_standalones(cursor: sqlite3.Cursor, client: Client, imgur_client: ImgurClient, medias: List[Tuple[int, str]]) -> None:  # noqa: E501
    for media_id, url in medias:
        hash_ = get_id(url)
        if hash_ is None:
            logging.warning("Unable to get hash from %s, skipping", url)
            continue

        metadata = {"id": hash_, "media_id": media_id, "album_id": None}
        if is_imgur(url):
            api_url = make_imgur_url("image", hash_)
            img = imgur_client.get_json(api_url)
            if img is None:
                logging.warning("Failed to get image metadata for %s", url)
            else:
                metadata.update(**img["data"])

            image = imgur_client.get_image(url, **metadata)
        else:
            # Reddit.
            image = client.get_image(url, **metadata)

        insert_or_ignore(cursor, "images", image)
        logging.info("Processed %s", url)

    return

def main() -> int:
    setup_logging()
    parser = base_parser(description=__doc__)
    parser.add_argument("-t", "--client-id", type=str, help="Imgur Client ID.")
    args = parser.parse_args()

    conn = sqlite3.connect(args.conn)
    cursor = conn.cursor()
    logging.info("Established database connection")

    status = 0
    try:
        logging.info("Starting images ingest")
        medias = get_links(cursor)
        logging.info("Found %s links to ingest", len(medias))

        album_medias, standalone_medias = [], []
        for media in medias:
            _, url = media
            if is_album(url):
                album_medias.append(media)
            else:
                standalone_medias.append(media)

        generic_client = Client()
        imgur_client = ImgurClient(args.client_id)

        logging.info("Ingesting album media")
        ingest_albums(cursor, imgur_client, album_medias)
        logging.info("Ingesting standalone media")
        ingest_standalones(cursor, generic_client, imgur_client, standalone_medias)
    except (RateLimitError, requests.Timeout) as e:
        logging.error("Transient HTTP error, saving progress: %s", e)
        conn.commit()
        status = 1
    except Exception as e:
        logging.error("Encountered error, aborting: %s", e)
        conn.rollback()
        status = 1
    else:
        conn.commit()
    finally:
        conn.close()
        logging.info("Finished ingesting images")

    return status


if __name__ == "__main__":
    sys.exit(main())
