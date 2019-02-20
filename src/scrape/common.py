from dataclasses import asdict
from typing import Callable, TypeVar
from urllib.parse import urlparse
import argparse
import logging
import sqlite3
import sys


PLACEHOLDER = "?"
T = TypeVar("T")


def from_json(cls: Callable[..., T], **data) -> T:
    fields = getattr(cls, "__dataclass_fields__")
    init_field_names = [f.name for f in fields.values() if f.init]
    kwds = {name: data.get(name) for name in init_field_names}
    return cls(**kwds)

def insert_or_ignore(cursor: sqlite3.Cursor, table: str, instance: object) -> None:
    d = asdict(instance)
    names, values = zip(*d.items())
    targets = ', '.join(names)
    params = ', '.join([PLACEHOLDER for _ in values])

    sql = f"insert or ignore into {table} ({targets}) values ({params})"
    cursor.execute(sql, values)
    return

def is_media_url(url: str) -> bool:
    parsed = urlparse(url)
    domain = parsed.netloc

    # Domain may include a prefix for images/mobile/etc.,
    # e.g. `m.imgur.com`, `i.redd.it`.
    is_imgur = domain.endswith("imgur.com")
    is_reddit = domain.endswith("redd.it") or domain.endswith("reddituploads.com")
    return is_imgur or is_reddit

def setup_logging() -> None:
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s:%(module)s:%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def base_parser(**kwds) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(**kwds)
    parser.add_argument("-c", "--conn", type=str, help="Database connection string.")
    return parser
