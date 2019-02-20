from dataclasses import asdict
from typing import Callable, TypeVar
from urllib.parse import urlparse
import sqlite3


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
