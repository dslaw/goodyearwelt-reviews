from dataclasses import InitVar, dataclass, field
import pytest
import sqlite3

from src.scrape.common import from_json, insert_or_ignore, is_media_url


@dataclass
class Post:
    author: str = field(init=False)
    user: InitVar[str]
    content: str

    def __post_init__(self, user: str, **_):
        self.author = user

@pytest.fixture
def cursor():
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    cursor.execute(
        """
        create table posts (
            id integer primary key autoincrement,
            author varchar not null,
            content varchar not null
        )
        """
    )
    yield cursor
    conn.close()


class TestFromJson(object):
    def test_initializes_dataclass(self):
        data = {"user": "user", "content": "content"}
        expected_post = Post(user=None, content="content")
        expected_post.author = "user"

        post = from_json(Post, **data)

        assert post == expected_post

    def test_only_selects_init_fields(self):
        data = {"user": "user", "content": "content", "unused": "unused"}
        expected_post = Post(user=None, content="content")
        expected_post.author = "user"

        post = from_json(Post, **data)

        assert post == expected_post

    def test_allows_missing_fields(self):
        data = {"user": "user"}
        expected_post = Post(user=None, content=None)
        expected_post.author = "user"

        post = from_json(Post, **data)

        assert post == expected_post

class TestInsertOrIgnore(object):
    table = "posts"

    def test_inserts(self, cursor):
        post = Post(user="user", content="content")

        insert_or_ignore(cursor, self.table, post)
        cursor.execute(f"select author, content from {self.table}")
        results = cursor.fetchall()

        assert len(results) == 1
        author, content = results[0]
        assert author == post.author
        assert content == post.content

    def test_ignores(self, cursor):
        # Violates not null constraint.
        post = Post(user="user", content=None)

        insert_or_ignore(cursor, self.table, post)
        cursor.execute(f"select author, content from {self.table}")
        results = cursor.fetchall()

        assert len(results) == 0

class TestIsMediaURL(object):
    @pytest.mark.parametrize("url", [
        "https://imgur.com/a/ABCDEFG",
        "https://i.imgur.com/a/ABCDEFG",
        "https://i.redd.it/ABCDEFG-FOOBAR",
        "https://reddituploads.com/ABCDEFG-FOOBAR"
    ], ids=["imgur_plain", "imgur_prefixed", "reddit_i", "reddituploads"])
    def test_is_media(self, url):
        assert is_media_url(url)

    def test_reddit(self):
        url = "https://reddit.com/r/subreddit/"
        assert not is_media_url(url)

    def test_unknown(self):
        url = "https://uploads.com/ABCDEFG"
        assert not is_media_url(url)
