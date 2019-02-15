import json
import pytest
import responses
import sqlite3

from src.fetch import (
    Submission,
    extract_submissions,
    get_oldest_submission,
    ingest,
    insert_submission,
    paginated_search,
)


with open("db/schema.sql") as fh:
    setup_sql = fh.read()

@pytest.fixture
def cursor():
    conn = sqlite3.connect(":memory:")
    conn.executescript(setup_sql)
    yield conn.cursor()
    conn.close()

@pytest.fixture(scope="module")
def listing():
    with open("tests/data/listing.json") as fh:
        data = json.load(fh)
    return data


class TestSubmission(object):
    def test_from_json(self):
        raw_submission = {
            "id": "testid",
            "subreddit": "testsr",
            "title": "Test",
            "author_fullname": "unknown",
            "url": "",
            "created_utc": 1e7,
            "selftext_html": "",
            "num_comments": 0,
            "gilded": 0,
            "downs": 0,
            "ups": 0,
            "score": 0,
            "something-extra": None,
            "extra-nested-data": {"more": "things", "even": "more"},
        }
        submission = Submission.from_json(**raw_submission)
        assert isinstance(submission, Submission)

class TestExtractSubmissions(object):
    def test_extracts_submissions(self, listing):
        n_expected = 10
        submissions = extract_submissions(listing)

        assert len(submissions) == n_expected

    def test_propagates_empty(self):
        # e.g. no more search results.
        listing = {
            "kind": "Listing",
            "data": {
                "after": None,
                "children": [],
            },
        }
        n_expected = 0

        submissions = extract_submissions(listing)

        assert len(submissions) == n_expected

class TestInsertSubmission(object):
    @pytest.mark.parametrize(
        "table", [
            "submission_facts",
            "submissions",
        ], ids=["fact", "dimension"]
    )
    def test_inserted(self, cursor, table):
        submission = Submission(
            id="testid",
            subreddit="testsr",
            title="Test",
            author_fullname="unknown",
            url="",
            created_utc=1e7,
            selftext_html="",
            num_comments=0,
            gilded=0,
            downs=0,
            ups=0,
            score=0
        )
        sql = f"select count(*) from {table} where id='testid'"

        insert_submission(cursor, submission, query="test")

        cursor.execute(sql)
        count, = cursor.fetchone()
        assert count == 1

class TestGetOldestSubmission(object):
    @pytest.mark.parametrize(
        "submission_ids, search_query, expected_id", [
            (["first1", "second"], "query", "first1"),
            (["first1", "second"], "non-existent", None),
            ([], "query", None),
        ], ids=["query-matches", "query-doesnt-match", "empty-table"]
    )
    def test_gets(self, cursor, submission_ids, search_query, expected_id):
        # Use index as a timestamp, which means the first element is the
        # oldest.
        #
        # NB: `url` field has a unique constraint, so reuse submission-id
        #     for it.
        for timestamp, s_id in enumerate(submission_ids):
            cursor.execute(
                """
                insert into submission_facts
                (id, title, author_fullname, url, created_utc, search_query)
                values
                (?, 'title', 'author', ?, ?, ?)
                """, (
                    s_id, f"url={s_id}", timestamp, "query"
                )
            )

        ret_id = get_oldest_submission(cursor, search_query)

        assert ret_id == expected_id

class MockSearchResults(object):
    limit = 5  # Ignore requested value.
    headers = {"Content-Type": "application/json"}

    def __init__(self, listing):
        self.kind = listing["kind"]
        self.children = listing["data"]["children"]

    @property
    def max_responses(self):
        n, remainder = divmod(len(self.children), self.limit)
        if remainder > 0:
            n += 1
        return n

    def listing(self, submissions, after):
        data = {
            "kind": self.kind,
            "data": {
                "children": submissions,
                "after": after,
            },
        }
        return json.dumps(data)

    def get(self, request):
        qs = responses.urlparse(request.url).query
        params = {k: v for k, v in responses.parse_qsl(qs)}
        after = params.get("after")

        if after is None:
            start_pos = 0
        else:
            s_ids = [s["data"]["id"] for s in self.children]
            try:
                start_pos = s_ids.index(after) + 1
            except IndexError:
                return (400, {}, "")

        end_pos = start_pos + self.limit
        submissions = self.children[start_pos:end_pos]

        if end_pos >= len(self.children):
            resp_after = None
        else:
            resp_after = submissions[-1]["data"]["id"]

        return (200, self.headers, self.listing(submissions, resp_after))

class TestPaginatedSearch(object):
    @responses.activate
    def test_mock(self, listing):
        subreddit = "mock"
        url = f"https://reddit.com/r/{subreddit}/search.json"

        mock_search = MockSearchResults(listing)
        responses.add_callback(responses.GET, url, mock_search.get)

        out = list(paginated_search(subreddit, query="query", after=None))

        assert all(r.ok for r in out)
        assert len(out) == mock_search.max_responses

class TestIngest(object):
    @responses.activate
    def test_mock_without_resume(self, cursor, listing):
        # NB: The subreddit name is hardcoded for `ingest`.
        subreddit = "goodyearwelt"
        url = f"https://reddit.com/r/{subreddit}/search.json"

        mock_search = MockSearchResults(listing)
        responses.add_callback(responses.GET, url, mock_search.get)

        ingest(cursor, query="query", resume=False)

        cursor.execute("select count(*) from submission_facts")
        facts_count = cursor.fetchone()[0]
        cursor.execute("select count(*) from submissions")
        dims_count = cursor.fetchone()[0]

        assert facts_count == len(mock_search.children)
        assert dims_count == len(mock_search.children)
