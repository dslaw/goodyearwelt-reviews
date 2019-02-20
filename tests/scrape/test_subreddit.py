import json
import pytest
import responses

from src.scrape.subreddit import extract_submissions, ingest, paginated_search


@pytest.fixture(scope="module")
def listing():
    with open("tests/data/listing.json") as fh:
        data = json.load(fh)
    return data


class TestExtractSubmissions(object):
    query = "query"
    subreddit = "subreddit"

    def test_extracts_all_submissions(self, listing):
        expected_len = 10
        extracted = extract_submissions(listing, self.subreddit, self.query)

        assert len(extracted) == expected_len

    def test_saves_additional_metadata(self, listing):
        extracted = extract_submissions(listing, self.subreddit, self.query)

        submissions = [s for s, _ in extracted]
        assert all(s.search_query == self.query for s in submissions)
        assert all(s.subreddit == self.subreddit for s in submissions)

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

        extracted = extract_submissions(listing, self.subreddit, self.query)

        assert len(extracted) == n_expected

    def test_medias_are_associated(self, listing):
        extracted = extract_submissions(listing, self.subreddit, self.query)
        assert all(m is None or s.id == m.submission_id for s, m in extracted)

    def test_medias_are_direct(self, listing):
        extracted = extract_submissions(listing, self.subreddit, self.query)
        medias = [m for _, m in extracted if m is not None]
        assert all(m.is_direct for m in medias)

    def test_medias_dont_have_text(self, listing):
        extracted = extract_submissions(listing, self.subreddit, self.query)
        medias = [m for _, m in extracted if m is not None]
        assert all(m.txt is None for m in medias)

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
        subreddit = "mock"
        url = f"https://reddit.com/r/{subreddit}/search.json"

        mock_search = MockSearchResults(listing)
        responses.add_callback(responses.GET, url, mock_search.get)

        ingest(cursor, query="query", subreddit=subreddit)

        cursor.execute("select count(*) from submissions")
        count = cursor.fetchone()[0]

        assert count == len(mock_search.children)
