import html

from src.scrape.extract_links import extract


class TestExtract(object):
    doc = html.escape(
        """
        Review of something.

        <a href="https://imgur.com/a/ABCDEFG">Album first</a>

        I purchased something on whenever. See this picture
        <a href="https://i.redd.it/direct.jpg">here</a>.
        """
    )

    def test_extracts_media_links(self):
        expected_urls = [
            "https://imgur.com/a/ABCDEFG",
            "https://i.redd.it/direct.jpg",
        ]
        expected_txts = ["Album first", "here"]

        medias = extract("s_id", self.doc)
        urls = [media.url for media in medias]
        txts = [media.txt for media in medias]

        assert urls == expected_urls
        assert txts == expected_txts

    def test_propagates_submission_id(self):
        submission_id = "s_id"
        medias = extract(submission_id, self.doc)
        assert all(media.submission_id == submission_id for media in medias)

    def test_never_direct(self):
        medias = extract("s_id", self.doc)
        assert all(not media.is_direct for media in medias)
