from requests import HTTPError
import json
import pytest
import responses

from src.scrape.images import (
    Client,
    ImgurClient,
    RateLimitError,
    get_id,
    get_links,
    ingest_albums,
    ingest_standalones,
    is_album,
    is_imgur,
    make_imgur_url,
    sniff_imgur_resource,
)


@pytest.fixture
def imgur_album():
    with open("tests/data/imgur-album.json") as fh:
        data = json.load(fh)
    return data

def add_image(url, body):
    mimetype = "image/jpeg"
    headers = {"Content-Type": mimetype}
    responses.add(responses.GET, url, body=body, headers=headers)
    return mimetype

def add_album_response(raw_album, img_body):
    data = raw_album["data"]
    album_id = data.get("id", "a_id")

    url = f"https://mock-imgur.com/a/{album_id}"
    api_url = f"https://api.imgur.com/3/album/{album_id}"

    # Ensure links point to mocked urls.
    data["id"] = album_id
    data["link"] = url

    # Point images to mocked urls.
    for i, img in enumerate(data["images"]):
        img["id"] = str(i)
        img_url = f"https://mock-imgur.com/{i}.jpg"
        md_url = f"https://api.imgur.com/3/image/{i}"
        img["link"] = img_url

        add_image(img_url, img_body)
        responses.add(responses.GET, md_url, json={"data": img})

    responses.add(responses.GET, url, status=500)
    responses.add(responses.GET, api_url, json=raw_album)

    # Return updated data and mocked API url.
    return raw_album, api_url

def insert_submission(cursor, s_id):
    cursor.execute(
        """
        insert into submissions
        values (?, '', '', '', '', '', 1, '', 0, 0, 0, 0, 0, 'query', 1)
        """,
        (s_id,)
    )
    return

def insert_media(cursor, m_id, s_id, url):
    cursor.execute(
        """
        insert into medias (id, submission_id, url, is_direct)
        values (?, ?, ?, 0)
        """,
        (m_id, s_id, url)
    )
    return


class TestSniffImgurResource(object):
    @pytest.mark.parametrize(
        "url, expected", [
            ("https://imgur.com/a/ABCDEFG", "album"),
            ("https://imgur.com/gallery/ABCDEFG", "gallery"),
            ("https://imgur.com/ABCDEFG", "image"),
            ("https://imgur.com/ABCDEFG.jpg", "image"),
        ], ids=["album", "gallery", "image:page", "image:direct"]
    )
    def test_gets_resource(self, url, expected):
        resource_type = sniff_imgur_resource(url)
        assert resource_type == expected

    def test_handles_trailing_slash(self):
        url = "https://imgur.com/a/ABCDEFG/"
        resource_type = sniff_imgur_resource(url)
        assert resource_type == "album"

    def test_ignores_extra_path_components(self):
        url = "https://imgur.com/a/ABCDEFG/image.jpg"
        resource_type = sniff_imgur_resource(url)
        assert resource_type == "album"

    def test_returns_none_for_unknown_resource(self):
        url = "https://imgur.com/unknown/ABCDEFG"
        resource_type = sniff_imgur_resource(url)
        assert resource_type is None

    def test_strips_album_anchor_link(self):
        url = "https://imgur.com/a/ABCDEFG#XYZ123"
        resource_type = sniff_imgur_resource(url)
        assert resource_type == "album"

class TestGetID(object):
    @pytest.mark.parametrize(
        "url, expected", [
            ("https://imgur.com/a/ABCDEFG", "ABCDEFG"),
            ("https://imgur.com/a/ABCDEFG#image.jpg", "ABCDEFG"),
            ("https://imgur.com/a/ABCDEFG/", "ABCDEFG"),
            ("https://imgur.com/gallery/ABCDEFG", "ABCDEFG"),
            ("https://imgur.com/ABCDEFG", "ABCDEFG"),
            ("https://imgur.com/ABCDEFG.jpg", "ABCDEFG"),
            ("https://i.redd.it/ABCDEFG.jpg", "ABCDEFG"),
            ("https://reddituploads.com/ABCDEFG.jpg", "ABCDEFG"),
        ], ids=[
            "album",
            "album:anchor",
            "trailing",
            "gallery",
            "image:page",
            "image:direct",
            "redd.it",
            "reddituploads",
        ]
    )
    def test_gets_id(self, url, expected):
        hash_ = get_id(url)
        assert hash_ == expected

class TestMakeImgurURL(object):
    def test_imgur_api_url(self):
        api_url = make_imgur_url("album", "ABCDEFG")
        assert api_url.endswith("album/ABCDEFG")

class TestIsImgur(object):
    @pytest.mark.parametrize(
        "url", [
            "https://imgur.com/a/foo",
            "https://i.imgur.com/a/foo",
            "https://m.imgur.com/a/foo",
        ], ids=["imgur", "i.imgur", "m.imgur"]
    )
    def test_is_imgur(self, url):
        assert is_imgur(url)

    @pytest.mark.parametrize(
        "url", [
            "https://i.redd.it/foo",
            "https://reddituploads.com/foo",
            "https://reddit.com/r/foo",
            "https://random.com/foo",
        ], ids=["i.redd", "reddituploads", "reddit", "random"]
    )
    def test_not_imgur(self, url):
        assert not is_imgur(url)

class TestIsAlbum(object):
    @pytest.mark.parametrize(
        "url", [
            "https://imgur.com/a/foo",
            "https://imgur.com/a/foo#image",
            "https://imgur.com/gallery/foo",
        ], ids=["album", "album:anchor", "gallery"]
    )
    def test_is_album(self, url):
        assert is_album(url)

    @pytest.mark.parametrize(
        "url", [
            "https://imgur.com/foo",
            "https://imgur.com/foo.jpg",
            "https://i.redd.it/foo",
            "https://reddituploads.com/foo",
        ], ids=["image:page", "image:direct", "i.redd", "reddituploads"]
    )
    def test_not_album(self, url):
        assert not is_album(url)

class TestGetLinks(object):
    def test_unprocessed_links(self, cursor):
        # `medias` has a FK constraint on `submissions.id`.
        insert_submission(cursor, "s_id")
        medias = [
            (1, "https://imgur.com/a/foo"),
            (2, "https://imgur.com/a/bar"),
            (3, "https://imgur.com/a/baz"),
        ]
        for id_, url in medias:
            insert_media(cursor, id_, "s_id", url)

        # Add a "processed" image, linked to the first media row.
        (processed_media_id, _), *expected = medias
        cursor.execute(
            """
            insert into images (id, media_id, url)
            values ('1', ?, 'https://imgur.com/baz-image.jpg')
            """,
            (processed_media_id,)
        )

        medias = get_links(cursor)

        assert medias == expected

class TestClient(object):
    url = "https://mock-imgur.com/image.jpg"
    body = b"data"

    @responses.activate
    def test_gets_image(self):
        mimetype = add_image(self.url, self.body)

        client = Client()
        image = client.get_image(self.url)

        assert image.url == self.url
        assert image.mimetype == mimetype
        assert image.img == self.body

    @responses.activate
    def test_metadata_is_propagated(self):
        add_image(self.url, self.body)

        client = Client()
        image = client.get_image(self.url, id="image", media_id=1)

        assert image.id == "image"
        assert image.media_id == 1

    @responses.activate
    def test_returns_metadata_if_request_fails(self):
        responses.add(responses.GET, self.url, status=404)

        client = Client()
        image = client.get_image(self.url, media_id=1, id="image")

        assert image.url == self.url
        assert image.mimetype is None
        assert image.img is None

    @pytest.mark.parametrize("status", Client.fail_on_statuses)
    @responses.activate
    def test_raises_error_on_auth_failure(self, status):
        responses.add(responses.GET, self.url, status=status)

        with pytest.raises(HTTPError):
            client = Client()
            client.get_image(self.url)

class TestImgurClient(object):
    @responses.activate
    def test_get_image_has_auth_header(self):
        def cb(request):
            auth = request.headers.get("Authorization")
            assert auth is not None
            assert auth == "Client-ID test"
            return (200, {}, b"data")

        url = "https://mock-imgur.com/image.jpg"
        responses.add_callback(responses.GET, url, cb)

        client = ImgurClient("test")
        client.get_image(url)

    @responses.activate
    def test_get_album_has_auth_header(self):
        body = json.dumps({
            "status": 200,
            "data": {
                "id": "foo",
                "images": [],
            },
        })

        def cb(request):
            auth = request.headers.get("Authorization")
            assert auth is not None
            assert auth == "Client-ID test"
            return (200, {}, body.encode())

        url = "https://mock-imgur.com/a/foo"
        responses.add_callback(responses.GET, url, cb)

        client = ImgurClient("test")
        client.get_album(url, media_id=1)

    @responses.activate
    def test_raises_error_on_rate_limited(self):
        url = "https://mock-imgur.com/a/foo"
        responses.add(
            responses.GET,
            url,
            status=400,  # XXX: Not sure what Imgur uses.
            body="Limit reached.",
            headers={
                "X-RateLimit-UserCredit": "500",
                "X-RateLimit-UserRemaining": "0",
                "X-RateLimit-UserReset": str(1_500_000_000),
            }
        )

        with pytest.raises(RateLimitError):
            client = ImgurClient("test")
            client.get_album(url, media_id=1)

    @responses.activate
    def test_gets_album(self, imgur_album):
        imgur_album, url = add_album_response(imgur_album, img_body=b"data")
        album_data = imgur_album["data"]

        client = ImgurClient("test")
        album, images = client.get_album(url, media_id=1)

        assert album.id == album_data["id"]
        assert album.media_id == 1
        assert len(images) == len(album_data["images"])
        assert all(image.album_id == album.id for image in images)
        assert all(image.media_id == 1 for image in images)

class TestIngestStandalones(object):
    @responses.activate
    def test_reddit_standalone(self, cursor):
        url = "https://mock.i.redd.it/image.jpg"
        body = b"data"
        mimetype = add_image(url, body)

        # FK constraints.
        media_id = 1
        insert_submission(cursor, "s_id")
        insert_media(cursor, media_id, "s_id", url)

        expected = [
            ("image", media_id, None, None, None, None, mimetype, url, None, body)
        ]

        medias = [(media_id, url)]
        client = Client()
        imgur_client = ImgurClient("test")
        ingest_standalones(cursor, client, imgur_client, medias)

        cursor.execute(
            """
            select
                id, media_id, album_id, title, description, uploaded_utc,
                mimetype, url, views, img
            from images
            """
        )
        records = cursor.fetchall()
        assert records == expected

    @responses.activate
    def test_imgur_standalone(self, cursor):
        url = "https://mock-imgur/image.jpg"
        body = b"data"
        mimetype = add_image(url, body)

        md_url = "https://api.imgur.com/3/image/image"
        md = {
            "data": {
                "id": "image",
                "description": "Image",
                "datetime": 1_500_000_000,
                "type": mimetype,
                "views": 0,
                "link": url,
            }
        }
        responses.add(responses.GET, md_url, json=md)

        # FK constraints.
        media_id = 1
        insert_submission(cursor, "s_id")
        insert_media(cursor, media_id, "s_id", url)

        expected = [(
            "image", media_id, None, None, md["data"]["description"],
            md["data"]["datetime"], mimetype, url, md["data"]["views"], body
        )]

        medias = [(media_id, url)]
        client = Client()
        imgur_client = ImgurClient("test")
        ingest_standalones(cursor, client, imgur_client, medias)

        cursor.execute(
            """
            select
                id, media_id, album_id, title, description, uploaded_utc,
                mimetype, url, views, img
            from images
            """
        )
        records = cursor.fetchall()
        assert records == expected

class TestIngestAlbums(object):
    @responses.activate
    def test_imgur_album(self, cursor, imgur_album):
        img_body = b"data"
        imgur_album, _ = add_album_response(imgur_album, img_body=img_body)

        album_data = imgur_album["data"]
        url = album_data["link"]  # Non-API URL.
        album_id = album_data["id"]

        # FK constraints.
        media_id = 1
        insert_submission(cursor, "s_id")
        insert_media(cursor, media_id, "s_id", url)

        expected_albums = [(
            album_id, media_id, album_data["title"], album_data["description"],
            album_data["datetime"], url, album_data["views"],
        )]
        expected_images = [
            (img["id"], media_id, album_id, img["title"], img["description"],
             img["datetime"], "image/jpeg", img["link"], img["views"],
             img_body)
            for img in album_data["images"]
        ]

        medias = [(media_id, url)]
        imgur_client = ImgurClient("test")
        ingest_albums(cursor, imgur_client, medias)

        cursor.execute(
            """
            select
                id, media_id, title, description, uploaded_utc, url, views
            from albums
            """
        )
        albums = cursor.fetchall()
        assert albums == expected_albums

        cursor.execute(
            """
            select
                id, media_id, album_id, title, description, uploaded_utc,
                mimetype, url, views, img
            from images
            """
        )
        images = cursor.fetchall()
        assert images == expected_images
