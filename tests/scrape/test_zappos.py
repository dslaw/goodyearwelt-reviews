from time import time
from unittest.mock import patch
from urllib.parse import parse_qsl, urlparse
import json
import pytest
import requests
import responses

from src.scrape.models import ProductSearchResult
from src.scrape.zappos import (
    SKIP_PRODUCT_STATUSES,
    ZapposClient,
    extract_description,
    get_products,
    paginated_search,
    reset_time,
    strip_legal_signs,
)


@pytest.fixture(scope="module")
def product_response():
    with open("tests/data/zappos-product.json") as fh:
        response_data = json.load(fh)
    return response_data


class TestResetTime(object):
    def test_when_reset_is_unnecessary(self):
        headers = {
            "X-RateLimit-Short-RateRemaining": "4",
            "X-RateLimit-Long-RateRemaining": "2000",
        }
        wait_seconds = reset_time(headers)
        assert wait_seconds == 0

    def test_when_short_limit_is_low(self):
        # Ensure reset time is in the future.
        ms = int(time() * 1000) + 100_000
        headers = {
            "X-RateLimit-Short-RateRemaining": "1",
            "X-RateLimit-Short-RateReset": str(ms),
            "X-RateLimit-Long-RateRemaining": "2000",
        }
        wait_seconds = reset_time(headers)
        assert wait_seconds > 0

    def test_when_long_limit_is_low(self):
        # Ensure reset time is in the future.
        ms = int(time() * 1000) + 100_000
        headers = {
            "X-RateLimit-Short-RateRemaining": "4",
            "X-RateLimit-Long-RateRemaining": "1",
            "X-RateLimit-Long-RateReset": str(ms),
        }
        wait_seconds = reset_time(headers)
        assert wait_seconds > 0

    def test_long_reset_when_limits_are_both_low(self):
        # Ensure long reset time is in the future,
        # and short reset time will be negative.
        ms = int(time() * 1000) + 100_000
        headers = {
            "X-RateLimit-Short-RateRemaining": "1",
            "X-RateLimit-Short-RateReset": "-1000",
            "X-RateLimit-Long-RateRemaining": "1",
            "X-RateLimit-Long-RateReset": str(ms),
        }
        wait_seconds = reset_time(headers)
        assert wait_seconds > 0

class TestStripLegalSigns(object):
    @pytest.mark.parametrize(
        "string, expected", [
            ("Brand\N{REGISTERED SIGN}", "Brand"),
            ("Brand\N{COPYRIGHT SIGN}", "Brand"),
            ("Brand\N{TRADE MARK SIGN}", "Brand"),
            ("Brand\N{TRADE MARK SIGN} Brand\N{COPYRIGHT SIGN}", "Brand Brand"),
        ], ids=["Registered", "Copyright", "Trademark", "Multiple"]
    )
    def test_strips(self, string, expected):
        assert strip_legal_signs(string) == expected

class TestExtractDescription(object):
    def test_ignores_link(self):
        expected = "Brand"
        html = "\n".join([
            "<ul>",
            "<li><a href='http://wherever.com' target=_blank>Brand Size Chart</a></li>",
            f"<li>{expected}</li>",
            "</ul>",
        ])
        assert extract_description(html, "Brand") == expected

    def test_returns_null_if_no_match(self):
        html = "\n".join([
            "<ul>",
            "<li>Other</li>",
            "</ul>",
        ])
        assert extract_description(html, "Brand") is None

    def test_gets_text_within_markup(self):
        expected = "For all your needs, Brand!"
        html = "\n".join([
            "<ul>",
            f"<li><strong>{expected}</strong></li>",
            "</ul>",
        ])
        assert extract_description(html, "Brand") == expected

    def test_gets_first_occurrence(self):
        expected = "For all your needs, Brand!"
        html = "\n".join([
            "<ul>",
            f"<li>{expected}</li>",
            "<li>Brand shoes are designed for extra comfort</li>",
            "</ul>",
        ])
        assert extract_description(html, "Brand") == expected

    def test_is_stripped(self):
        expected = "For all your needs, Brand!"
        html = "\n".join([
            "<ul>",
            f"<li>{expected}\N{TRADE MARK SIGN}</li>",
            "<li>Brand shoes are designed for extra comfort</li>",
            "</ul>",
        ])
        assert extract_description(html, "Brand") == expected

def headers():
    return {
        "Content-Type": "application/json",
        "X-RateLimit-Short-RateRemaining": "4",
        "X-RateLimit-Short-RateReset": "-1",
        "X-RateLimit-Long-RateRemaining": "2000",
        "X-RateLimit-Long-RateReset": "-1",
    }

class TestZapposClient(object):
    url = "http://mock.com/path"
    key = "api-key"

    def test_with_key(self):
        key = "api-key"
        params = {"a": 1, "b": 2}
        expected = {"a": 1, "b": 2, "key": key}

        client = ZapposClient(key)
        out = client.with_key(params)

        assert out == expected
        assert params == {"a": 1, "b": 2}  # No side-effects.

    @responses.activate
    def test_dispatch_adds_key(self):
        def has_key(request):
            parsed = urlparse(request.url)
            param_names, param_values = zip(*parse_qsl(parsed.query))
            idx = param_names.index("key")
            assert param_values[idx] == self.key
            return (200, {}, None)

        responses.add_callback(responses.GET, self.url, callback=has_key)

        client = ZapposClient(self.key)
        client.dispatch("GET", self.url)

    @responses.activate
    def test_dispatch_retries_when_throttled(self):
        responses.add(responses.GET, self.url, status=429)
        responses.add(responses.GET, self.url, status=200)

        client = ZapposClient(self.key)
        client.retry_delay_seconds = .1
        client.dispatch("GET", self.url)

    @responses.activate
    def test_dispatch_raises_unknown_error(self):
        responses.add(responses.GET, self.url, status=404)

        with pytest.raises(requests.RequestException):
            client = ZapposClient(self.key)
            client.dispatch("GET", self.url)

    @responses.activate
    def test_dispatch_pre_empts_rate_limiting(self):
        delay_seconds = 1

        def cb(request):
            reset_ms = int(1000 * (time() + delay_seconds))
            headers = {
                "X-RateLimit-Short-RateRemaining": "1",
                "X-RateLimit-Short-RateReset": str(reset_ms),
                "X-RateLimit-Long-RateRemaining": "2000",
            }
            return (200, headers, None)

        responses.add_callback(responses.GET, self.url, callback=cb)

        with patch("src.scrape.zappos.sleep", return_value=None) as patched_sleep:
            client = ZapposClient(self.key)
            client.dispatch("GET", self.url)

            assert patched_sleep.call_count == 1

    @responses.activate
    def test_gets_product(self, product_response):
        product_id = 7324205

        def has_includes(request):
            parsed = urlparse(request.url)
            param_names, _ = zip(*parse_qsl(parsed.query))
            assert "includes" in param_names

            headers = {"Content-Type": "application/json"}
            return (200, headers, json.dumps(product_response))

        responses.add_callback(
            responses.GET,
            f"http://api.zappos.com/Product/{product_id}",
            callback=has_includes,
        )

        client = ZapposClient(self.key)
        product = client.product_description(product_id)

        assert product.id == product_id
        assert product.description is None  # Empty description, no match found.

    @responses.activate
    def test_search_has_params(self):
        url = "http://api.zappos.com/Search"
        term = "query"
        page = "1"
        limit = "10"
        includes = ["categoryFacet"]
        filters = {"categoryFacet": ["Shoes", "Boots"]}
        expected_params = {
            "term": term,
            "page": page,
            "limit": limit,
            "includes": json.dumps(includes),
            "filters": json.dumps(filters),
        }

        def has_params(request):
            parsed = urlparse(request.url)
            params = {
                name: value
                for name, value in parse_qsl(parsed.query)
                if name != "key"
            }
            assert params == expected_params
            return (200, {}, None)

        responses.add_callback(responses.GET, url, callback=has_params)

        client = ZapposClient(self.key)
        client.search(term, int(page), int(limit))

class TestGetProducts(object):
    @pytest.mark.parametrize("status_code", SKIP_PRODUCT_STATUSES)
    @responses.activate
    def test_skips_skippable_errors(self, status_code):
        psr = ProductSearchResult("brand", 123, "name", "category", "query")
        url = f"http://api.zappos.com/Product/{psr.product_id}"
        responses.add(responses.GET, url, status=status_code)

        client = ZapposClient("api-key")
        products = list(get_products(client, [psr]))

        assert products == []

    @responses.activate
    def test_gets_multiple_products(self, product_response):
        p_ids = (123, 124)
        psrs = []
        for p_id in p_ids:
            psr = ProductSearchResult("brand", p_id, "name", "category", "query")
            url = f"http://api.zappos.com/Product/{psr.product_id}"
            responses.add(responses.GET, url, json=product_response)
            psrs.append(psr)

        client = ZapposClient("api-key")
        products = list(get_products(client, psrs))

        assert len(products) == len(psrs)
        assert products[0].id == p_ids[0]
        assert products[1].id == p_ids[1]


class TestPaginatedSearch(object):
    key = "api-key"

    @responses.activate
    def test_paginated_search(self):
        url = "http://api.zappos.com/Search"
        expected_count = 2

        def cb(request):
            cb.request_count += 1
            content = json.dumps({
                "totalResultCount": str(expected_count),
                "status": "200",
                # Return one result per page for simplicity.
                "results": [{
                    "brandName": "brand",
                    "productId": "123",
                    "productName": "brand's product",
                    "caegoryFacet": "Shoes",
                }],
            })
            headers = {"Content-Type": "application/json"}
            return (200, headers, content)

        cb.request_count = 0
        responses.add_callback(responses.GET, url, callback=cb)

        client = ZapposClient(self.key)
        search_results = paginated_search(client, term="query")

        assert len(search_results) == expected_count
        assert cb.request_count == expected_count
