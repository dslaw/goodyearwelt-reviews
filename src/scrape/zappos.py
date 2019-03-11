"""Collect training data from Zappos."""

from bs4 import BeautifulSoup, element
from datetime import datetime
from time import sleep
from typing import Dict, Iterable, List, MutableMapping, Optional
import json
import logging
import re
import requests
import sqlite3
import sys

from src.scrape.common import base_parser, insert_or_ignore, from_json, setup_logging
from src.scrape.models import Product, ProductSearchResult


# If a larger limit is requested, the server will impose this limit.
MAX_SEARCH_LIMIT = 500
# The product type (`productTypeFacet`) "Shoes" encompasses the categories
# (`categoryFacet`) below, as well as others e.g. Sandals, but we use a
# smaller set in the interest of reducing the number of requests.
SHOE_CATEGORIES = ("Shoes", "Boots")
LOG_INTERVAL = 50
# Ignore these HTTP status codes when attempting to fetch products.
SKIP_PRODUCT_STATUSES = (404, 504)


def reset_time(headers: MutableMapping[str, str]) -> float:
    def le_threshold(type_: str) -> bool:
        remaining = headers.get(f"X-RateLimit-{type_}-RateRemaining")
        if remaining is None:
            return False
        return int(remaining) <= 1

    reset_long = le_threshold("Long")
    reset_short = le_threshold("Short")

    if not reset_long and not reset_short:
        return 0

    type_ = "Long" if reset_long else "Short"
    reset_ms = int(headers.get(f"X-RateLimit-{type_}-RateReset", 0))
    reset_time = datetime.fromtimestamp(reset_ms / 1_000)
    wait_seconds = (reset_time - datetime.now()).total_seconds()
    return wait_seconds

def strip_legal_signs(string: str) -> str:
    signs = (
        "\N{REGISTERED SIGN}",
        "\N{COPYRIGHT SIGN}",
        "\N{TRADE MARK SIGN}",
    )
    for sign in signs:
        string = string.replace(sign, "")
    return string

def extract_description(description_html: str, brand: str) -> Optional[str]:
    pattern = re.compile(rf"\b{brand}\b", flags=re.IGNORECASE)
    soup = BeautifulSoup(description_html, "html.parser")

    for item in soup.find_all("li"):
        for child in item.children:
            # Link text will describe the link, not the product.
            # And is likely a sizing chart link, anyway.
            if isinstance(child, element.Tag) and child.has_attr("href"):
                continue

            # Pull text out of additional formating tag (e.g. <strong>),
            # if necessary.
            text = getattr(child, "text", str(child))
            normalized = strip_legal_signs(text)

            # Assume the first mention is the only one of interest.
            # Or most pertinent one, at least.
            if re.search(pattern, normalized):
                return normalized

    return None

class ZapposClient(object):
    base_url = "http://api.zappos.com"
    max_retries = 1
    retry_delay_seconds = 2 * 60

    def __init__(self, api_key: str):
        self.api_key = api_key

    def with_key(self, params: Dict[str, str]) -> Dict[str, str]:
        return {**params, "key": self.api_key}

    def dispatch(self, method: str, url: str, **kwds) -> requests.Response:
        params = self.with_key(kwds.pop("params", {}))

        for _ in range(self.max_retries + 1):
            response = requests.request(method, url, params=params, **kwds)
            if response.status_code != 429:
                break

            logging.warning(
                "Encountered rate limit, waiting %s seconds",
                self.retry_delay_seconds
            )
            sleep(self.retry_delay_seconds)

        response.raise_for_status()

        # Pre-empt rate-limiting.
        wait_time = reset_time(response.headers)
        if wait_time > 0:
            logging.info("Approaching rate limit - waiting %s seconds", wait_time)
            sleep(wait_time)
        return response

    def search(self, term: str, page: int, limit: int) -> requests.Response:
        search_url = f"{self.base_url}/Search"
        params = {
            "term": term,
            "page": str(page),
            "limit": str(limit),
            "includes": json.dumps(["categoryFacet"]),
            "filters": json.dumps({"categoryFacet": SHOE_CATEGORIES}),
        }
        return self.dispatch("GET", search_url, params=params)

    def product_description(self, product_id: int) -> Product:
        if product_id < 0:
            raise ValueError("`product_id` cannot be negative")

        product_url = f"{self.base_url}/Product/{product_id}"
        includes = ["description"]
        params = {"includes": json.dumps(includes)}
        response = self.dispatch("GET", product_url, params=params)

        data = response.json()
        # This is couched in an array for some reason.
        product_raw, *unexpected = data["product"]
        if unexpected:
            logging.warning(
                "Found %s additional products for product-id: %s",
                len(unexpected),
                product_id
            )

        product = from_json(Product, **product_raw, id=product_id)
        if product.description is not None:
            sentence = extract_description(product.description, product.brand)
            product.description = sentence
        return product

def paginated_search(client: ZapposClient, term: str) -> List[ProductSearchResult]:
    stop_at = None
    page = 1
    results: List[ProductSearchResult] = []
    while stop_at is None or len(results) < stop_at:
        response = client.search(term, page=page, limit=MAX_SEARCH_LIMIT)

        data = response.json()
        if stop_at is None:
            stop_at = int(data["totalResultCount"])

        results.extend([
            from_json(ProductSearchResult, **result, search_query=term)
            for result in data["results"]
        ])
        page += 1

    return results

def get_products(client: ZapposClient, records: List[ProductSearchResult]) -> Iterable[Product]:  # noqa: E501
    for i, record in enumerate(records, start=1):
        if i % LOG_INTERVAL == 0:
            logging.info(f"Fetching product {i}/{len(records)}")

        try:
            product = client.product_description(record.product_id)
        except requests.RequestException as e:
            if e.response.status_code in SKIP_PRODUCT_STATUSES:
                continue
            raise e
        yield product

def main() -> int:
    setup_logging()
    parser = base_parser(description=__doc__)
    parser.add_argument("--api-key", required=True, type=str, help="Zappos API key.")
    parser.add_argument("--search", action="store_true", help="Search for products.")
    parser.add_argument("--query", type=str, default="", help="Search query.")
    parser.add_argument("--fetch", action="store_true", help="Get product information.")
    args = parser.parse_args()

    if args.search and args.fetch:
        raise RuntimeError("`search` and `fetch` may not both be given")

    if not args.search and not args.fetch:
        raise RuntimeError("`search` or `fetch` must be given")

    conn = sqlite3.connect(args.conn)
    cursor = conn.cursor()

    client = ZapposClient(args.api_key)

    status = 0
    try:
        if args.search:
            logging.info("Starting product search for %s", args.query)
            search_results = paginated_search(client, term=args.query)
            logging.info("Writing %s search results", len(search_results))
            for result in search_results:
                insert_or_ignore(cursor, "searches", result)
        elif args.fetch:
            logging.info("Getting products to fetch")
            cursor.execute(
                """
                select
                    brand,
                    product_id,
                    product_name,
                    category,
                    search_query
                from (
                    select
                        *,
                        row_number() over (partition by product_id) as row_no
                    from searches
                    where
                        -- Kids brands won't be on /r/goodyearwelt.
                        lower(brand) not like '%kids%'
                        -- These will simply become confusing.
                        and lower(brand) not like '%boots'
                        and lower(brand) not like '%shoes'
                ) as t
                where
                    row_no = 1
                    and product_id not in (select id from products);
                """
            )
            records = [ProductSearchResult(*row) for row in cursor]
            logging.info("Found %s products", len(records))

            logging.info("Ingesting product(s) information")
            for product in get_products(client, records):
                insert_or_ignore(cursor, "products", product)
    except Exception as e:
        status = 1
        logging.error("Encountered error, aborting: %s", e)
        if not args.fetch:
            conn.rollback()
        else:
            conn.commit()
    else:
        logging.info("Committing results")
        conn.commit()
    finally:
        conn.close()

    return status


if __name__ == "__main__":
    sys.exit(main())
