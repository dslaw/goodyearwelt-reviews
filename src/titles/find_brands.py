"""Find known brands in submission titles."""

from csv import DictWriter
from dataclasses import asdict, dataclass, fields
from typing import List, Iterable
import html
import re
import sqlite3
import sys

from src.scrape.common import base_parser


def sub_if(pattern: str, replacement: str, string: str, **kwargs) -> str:
    matched = re.search(pattern, string, **kwargs)
    if matched is None:
        return string
    return re.sub(pattern, replacement, string, **kwargs)

def sub_all(pattern: str, replacement: str, string: str, **kwargs) -> str:
    while string:
        previous_len = len(string)
        string = sub_if(pattern, replacement, string, **kwargs)
        if previous_len == len(string):
            break
    return string

abbreviations = {
    "AE": "Allen Edmonds",
    "C and J": "Crockett and Jones",
    "G and G": "Gaziano and Girling",
    "J Crew": "JCrew",
    "J Fitzpatrick": "JFitzpatrick",
    "RW": "Red Wing",
    "Red Wings": "Red Wing",
    "SLP": "Saint Laurent",
    "Thursdays": "Thursday",
    "MMM": "MMM",  # Leave this alone - no one types the full name out..
    "MTM": "made-to-measure",  # Not a brand.
    "MTO": "made-to-order",  # Not a brand.
    "GYW": "goodyear welt",  # Not a brand,
}

def sub_abbreviations(string: str) -> str:
    for fixed, replacement in abbreviations.items():
        pattern = rf"\b{fixed}\b"
        string = sub_if(pattern, replacement, string)
    return string

def process(title: str) -> str:
    escaped = html.unescape(title)

    # Both ampersands and "and" are used. The latter, as part of
    # a name, is undetectable via regex, so we replace the former.
    spaced = sub_if(r"([^\s])&([^\s])", r"\1 & \2", escaped)
    compounded = sub_if(r"\s&\s", " and ", spaced)

    # e.g. white's -> whites, red wing's -> red wings
    unpossessed = sub_if(r"(\w+)'s", r"\1", compounded, flags=re.IGNORECASE)

    # e.g. R.M. Williams -> RM Williams, RM. Williams -> RM Williams
    undotted = sub_all(r"\.", "", unpossessed)

    # Account for known abbreviations.
    expanded = sub_abbreviations(undotted)
    return expanded

@dataclass(frozen=True)
class Annotation:
    submission_id: str
    brand: str
    start_pos: int
    end_pos: int

annotation_fields = tuple(f.name for f in fields(Annotation))

def make_annotations(doc: str, brands: Iterable[str], id_: str) -> List[Annotation]:
    annotations = []
    for brand in brands:
        for match in re.finditer(brand, doc, flags=re.IGNORECASE):
            annotations.append(
                Annotation(
                    submission_id=id_,
                    brand=brand,
                    start_pos=match.start(),
                    end_pos=match.end()
                )
            )
    return annotations

def main() -> int:
    parser = base_parser(description=__doc__)
    parser.add_argument("-b", "--brands", type=str, help="File containing known brands.")
    parser.add_argument("-d", "--dst", type=str, help="Output file")
    args = parser.parse_args()

    with open(args.brands) as fh:
        known_brands = {process(brand.strip()) for brand in fh.readlines()}

    with sqlite3.connect(args.conn) as conn:
        cursor = conn.cursor()
        cursor.execute("select id, title from submissions")
        titles = [(s_id, process(title)) for s_id, title in cursor]

    annotations = []
    for s_id, title in titles:
        annotations.extend(make_annotations(title, known_brands, s_id))

    with open(args.dst, "w") as output_fh:
        writer = DictWriter(output_fh, annotation_fields)
        writer.writeheader()
        writer.writerows(map(asdict, annotations))

    return 1

if __name__ == "__main__":
    sys.exit(main())
