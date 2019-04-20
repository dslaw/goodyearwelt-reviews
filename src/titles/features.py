"""Create quantitative features from annotated documents."""

from sklearn.externals import joblib
from typing import Any, List, Tuple
import numpy as np
import spacy

from src.scrape.common import base_parser
from src.titles.train import entity_label, holdout_brands
from src.titles.utils import Annotation, get_data, make_training


def is_org(entity: str) -> int:
    return int("ORG" in entity)

def make_features(model: Any, annotations: List[Annotation]) -> Tuple[np.array, np.array]:
    """Make label and feature arrays from each document."""

    y_seqs: List[np.array] = []
    X_seqs: List[np.array] = []
    for text, annos in annotations:
        # NB: using `make_doc` will not generate the embeddings.
        doc = model(text)
        gold = spacy.gold.GoldParse(doc, **annos)
        labels = np.array([is_org(label) for label in gold.ner], dtype=np.uint)
        embeddings = np.array([token.vector for token in doc])
        y_seqs.append(labels)
        X_seqs.append(embeddings)

    return np.array(y_seqs), np.array(X_seqs)

def main() -> None:
    nlp = spacy.load("en")

    parser = base_parser(description=__doc__)
    parser.add_argument(
        "-a", "--annotations",
        type=str, help="File containing brand annotations"
    )
    parser.add_argument("-d", "--dst", type=str, help="Output file")
    args = parser.parse_args()

    df = get_data(args.conn, args.annotations)
    is_holdout = df.brand.isin(holdout_brands)
    rest = df[~is_holdout].reset_index(drop=True)

    annotations = make_training(rest, label=entity_label)
    y, X = make_features(nlp, annotations)
    joblib.dump((y, X), args.dst)

    return


if __name__ == "__main__":
    main()
