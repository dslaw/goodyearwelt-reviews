"""Create quantitative features from annotated documents."""

from typing import Any, List, Tuple
import joblib
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
    rs = np.random.RandomState(13)

    parser = base_parser(description=__doc__)
    parser.add_argument(
        "-a", "--annotations",
        type=str, help="File containing brand annotations"
    )
    parser.add_argument("-d", "--dst", type=str, help="Output file")
    parser.add_argument(
        "-p", "--proportion-holdout",
        default=.1, type=float, help="Proportion of documents to hold out"
    )
    args = parser.parse_args()

    if args.proportion_holdout < 0 or args.proportion_holdout > 1:
        raise ValueError("Holdout proportion must be in [0, 1]")

    df = get_data(args.conn, args.annotations)

    # Create disjoint out-of-vocabulary holdout, within-vocabulary holdout,
    # and "main" sets.
    is_oov = df.brand.isin(holdout_brands)
    oov_indices = df[is_oov].index
    rest = df[~is_oov]

    holdout_size = int(args.proportion_holdout * len(rest))
    holdout_indices = rs.choice(rest.index, size=holdout_size, replace=False)
    main_indices = rest.index[~rest.index.isin(holdout_indices)]

    # Persist indices to original dataframe so that text can be recovered.
    set_indices = (oov_indices, holdout_indices, main_indices)
    names = ("oov", "holdout", "main")
    sets = {}
    for name, indices in zip(names, set_indices):
        df_s = df.iloc[indices]
        annotations = make_training(df_s, label=entity_label)
        y, X = make_features(nlp, annotations)
        sets[name] = {"idx": indices, "y": y, "X": X}

    joblib.dump(sets, args.dst)
    return


if __name__ == "__main__":
    main()
