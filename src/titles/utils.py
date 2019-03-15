from typing import Any, Dict, List, Tuple
import pandas as pd
import sqlite3
from spacy.gold import GoldParse
from spacy.tokens import Doc

from src.titles.annotate import process


def get_data(db_name: str, annotations_filename: str) -> pd.DataFrame:
    annotations = pd.read_csv(annotations_filename)
    with sqlite3.connect(db_name) as conn:
        titles = pd.read_sql(
            "select id, title as doc from submissions",
            con=conn
        )
        products = pd.read_sql(
            "select cast(id as text) as id, description as doc from products "
            "where description is not null",
            con=conn,
        )

    df = (
        pd
        .concat((titles, products), axis=0)
        .merge(annotations, on="id")
    )
    df["doc"] = df.doc.map(process)
    df["brand"] = df.brand.map(process)
    empty = df.brand == ""
    return df.loc[~empty]


Entity = Tuple[int, int, str]
Annotation = Tuple[str, Dict[str, List[Entity]]]

def make_training(df: pd.DataFrame, label: str, grouper: str = "id") -> List[Annotation]:
    annotated = []
    for _, df_g in df.groupby(grouper):
        # There should only be one document within each group.
        text = df_g.iloc[0].doc
        entities = [
            (anno["start_pos"], anno["end_pos"], label)
            for anno in df_g.to_dict("record")
        ]
        annotated.append((text, {"entities": entities}))
    return annotated

DocGold = Tuple[Doc, GoldParse]

# TODO: do spacy langs have a base class?
def make_evaluation(model: Any, annotations: List[Annotation]) -> List[DocGold]:
    doc_golds = []
    for text, annotation in annotations:
        doc = model.make_doc(text)
        gold = GoldParse(doc, **annotations)
        doc_golds.append((doc, gold))
    return doc_golds
