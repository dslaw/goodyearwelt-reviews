"""Update spacy's NER model to recognize footwear brands."""

from numpy.random import RandomState
from pathlib import Path
from spacy.util import compounding, decaying, minibatch
import pandas as pd
import pickle
import spacy

from src.titles.annotate import process
from src.titles.utils import get_data, make_training


entity_label = "ORG"
holdout_brands = {
    process(brand) for brand in (
        "Allen Edmonds",
        "Carmina",
        "Crockett & Jones",
        "Meermin",
        "Nicks",
        "Zonkey"
    )
}


def get_batches(training_data):
    # https://spacy.io/usage/training#section-tips
    max_batch_size = 16 + 8
    n_samples = len(training_data)
    if n_samples < 1000:
        max_batch_size /= 2
    if n_samples < 500:
        max_batch_size /= 2
    batch_size = compounding(1, max_batch_size, 1.001)
    return minibatch(training_data, size=batch_size)

def check_random_state(random_state):
    if isinstance(random_state, RandomState):
        return random_state
    if random_state is None:
        return RandomState(None)
    if isinstance(random_state, int) or hasattr(random_state, "__iter__"):
        return RandomState(random_state)
    raise ValueError

class IncrementalTrainer(object):
    """Pause and unpause training."""

    def __init__(self, model, dropout_range, label, n_epochs=100, random_state=None):
        if len(dropout_range) != 2:
            raise ValueError
        if not isinstance(label, str):
            raise TypeError
        if n_epochs <= 0:
            raise ValueError

        self.model = model
        self.dropout_range = dropout_range
        self.dropout_gen = decaying(max(dropout_range), min(dropout_range), decay=.01)
        self.label = label
        self.n_epochs = n_epochs
        self.random_state = check_random_state(random_state)
        self.loss_chain = []

    def fit(self, training_data, stop_after=None, copy=True):
        ner = self.model.get_pipe("ner")
        # Assume all training data has only this label.
        ner.add_label(self.label)

        if copy:
            training_data = training_data[:]

        max_epochs = self.n_epochs
        if stop_after is not None:
            max_epochs = min(max_epochs, stop_after)

        other_pipes = [pipe for pipe in self.model.pipe_names if pipe != "ner"]
        with self.model.disable_pipes(*other_pipes):
            for i in range(max_epochs):
                self.random_state.shuffle(training_data)
                dropout = next(self.dropout_gen)
                batches = get_batches(training_data)
                losses = {}
                for batch in batches:
                    texts, annotations = zip(*batch)
                    self.model.update(
                        texts,
                        annotations,
                        drop=dropout,
                        losses=losses
                    )

                self.loss_chain.append(losses["ner"])
                print(i, losses["ner"])

        return self

    @staticmethod
    def _output_names(subdir, prefix):
        subdir = Path(subdir)
        model_subdir = subdir / f"{prefix}-ner_model"
        container_filename = subdir / f"{prefix}-ner_container"
        return model_subdir, container_filename

    def to_disk(self, subdir, prefix):
        model_subdir, container_filename = self._output_names(subdir, prefix)
        self.model.to_disk(model_subdir)

        stateful_params = {
            # Generator can't be pickled.
            "dropout_range": self.dropout_range,
            "label": self.label,
            "n_epochs": self.n_epochs,
            "random_state": self.random_state,
            "loss_chain": self.loss_chain,
        }
        with open(str(container_filename), "wb") as fh:
            pickle.dump(stateful_params, fh)

        return

    @classmethod
    def from_disk(cls, subdir, prefix, lang="en"):
        model_subdir, container_filename = cls._output_names(subdir, prefix)

        if not model_subdir.exists() or not container_filename.exists():
            raise FileNotFoundError

        nlp = spacy.load(lang)
        model = nlp.from_disk(model_subdir)

        with open(str(container_filename), "rb") as fh:
            params = pickle.load(fh)

        trainer = cls(
            model=model,
            dropout_range=params["dropout_range"],
            label=params["label"],
            n_epochs=params["n_epochs"],
            random_state=params["random_state"],
        )
        trainer.loss_chain = params["loss_chain"]

        # Advance dropout generator to correct position.
        for _ in trainer.loss_chain:
            next(trainer.dropout_gen)

        return trainer


if __name__ == "__main__":
    output_dir = Path("data")
    output_prefix = "titles"
    params = {
        "dropout_range": (.8, .5),
        "label": entity_label,
        "n_epochs": 120,
        "random_state": 13,
    }
    run_size = 30

    input_filename = "data/title_brand_annotations.csv"
    db_filename = "data/posts.sqlite"

    try:
        trainer = IncrementalTrainer.from_disk(output_dir, output_prefix)
    except FileNotFoundError:
        nlp = spacy.load("en_core_web_sm")
        trainer = IncrementalTrainer(nlp, **params)

    df = get_data(db_filename, input_filename)
    is_holdout = df.brand.isin(holdout_brands)
    rest = df[~is_holdout].reset_index(drop=True)

    annotations = make_training(rest, label=entity_label)

    rs = RandomState(131313)
    n_documents = len(annotations)
    train_size = int(.7 * n_documents)
    print(f"Training with: {train_size} / {n_documents} documents")

    training_indices = rs.choice(n_documents, size=train_size, replace=False)
    training_documents = [annotations[i] for i in training_indices]

    trainer.fit(training_documents, stop_after=run_size, copy=False)
    trainer.to_disk(output_dir, output_prefix)