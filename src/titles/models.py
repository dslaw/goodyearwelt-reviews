"""Perform cross-validation for simple models using word embeddings."""

from sklearn import metrics
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import KFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
import numpy as np
import pandas as pd

from src.titles.hmm import HMM, RegularizedHMM
from src.titles.utils import split_holdout


data_filename = "data/title_embeddings.pkl"
output_filename = "data/model_cv.csv"

pos_class = 1

y, X = joblib.load(data_filename)
available, _ = split_holdout(list(zip(y, X)), frac=.1)
y, X = map(np.asarray, zip(*available))


# Each document is an indepdent sequence of tokens. For sequential
# models, the model should perform similary regardless of the order
# of sequences. For iid models, splitting at the document-level instead
# of over tokens may pose problematic as classes are highly imbalanced.
cv_values = []
kf = KFold(n_splits=7, shuffle=True, random_state=13)
for k, (train_idx, test_idx) in enumerate(kf.split(X)):
    X_train = np.concatenate(X[train_idx])
    y_train = np.concatenate(y[train_idx])
    lengths_train = np.array([len(seq) for seq in y[train_idx]])

    X_test = np.concatenate(X[test_idx])
    y_test = np.concatenate(y[test_idx])
    lengths_test = np.array([len(seq) for seq in y[test_idx]])

    pipelines = [
        Pipeline([
            ("scale", StandardScaler(copy=True)),
            ("logit", LogisticRegression(
                penalty="l1",
                solver="liblinear",
                random_state=13
            )),
        ]),
        # See Das et al (2015), section 3 for justification of assuming embeddings
        # as multivariate Gaussian distributed.
        # https://rajarshd.github.io/papers/acl2015.pdf
        Pipeline([
            ("hmm", HMM(algorithm="viterbi")),
        ]),
        Pipeline([
            ("scale", StandardScaler(copy=True)),
            ("hmm_reg", RegularizedHMM(
                algorithm="viterbi",
                alpha=.5,
                assume_centered=True
            )),
        ]),
    ]

    for pipeline in pipelines:
        model_name, _ = pipeline.steps[-1]
        print(f"Running {model_name} for fold {k}")

        pipeline.fit(X_train, y_train)

        if model_name.startswith("hmm"):
            # XXX: This corresponds to the log-loss of MAP decoding.
            _, model = pipeline.steps[-1]
            pred_train = model.predict_proba(X_train, lengths=lengths_train)
            pred_test = model.predict_proba(X_test, lengths=lengths_test)
        else:
            pred_train = pipeline.predict_proba(X_train)
            pred_test = pipeline.predict_proba(X_test)

        train_loss = metrics.log_loss(y_train, pred_train[:, pos_class])
        test_loss = metrics.log_loss(y_test, pred_test[:, pos_class])

        # More MAP decoding.
        label_train = np.argmax(pred_train, axis=1)
        label_test = np.argmax(pred_test, axis=1)
        train_precision = metrics.precision_score(y_train, label_train)
        test_precision = metrics.precision_score(y_test, label_test)
        train_recall = metrics.recall_score(y_train, label_train)
        test_recall = metrics.recall_score(y_test, label_test)

        cv_values.append({
            "name": model_name,
            "k": k,
            "train": True,
            "log_loss": train_loss,
            "precision": train_precision,
            "recall": train_recall,
        })
        cv_values.append({
            "name": model_name,
            "k": k,
            "train": False,
            "log_loss": test_loss,
            "precision": test_precision,
            "recall": test_recall,
        })

within_cv = pd.DataFrame.from_records(cv_values)
within_cv.to_csv(output_filename, index=False)
