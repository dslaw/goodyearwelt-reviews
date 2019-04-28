"""Perform cross-validation for simple models using word embeddings."""

from sklearn import metrics
from sklearn.externals import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import KFold
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

    # See Das et al (2015), section 3 for justification of assuming embeddings
    # are multivariate Gaussian.
    # https://rajarshd.github.io/papers/acl2015.pdf
    models = [
        ("logit", LogisticRegression(penalty="l1", solver="liblinear", random_state=13)),
        ("hmm", HMM(algorithm="viterbi")),
        ("hmm_reg", RegularizedHMM(algorithm="viterbi", alpha=.5, assume_centered=True)),
    ]

    scaler = StandardScaler(copy=True).fit(X_train)
    X_train = scaler.transform(X_train)
    X_test = scaler.transform(X_test)

    for name, model in models:
        print(f"Running {name} for fold {k}")

        model.fit(X_train, y_train)

        # XXX: For HMMs, using `predict_proba` corresponds to MAP decoding.
        if "HMM" in name:
            pred_train = model.predict_proba(X_train, lengths=lengths_train)
            pred_test = model.predict_proba(X_test, lengths=lengths_test)
        else:
            pred_train = model.predict_proba(X_train)
            pred_test = model.predict_proba(X_test)

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
            "name": name,
            "k": k,
            "train": True,
            "log_loss": train_loss,
            "precision": train_precision,
            "recall": train_recall,
        })
        cv_values.append({
            "name": name,
            "k": k,
            "train": False,
            "log_loss": test_loss,
            "precision": test_precision,
            "recall": test_recall,
        })

within_cv = pd.DataFrame.from_records(cv_values)
within_cv.to_csv(output_filename, index=False)
