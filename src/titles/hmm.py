"""Supervised Gaussian-HMM."""

from hmmlearn.hmm import GaussianHMM
from hmmlearn.utils import iter_from_X_lengths
from sklearn.covariance import graphical_lasso
import numpy as np


def estimate_transmat(n_classes, X, y, lengths):
    """MLE transition probabilities."""

    counts = np.zeros((n_classes, n_classes), dtype=np.int)
    for i, j in iter_from_X_lengths(X, lengths):
        for t in range(i + 1, j):
            prev = y[t - 1]
            curr = y[t]
            counts[prev, curr] += 1

    marginals = np.sum(counts, axis=1).reshape(n_classes, -1)
    return counts / marginals

class HMM(GaussianHMM):
    """Supervised Hidden Markov model with Gaussian emissions."""

    def __init__(self, algorithm="viterbi"):
        self.algorithm = algorithm
        self.covariance_type = "full"

    # TODO: handle vanishing variance.
    def _estimate(self, X):
        mean = np.mean(X, axis=0)
        cov = np.cov(X.T)
        return mean, cov

    def fit(self, X, y, lengths=None):
        classes, class_counts = np.unique(y, return_counts=True)
        n_classes = len(classes)
        n_samples, n_features = X.shape

        self.n_features = n_features
        self.n_components = n_classes

        # Empirical probability of starting in class `k`.
        if lengths is None:
            # Use overall proportions, as if only the first
            # observation in the sequence is used the probability
            # will be fixed.
            start_counts = class_counts
        else:
            start_counts = np.zeros((n_classes,), dtype=np.int)
            for i, _ in iter_from_X_lengths(X, lengths):
                start_class = y[i]
                start_counts[start_class] += 1

        startprob = start_counts / np.sum(start_counts)

        # Empirical parameters of Gaussian distributions.
        # XXX: How to reconcile this with `lengths`, if at all?
        means, covs = [], []
        for class_ in range(n_classes):
            X_s = X[y == class_]
            mean, cov = self._estimate(X_s)
            means.append(mean)
            covs.append(cov[np.newaxis, :])

        # Empirical transition probabilities.
        transmat = estimate_transmat(n_classes, X, y, lengths)

        self.startprob_ = startprob
        self.transmat_ = transmat
        self.means_ = np.array(means)
        self.covars_ = np.concatenate(covs, axis=0)
        return self

    def predict_proba(self, X, lengths=None):
        eps = np.finfo(float).eps
        posteriors = super().predict_proba(X, lengths=lengths)
        return np.clip(posteriors, eps, 1 - eps)


class RegularizedHMM(HMM):
    def __init__(self, algorithm="viterbi", alpha=0, max_iter=100):
        super().__init__(algorithm=algorithm)
        self.alpha = alpha  # TODO: validate
        self.max_iter = max_iter

    def _estimate(self, X):
        mean, cov = super()._estimate(X)
        reg_cov, _ = graphical_lasso(cov, alpha=self.alpha, max_iter=self.max_iter)
        return mean, reg_cov
