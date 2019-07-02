import numpy as np
import pytest

from src.titles.hmm import HMM, RegularizedHMM, estimate_transmat


@pytest.mark.parametrize("lengths, expected", [
    (None, np.array([[.75, .25], [.5, .5]])),
    # Last element in `y` is excluded from the counting process.
    ([6, 1], np.array([[1, 0], [.5, .5]])),
])
def test_estimate_transmat(lengths, expected):
    n_classes = 2
    y = np.array([1, 1, 0, 0, 0, 0, 1])
    X = np.empty((len(y), 3), dtype=np.uint)

    out = estimate_transmat(n_classes, X, y, lengths=lengths)
    np.testing.assert_array_equal(out, expected)


class TestHMM(object):
    def test_init(self):
        hmm = HMM(algorithm="foo")
        assert hmm.algorithm == "foo"
        assert hmm.covariance_type == "full"

    def test_estimate(self):
        hmm = HMM()
        X = np.array([[0, 1], [2, 3]])
        mean, cov = hmm._estimate(X)

        np.testing.assert_array_equal(mean, np.array([1, 2]))
        np.testing.assert_array_equal(cov, np.array([[2, 2], [2, 2]]))

    # Smoke test.
    def test_fit(self):
        rs = np.random.RandomState(13)
        mean = np.array([0, .2])
        cov = np.array([[1, 0], [0, 1]])
        X = rs.multivariate_normal(mean, cov, size=100)
        y = rs.choice([0, 1], replace=True, size=100)

        hmm = HMM().fit(X, y=y, lengths=None)
        assert hmm.n_features == 2
        assert hmm.n_components == 2
        assert hmm.startprob_.shape == (2,)
        assert hmm.transmat_.shape == (2, 2)
        assert hmm.means_.shape == (2, 2)
        assert hmm.covars_.shape == (2, 2, 2)

# TODO
class TestRegularizedHMM(object):
    def test_init(self):
        hmm = RegularizedHMM(algorithm="foo", alpha=.5, assume_centered=True, max_iter=10)
        assert hmm.algorithm == "foo"
        assert hmm.alpha == .5
        assert hmm.covariance_type == "full"
        assert hmm.max_iter == 10

    def test_estimate(self):
        X = np.array([[1, 1], [2, 2], [3, 3]])
        hmm = RegularizedHMM(alpha=.5, max_iter=2)
        _, reg_cov = hmm._estimate(X)
        cov = np.cov(X.T)
        assert np.all(reg_cov <= cov)
