# pylint: disable=invalid-name
# pylint: disable=missing-function-docstring

"""Useful matrix functions."""

import numpy as np


def psd_approx(A):
    """Approximates a real symmetric matrix with a positive semi-definite.

    The approximated matrix is the one closest to the input matrix in Frobenius norm. It is
    obtained by an eigendecomposition in which negative eigenvalues are set to zero. The
    reconstruction uses the transposed eigenvector matrix as the inverse, so the input must be
    symmetric.

    Parameters
    ----------
    A : numpy.ndarray
        2D array representing the square symmetric input matrix

    Returns
    -------
    numpy.ndarray
        output square matrix of the same shape that is positive semi-definite

    See Also
    --------
    sqrtm : matrix square root

    Examples
    --------
    >>> from mt import np
    >>> A = np.array([[1.0, 2.0], [2.0, 1.0]])  # eigenvalues 3 and -1
    >>> np.psd_approx(A).round(3)
    array([[1.5, 1.5],
           [1.5, 1.5]])
    """

    W, V = np.linalg.eig(A)  # A = V diag(W) V^{-1}
    W = np.where(W >= 0, W, 0)

    A = V @ np.diag(W) @ V.T

    return A


def sqrtm(A):
    """Computes the matrix square root of a real positive semi-definite matrix.

    The input is assumed symmetric. Non-positive eigenvalues are treated as zero.

    Parameters
    ----------
    A : numpy.ndarray
        2D array representing the input matrix

    Returns
    -------
    numpy.ndarray
        output square matrix such that its square equals the input matrix

    See Also
    --------
    powm : general matrix power

    Examples
    --------
    >>> from mt import np
    >>> A = np.array([[2.0, 1.0], [1.0, 2.0]])
    >>> S = np.sqrtm(A)
    >>> np.allclose(S @ S, A)
    True
    """
    W, V = np.linalg.eig(A)  # A = V diag(W) V^{-1}
    W = np.array([np.sqrt(x) if x > 0 else 0 for x in W])
    return V @ np.diag(W) @ V.T


def powm(A, exp):
    """Computes the matrix power of a real positive semi-definite matrix.

    The input is assumed symmetric. Non-positive eigenvalues are treated as zero, so the result is
    only meaningful for a positive exponent.

    Parameters
    ----------
    A : numpy.ndarray
        2D array representing the input matrix
    exp : float
        power exponent

    Returns
    -------
    numpy.ndarray
        output square matrix equal to the input matrix raised to the power of `exp`

    See Also
    --------
    sqrtm : the special case ``exp=0.5``

    Examples
    --------
    >>> from mt import np
    >>> A = np.array([[2.0, 1.0], [1.0, 2.0]])
    >>> np.powm(A, 2).round(3)
    array([[5., 4.],
           [4., 5.]])
    """
    W, V = np.linalg.eig(A)  # A = V diag(W) V^{-1}
    W = np.array([pow(x, exp) if x > 0 else 0 for x in W])
    return V @ np.diag(W) @ V.T
