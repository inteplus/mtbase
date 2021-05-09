# pylint: disable=invalid-name
# pylint: disable=missing-function-docstring

'''Useful matrix functions.'''

import numpy as np


def psd_approx(A):
    '''Approximates a symmetric matrix with a positive semi-definite.

    The approximated matrix is the one closest to the input matrix in Frobenius norm.

    Parameters
    ----------
    A : numpy.ndarray
        2D array representing the square input matrix

    Returns
    -------
    numpy.ndarray
        output square matrix of the same shape that is positive semi-definite
    '''

    W, V = np.linalg.eig(A) # A = V diag(W) V^{-1}
    W = np.where(W >= 0, W, 0)

    A = V @ np.diag(W) @ V.T

    return A
