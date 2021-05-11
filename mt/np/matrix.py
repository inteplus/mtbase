# pylint: disable=invalid-name
# pylint: disable=missing-function-docstring

'''Useful matrix functions.'''

import numpy as np


__all__ = ['psd_approx', 'sqrtm', 'powm']


def psd_approx(A):
    '''Approximates a real symmetric matrix with a positive semi-definite.

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


def sqrtm(A):
    '''Computes the matrix square root of a real positive semi-definite matrix.

    Parameters
    ----------
    A : numpy.ndarray
        2D array representing the input matrix

    Returns
    -------
    numpy.ndarray
        output square matrix such that its square equals the input matrix
    '''
    W, V = np.linalg.eig(A) # A = V diag(W) V^{-1}
    W = np.array([np.sqrt(x) if x > 0 else 0 for x in W])
    return V @ np.diag(W) @ V.T


def powm(A, exp):
    '''Computes the matrix power of a real positive semi-definite matrix.

    Parameters
    ----------
    A : numpy.ndarray
        2D array representing the input matrix
    exp : float
        power exponent

    Returns
    -------
    numpy.ndarray
        output square matrix such that it, raised to the power of exp, equals the input matrix
    '''
    W, V = np.linalg.eig(A) # A = V diag(W) V^{-1}
    W = np.array([pow(x, exp) if x > 0 else 0 for x in W])
    return V @ np.diag(W) @ V.T
