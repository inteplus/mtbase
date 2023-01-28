"""Useful functions dealing with sparse numpy arrays."""


import numpy as np


class SparseNdarray:
    """A sparse ndarray, following TensorFlow's convention.

    Attributes
    ----------
    values : numpy.ndarray
        A 1D ndarray with shape (N,) containing all nonzero values. The dtype of 'values' specifies
        the dtype of the sparse ndarray.
    indices : numpy.ndarray
        A 2D ndarray with shape (N, rank), containing the indices of the nonzero values.
    dense_shape : tuple
        An integer tuple with 'rank' elements, specifying the shape of the ndarray.
    """

    def __init__(self, values: np.ndarray, indices: np.ndarray, dense_shape: tuple):
        self.values = values
        self.indices = indices
        self.dense_shape = dense_shape

    def __repr__(self):
        return "SparseNdarray(dense_shape=%r, dtype=%r, len(indices)=%r)" % (
            self.dense_shape,
            self.values.dtype,
            len(self.indices),
        )

    @staticmethod
    def from_ndarray(arr: np.ndarray):
        nonzero = np.nonzero(arr)
        values = arr[nonzero]
        indices = np.stack(nonzero, axis=-1)
        dense_shape = arr.shape
        return SparseNdarray(values, indices, dense_shape)

    def to_ndarray(self):
        arr = np.zeros(self.dense_shape, dtype=self.values.dtype)
        arr[tuple(self.indices.T)] = self.values
        return arr
