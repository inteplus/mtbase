"""Useful functions dealing with sparse numpy arrays."""


import typing as tp
import numpy as np

from .ndarray import to_b85, from_b85


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

    # ----- serialisation -----

    def to_ndarray(self) -> str:
        arr = np.zeros(self.dense_shape, dtype=self.values.dtype)
        arr[tuple(self.indices.T)] = self.values
        return arr

    @staticmethod
    def from_ndarray(arr: np.ndarray) -> "SparseNdarray":
        nonzero = np.nonzero(arr)
        values = arr[nonzero]
        indices = np.stack(nonzero, axis=-1)
        dense_shape = arr.shape
        return SparseNdarray(values, indices, dense_shape)

    def to_json(self) -> str:
        """Serialises to a dictionary."""
        return {
            "shape": self.dense_shape.tolist(),
            "indices": to_b85(self.indices),
            "values": to_b85(self.values),
        }

    @staticmethod
    def from_json(json_obj: str) -> "SparseNdarray":
        """Deserialises from a dictionary"""
        values = from_b85(json_obj["values"])
        indices = from_b85(json_obj["indices"])
        shape = tuple(json_obj["shape"])
        return SparseNdarray(values, indices, shape)

    def to_spcoo(self) -> "scipy.sparse.coo_array":
        """Serialises to a :class:`scipy.sparse.coo_array` instance.

        Only works with a matrix or a vector, the latter is treated as a matrix with 1 column.
        """
        rank = len(self.dense_shape)
        if rank > 2 or rank < 1:
            msg = f"Only matrices or vectors are accepted. The array has shape {self.dense_shape}."
            raise ValueError(msg)

        import scipy.sparse as ss

        if rank == 2:
            return ss.coo_array(
                (self.values, (self.indices[:, 0], self.indices[:, 1])),
                shape=self.dense_shape,
            )

        if rank == 1:
            return ss.coo_array(
                (self.values, (self.indices[:, 0], [0] * len(self.values))),
                shape=self.dense_shape + (1,),
            )

    @staticmethod
    def from_spcoo(arr) -> "SparseNdarray":
        """Deserialises from a sparse matrix of type :class:`scipy.sparse.coo_array`."""
        indices = np.stack([arr.row, arr.col], axis=1)
        return SparseNdarray(arr.data, indices, arr.shape)


def sparse_vector(
    values: np.ndarray, indices: np.ndarray, dim: tp.Optional[int] = None
) -> "scipy.sparse.coo_array":
    """Makes a sparse vector as a single-column sparse matrix in COO format of scipy.

    Invoking the function may require importing scipy.

    Attributes
    ----------
    values : array_like
        A 1D ndarray with shape (N,) containing all nonzero values. The dtype of 'values' specifies
        the dtype of the sparse ndarray.
    indices : array_like
        A 1D ndarray with shape (N,), containing the indices of the nonzero values. Let D be th
        maximum value of any element of `indices`, plus 1.
    dim : int
        the dimensionality of the sparse vector. Must be greater than or equal to D if provided.
        Otherwise, it is set to D.
    """

    D = max(indices) + 1 if indices else 1
    if dim is None:
        dim = D
    elif dim < D:
        raise ValueError(f"Argument 'dim' must be at least {D}. Got {dim} instead.")

    import scipy.sparse as ss  # need scipy

    N = len(values)
    arg1 = (values, (indices, np.zeros(N, dtype=int)))
    return ss.coo_array(arg1, shape=(dim, 1))
