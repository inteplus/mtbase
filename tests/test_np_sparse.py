import sys
import unittest

sys.path.insert(0, "/home/minhtri/gitcentre/mtbase")

import numpy as np
import mt.np

assert mt.np.__file__.startswith("/home/minhtri/gitcentre/mtbase/"), mt.np.__file__

from mt.np.sparse import SparseNdarray, sparse_vector


class TestSparse(unittest.TestCase):
    def test_to_json_with_tuple_shape_roundtrip(self):
        a = np.array([[0, 2], [3, 0]])
        s = SparseNdarray.from_ndarray(a)
        j = s.to_json()
        self.assertEqual(j["shape"], [2, 2])
        s2 = SparseNdarray.from_json(j)
        self.assertEqual(s2.dense_shape, (2, 2))
        self.assertTrue((s2.to_ndarray() == a).all())

    def test_to_json_with_array_shape(self):
        s = SparseNdarray(np.array([1]), np.array([[0, 1]]), np.array([2, 3]))
        self.assertEqual(s.to_json()["shape"], [2, 3])

    def test_sparse_vector_numpy_indices(self):
        v = sparse_vector(np.array([1.0, 2.0]), np.array([0, 3]))
        self.assertEqual(v.shape, (4, 1))
        self.assertEqual(v.toarray().ravel().tolist(), [1.0, 0.0, 0.0, 2.0])

    def test_sparse_vector_empty(self):
        self.assertEqual(sparse_vector(np.array([]), np.array([], dtype=int)).shape, (1, 1))
        self.assertEqual(sparse_vector(np.array([]), []).shape, (1, 1))


if __name__ == "__main__":
    unittest.main()
