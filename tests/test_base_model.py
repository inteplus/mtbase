import sys
import unittest

sys.path.insert(0, "/home/minhtri/gitcentre/mtbase")

import mt.base

assert mt.base.__file__.startswith("/home/minhtri/gitcentre/mtbase/"), mt.base.__file__

from mt.base.model import NameScope


class TestNameScope(unittest.TestCase):
    def test_next_advances(self):
        s = NameScope("blk")
        self.assertEqual(s.prefix(), "blk_0")
        next(s)
        self.assertEqual(s.prefix(), "blk_1")
        next(s)
        self.assertEqual(s("a"), f"blk_2{s._sep}a")

    def test_iter_resets_and_returns_self(self):
        s = NameScope("blk")
        next(s)
        it = iter(s)
        self.assertIs(it, s)
        self.assertEqual(s.prefix(), "blk_0")

    def test_full_prefix_includes_all_ancestors(self):
        a = NameScope("a")
        b = NameScope("b", parent_scope=a)
        c = NameScope("c", parent_scope=b)
        sep = a._sep
        self.assertEqual(c.prefix(True), sep.join(["a_0", "b_0", "c_0"]))
        next(a)
        self.assertEqual(c("x"), sep.join(["a_1", "b_0", "c_0", "x"]))


if __name__ == "__main__":
    unittest.main()
