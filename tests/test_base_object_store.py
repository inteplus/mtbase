import sys
import time
import unittest

sys.path.insert(0, "/home/minhtri/gitcentre/mtbase")

import mt.base

assert mt.base.__file__.startswith("/home/minhtri/gitcentre/mtbase/"), mt.base.__file__

from mt.base import object_store as os_


class TestObjectStore(unittest.TestCase):
    def test_get_refreshes_access_time(self):
        store = os_.create(min_free_mem_pct=0.0)
        os_.put(store, "a", 1)
        os_.put(store, "b", 2)
        t_before = store["item_a"][1]
        time.sleep(0.01)
        self.assertEqual(os_.get(store, "a"), 1)
        self.assertGreater(store["item_a"][1], t_before)
        self.assertGreater(store["item_a"][1], store["item_b"][1])

    def test_rotate_eviction_order(self):
        store = os_.create(min_free_mem_pct=0.0)
        os_.put(store, "a", 1)
        os_.put(store, "b", 2)
        os_.get(store, "a")
        removed = []
        orig = os_.remove

        def spy(s, k):
            removed.append(k)
            return orig(s, k)

        store["min_free_mem_pct"] = 1.01
        os_.remove = spy
        try:
            os_.put(store, "c", 3)
        finally:
            os_.remove = orig
        self.assertEqual([k for k in removed if k != "c"], ["b", "a"])

    def test_rejected_put_keeps_existing_object(self):
        store = os_.create(min_free_mem_pct=0.0, put_policy="strict")
        os_.put(store, "a", 1)
        store["min_free_mem_pct"] = 1.01
        self.assertFalse(os_.put(store, "a", 2))
        self.assertEqual(os_.get(store, "a"), 1)
        self.assertEqual(os_.count(store), 1)


if __name__ == "__main__":
    unittest.main()
