import sys
import unittest

sys.path.insert(0, "/home/minhtri/gitcentre/mtbase")

import mt.concurrency
from mt.concurrency.aio import concurrent_pool_func, asyn_pmap
import asyncio

assert mt.concurrency.__file__.startswith("/home/minhtri/gitcentre/mtbase")

state = {"running": 0, "peak": 0}


async def double(x, context_vars={}):
    return 2 * x


async def slow_double(x, context_vars={}):
    state["running"] += 1
    state["peak"] = max(state["peak"], state["running"])
    await asyncio.sleep(0.01 * (5 - x % 5))  # later items finish earlier
    state["running"] -= 1
    return 2 * x


async def fail_on_3(x, context_vars={}):
    await asyncio.sleep(0.01)
    if x == 3:
        raise KeyError(x)
    return x


class TestConcurrentPoolFunc(unittest.TestCase):
    def test_ordered_outputs(self):
        ok, out = concurrent_pool_func(tuple(range(8)), asyn_func=slow_double, max_concurrency=3)
        self.assertTrue(ok)
        self.assertEqual(out, [2 * i for i in range(8)])

    def test_concurrency_bound(self):
        state.update(running=0, peak=0)
        concurrent_pool_func(tuple(range(8)), asyn_func=slow_double, max_concurrency=3)
        self.assertEqual(state["peak"], 3)

    def test_misspelt_keyword(self):
        state.update(running=0, peak=0)
        ok, out = concurrent_pool_func(tuple(range(6)), asyn_func=slow_double, max_concurency=2)
        self.assertTrue(ok)
        self.assertEqual(state["peak"], 2)

    def test_empty(self):
        self.assertEqual(concurrent_pool_func((), asyn_func=double, max_concurrency=2), (True, []))

    def test_exception(self):
        ok, out = concurrent_pool_func(tuple(range(6)), asyn_func=fail_on_3, max_concurrency=2)
        self.assertFalse(ok)
        self.assertIsInstance(out, KeyError)

    def test_asyn_pmap_concurrent(self):
        out = list(
            asyn_pmap(
                double, range(10), block_size=4, max_concurrency_for_block_items=3,
                pool_processes=2,
            )
        )
        self.assertEqual(out, [2 * i for i in range(10)])


if __name__ == "__main__":
    unittest.main()
