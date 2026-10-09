import ast
import logging
import sys
import unittest

sys.path.insert(0, "/home/minhtri/gitcentre/mtbase")

import mt.logg

assert mt.logg.__file__.startswith("/home/minhtri/gitcentre/mtbase/"), mt.logg.__file__


class TestAsyncioAlias(unittest.TestCase):
    def test_warning_is_actually_called(self):
        src = open("/home/minhtri/gitcentre/mtbase/mt/asyncio/__init__.py").read()
        tree = ast.parse(src)
        # a bare `warnings.warn` attribute expression statement is the bug
        for node in ast.walk(tree):
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Attribute):
                self.fail("bare attribute expression statement (warnings.warn not called)")
        calls = [
            n
            for n in ast.walk(tree)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "warn"
        ]
        self.assertEqual(len(calls), 1)


class TestMakeLogger(unittest.TestCase):
    def test_no_duplicate_handlers(self):
        name = "mtbase_test_dup_handlers"
        n0 = len(logging.getLogger(name).handlers)
        mt.logg.make_logger(name)
        n1 = len(logging.getLogger(name).handlers)
        mt.logg.make_logger(name)
        n2 = len(logging.getLogger(name).handlers)
        self.assertEqual(n1, n0 + 1)
        self.assertEqual(n2, n1)


if __name__ == "__main__":
    unittest.main()
