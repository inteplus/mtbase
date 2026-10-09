import asyncio
import os
import sys
import threading
import unittest
import warnings
import zipfile as zf_

sys.path.insert(0, "/home/minhtri/gitcentre/mtbase")

import mt.base

assert mt.base.__file__.startswith("/home/minhtri/gitcentre/mtbase/"), mt.base.__file__

from mt.base import bg_invoke, functional
from mt.base import zipfile as mtzip
from mt.base import filetype as mtft


class TestIteratorAsGenerator(unittest.TestCase):
    def test_exhausted_iterator_ends_generator(self):
        g = functional.iterator_as_generator(iter([1, 2]))
        self.assertEqual(list(g), [1, 2])

    def test_infinite(self):
        import itertools

        g = functional.iterator_as_generator(itertools.count(3))
        self.assertEqual([next(g), next(g)], [3, 4])


class TestParallelise(unittest.TestCase):
    def test_warn_without_logger_is_not_silent(self):
        def f(i):
            if i == 1:
                raise ValueError("boom-in-job")
            return i

        from mt import logg

        records = []

        class H(logg.Handler if hasattr(logg, "Handler") else __import__("logging").Handler):
            def emit(self, record):
                records.append(record.getMessage())

        h = H()
        logg.logger.logger.addHandler(h)
        try:
            out = bg_invoke.parallelise(f, 2, num_threads=2, bg_exception="warn")
        finally:
            logg.logger.logger.removeHandler(h)
        self.assertEqual(out, [0, None])
        self.assertTrue(any("boom-in-job" in m for m in records), records)


class TestExtractedFolder(unittest.TestCase):
    def test_temp_dir_removed_on_exit(self):
        import io

        buf = io.BytesIO()
        with zf_.ZipFile(buf, "w") as z:
            z.writestr("d/x.txt", "hi")
        keep = []
        with zf_.ZipFile(buf) as z:
            with mtzip.extract_temporarily(z) as folder:
                keep.append(folder)  # keep a reference, so GC cannot clean up
                path = folder.dirpath
                self.assertTrue(os.path.exists(os.path.join(path, "d", "x.txt")))
            self.assertFalse(os.path.exists(path))


class TestFiletype(unittest.TestCase):
    def test_missing_async_key_means_sync(self):
        png = b"\x89PNG\r\n\x1a\n" + bytes(300)
        self.assertTrue(asyncio.run(mtft.is_image_asyn(png)))
        self.assertTrue(asyncio.run(mtft.is_image_asyn(png, context_vars={})))
        self.assertEqual(asyncio.run(mtft.image_match_asyn(png)).extension, "png")


if __name__ == "__main__":
    unittest.main()
