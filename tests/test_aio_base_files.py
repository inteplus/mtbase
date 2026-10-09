import os
import sys
import stat
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, "/home/minhtri/gitcentre/mtbase")

import mt.aio
from mt import aio
from mt.aio import files as aiofiles_mod, path as aiopath

assert mt.aio.__file__.startswith("/home/minhtri/gitcentre/mtbase"), mt.aio.__file__

SYNC = {"async": False}
ASYN = {"async": True}


async def double_later(x, context_vars={}):
    await aio.sleep(0.001, context_vars=context_vars)
    return 2 * x


async def tagged(x, tag="", context_vars={}):
    return (x, tag, dict(context_vars))


class TestArun(unittest.TestCase):
    def test_sync_mode_does_not_raise(self):
        self.assertEqual(aio.arun(double_later, 3, context_vars=SYNC), 6)

    def test_sync_mode_forwards_context_and_kwds(self):
        x, tag, cv = aio.arun(tagged, 1, tag="t", context_vars={"async": False, "k": 5})
        self.assertEqual((x, tag), (1, "t"))
        self.assertEqual(cv, {"async": False, "k": 5})


class TestArunAsync(unittest.IsolatedAsyncioTestCase):
    async def test_async_mode(self):
        self.assertEqual(await aio.arun(double_later, 4, context_vars=ASYN), 8)


class TestFilesSync(unittest.TestCase):
    def test_write_binary_sync_make_dirs(self):
        with tempfile.TemporaryDirectory() as d:
            fp = os.path.join(d, "a", "b", "x.bin")
            n = aio.srun(aiofiles_mod.write_binary, fp, b"abc", make_dirs=True)
            self.assertEqual(n, 3)
            self.assertEqual(open(fp, "rb").read(), b"abc")
            self.assertEqual(os.listdir(os.path.dirname(fp)), ["x.bin"])

    def test_write_text_sync_make_dirs(self):
        with tempfile.TemporaryDirectory() as d:
            fp = os.path.join(d, "a", "x.txt")
            aio.srun(aiofiles_mod.write_text, fp, "hi", make_dirs=True)
            self.assertEqual(open(fp).read(), "hi")

    def test_npz_save_sync_make_dirs(self):
        import numpy as np

        with tempfile.TemporaryDirectory() as d:
            fp = os.path.join(d, "a", "x.npz")
            aio.srun(aiofiles_mod.npz_save, fp, {"x": np.arange(3)}, make_dirs=True)
            out = aio.srun(aiofiles_mod.npz_load, fp)
            self.assertEqual(out["x"].tolist(), [0, 1, 2])

    def test_write_sync_without_make_dirs_still_fails(self):
        with tempfile.TemporaryDirectory() as d:
            fp = os.path.join(d, "nodir", "x.txt")
            with self.assertRaises(OSError):
                aio.srun(aiofiles_mod.write_text, fp, "hi")

    def test_safe_rename_honours_sync_context(self):
        with tempfile.TemporaryDirectory() as d:
            a, b = os.path.join(d, "a"), os.path.join(d, "b")
            open(a, "w").write("1")
            seen = []
            orig = aiofiles_mod.rename_asyn

            async def spy(*args, context_vars={}, **kwds):
                seen.append(context_vars["async"])
                return await orig(*args, context_vars=context_vars, **kwds)

            with mock.patch.object(aiofiles_mod, "rename_asyn", spy):
                aio.srun(aiofiles_mod.safe_rename, a, b)
            self.assertEqual(seen, [False])
            self.assertTrue(os.path.exists(b) and not os.path.exists(a))

    def test_safe_chmod_and_rename_sync(self):
        with tempfile.TemporaryDirectory() as d:
            a, b = os.path.join(d, "a"), os.path.join(d, "b")
            open(a, "w").write("1")
            aio.srun(aiofiles_mod.safe_chmod_and_rename, a, b, 0o600)
            self.assertEqual(stat.S_IMODE(os.stat(b).st_mode), 0o600)
            aio.srun(aiofiles_mod.safe_chmod, b, 0o640)
            self.assertEqual(stat.S_IMODE(os.stat(b).st_mode), 0o640)


class TestFilesAsync(unittest.IsolatedAsyncioTestCase):
    async def test_safe_rename_default_context_is_async(self):
        with tempfile.TemporaryDirectory() as d:
            a, b = os.path.join(d, "a"), os.path.join(d, "b")
            open(a, "w").write("1")
            await aiofiles_mod.safe_rename(a, b)
            self.assertTrue(os.path.exists(b))


class TestRemoveAsynControlFlow(unittest.TestCase):
    def test_rmtree_error_raised_on_linux(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "sub")
            os.mkdir(p)
            with mock.patch.object(aiopath._su, "rmtree", side_effect=OSError("boom")):
                with mock.patch.object(aiopath._pl, "system", return_value="Linux"):
                    with self.assertRaises(OSError):
                        aio.srun(aiopath.remove_asyn, p)

    def test_rmtree_error_swallowed_on_windows(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "sub")
            os.mkdir(p)
            with mock.patch.object(aiopath._su, "rmtree", side_effect=OSError("boom")):
                with mock.patch.object(aiopath._pl, "system", return_value="Windows"):
                    aio.srun(aiopath.remove_asyn, p)  # no error


class _FakeHandle:
    closed = False

    def close(self):
        self.closed = True


class TestCreateFileH5Exit(unittest.TestCase):
    """Exercises __exit__ with a stub handle, so h5py is not needed."""

    def _make(self, d, file_mode):
        c = aiofiles_mod.CreateFileH5(os.path.join(d, "x.h5"), file_mode=file_mode)
        c.tmp_filepath = aiofiles_mod.make_mttmp_filepath(c.filepath)
        open(c.tmp_filepath, "w").write("data")
        c.handle = _FakeHandle()
        return c

    def test_exit_with_file_mode(self):
        with tempfile.TemporaryDirectory() as d:
            c = self._make(d, 0o600)
            c.__exit__(None, None, None)
            self.assertTrue(c.handle.closed)
            self.assertEqual(stat.S_IMODE(os.stat(c.filepath).st_mode), 0o600)
            self.assertFalse(os.path.exists(c.tmp_filepath))

    def test_exit_on_exception_closes_handle_and_keeps_tmp(self):
        with tempfile.TemporaryDirectory() as d:
            c = self._make(d, 0o600)
            c.__exit__(ValueError, ValueError("x"), None)
            self.assertTrue(c.handle.closed)
            self.assertTrue(os.path.exists(c.tmp_filepath))
            self.assertFalse(os.path.exists(c.filepath))


if __name__ == "__main__":
    unittest.main()
