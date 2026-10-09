"""Utilities dealing with a zip file."""


import zipfile as _zf
import tempfile as _tf

from .path import make_dirs, remove

__all__ = ["extract_temporarily", "ExtractedFolder"]


class ExtractedFolder(object):
    """A scope for use in a with statement that holds the folder of an extracted zip file.

    The `dirpath` attribute of the with object contains the extracted dirpath. Normally you get an
    instance from :func:`extract_temporarily` rather than creating it yourself.

    On exit from the with statement, the folder is removed, whether it was extracted to a
    user-specified directory or to a temporary directory created by :func:`extract_temporarily`.

    Parameters
    ----------
    dirpath : str or tempfile.TemporaryDirectory
        path to the extracted folder, or a temporary directory object whose name is the path
    """

    def __init__(self, dirpath):
        """Initialises the scope. See the class docstring for the parameters."""
        if isinstance(dirpath, _tf.TemporaryDirectory):
            self.temp_dir = dirpath
            self.dirpath = dirpath.name
        else:
            self.temp_dir = None
            self.dirpath = dirpath

    def __enter__(self):
        """Returns the object itself, so `dirpath` can be read in the with statement."""
        return self

    def __exit__(self, type, value, traceback_obj):
        """Removes the extracted folder (see the class docstring)."""
        if self.temp_dir is None:
            remove(self.dirpath)
        else:
            self.temp_dir.cleanup()


def extract_temporarily(zipfile, out_dirpath=None):
    """Extracts a zip file to a local dirpath, to be deleted after use.

    Parameters
    ----------
    zipfile : str or zipfile.ZipFile
        an open ZipFile instance or a local path to a zip file
    out_dirpath : str, optional
        If specified, a path to hold all the extracted zip files. It is created if needed.
        Otherwise, a temporary directory is generated.

    Returns
    -------
    ExtractedFolder
        the extracted folder wrapped in an object for use in a with statement. When `out_dirpath`
        is given or not, the folder is removed upon exiting the with statement.

    Examples
    --------
    >>> import io, os, zipfile
    >>> from mt.base.zipfile import extract_temporarily
    >>> buf = io.BytesIO()
    >>> with zipfile.ZipFile(buf, "w") as zf:
    ...     zf.writestr("data/hello.txt", "hi")
    ...
    >>> with zipfile.ZipFile(buf) as zf:
    ...     with extract_temporarily(zf) as folder:
    ...         print(sorted(os.listdir(folder.dirpath)))
    ...         print(open(os.path.join(folder.dirpath, "data", "hello.txt")).read())
    ['data']
    hi
    """

    if not isinstance(zipfile, _zf.ZipFile):
        zipfile = _zf.ZipFile(zipfile)

    if out_dirpath is None:
        out_dir = _tf.TemporaryDirectory()
        out_dirpath = out_dir.name
    else:
        out_dir = None

    make_dirs(out_dirpath)
    zipfile.extractall(out_dirpath)

    return ExtractedFolder(out_dirpath if out_dir is None else out_dir)
