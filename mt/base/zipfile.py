'''Utilities dealing with a zip file.'''


import zipfile as _zf
import tempfile as _tf

from .path import make_dirs, remove

__all__ = ['extract_temporarily', 'ExtractedFolder']


class ExtractedFolder(object):
    '''A scope for use in a with statement. Upon exiting, the extracted folder is removed.

    The 'dirpath' attribute of the with object contains the extracted dirpath.
    '''

    def __init__(self, dirpath):
        if isinstance(dirpath, _tf.TemporaryDirectory):
            self.temp_dir = dirpath
            self.dirpath = dirpath.name
        else:
            self.temp_dir = None
            self.dirpath = dirpath
    
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self.temp_dir is None:
            remove(self.dirpath)


def extract_temporarily(zipfile, out_dirpath=None):
    '''Extracts a zip file to an local dirpath, deleting the dirpath after use.

    Parameters
    ----------
    zipfile : str or zipfile.ZipFile
        an open ZipFile instance or a local path to a zip file
    out_dirpath : str, optional
        If specified, a path to hold all the extracted zip files. Otherwise, a temporary directory is generated.

    Returns
    -------
    ExtractedFolder
        the extracted folder wrapped in an object for use in a with statement. The folder is removed upon exitting the with statement.
    '''

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
