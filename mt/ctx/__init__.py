"""Monkey-patching contextlib.

For python 3.6 or earlier, this module corresponds to :module:`contextlib2`. Otherwise, it
corresponds to :module:`contextlib`.
"""

from packaging import version
import platform

if version.parse(platform.python_version()) < version.parse("3.7"):
    from contextlib2 import *
else:
    from contextlib import *
