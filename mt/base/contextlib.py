'''Monkey-patching contextlib.'''

from packaging import version
import platform

if version.parse(platform.python_version()) < version.parse('3.7'):
    from contextlib2 import *
else:
    from contextlib import *
