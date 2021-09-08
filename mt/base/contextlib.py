'''Monkey-patching contextlib.'''

from packaging import version
import platform

from contextlib import *

if version.parse(platform.python_version()) < version.parse('3.7'):
    from async_generator import asynccontextmanager
