"""Features related to ipython, jupyter and the like."""

from .ipython import *
from .jupyter import *


__api__ = [
    "inside_ipython",
    "get_ipython_type",
    "display_page_in_jupyter",
]
