"""Features related to ipython, jupyter and the like.

The public functions are :func:`inside_ipython`, :func:`inside_sagemaker`,
:func:`get_ipython_type` and :func:`display_page_in_jupyter`.
"""

from .ipython import *
from .jupyter import *


__api__ = [
    "inside_ipython",
    "inside_sagemaker",
    "get_ipython_type",
    "display_page_in_jupyter",
]
