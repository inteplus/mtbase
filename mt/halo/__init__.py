# -*- coding: utf-8 -*-
__author__ = "Manraj Singh"
__email__ = "manrajsinghgrover@gmail.com"

from .halo import Halo
from .halo_notebook import HaloNotebook


def get_halo_class():
    from mt.ipython import get_ipython_type

    s = get_ipython_type()
    return HaloNotebook if s in ("jupyter", "colab", "sagemaker") else Halo


HaloAuto = get_halo_class()
