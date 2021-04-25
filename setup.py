#!/usr/bin/env python3

from setuptools import setup, find_namespace_packages
from mt.base.version import version

setup(name='mtbase',
      version=version,
      description="The most fundamental Python modules for Minh-Tri Pham",
      author=["Minh-Tri Pham"],
      packages=find_namespace_packages(include=['mt.*']),
      install_requires=[
          'psutil',
          'getmac',
          'netifaces',
          'colorama',  # for colored text
          'Cython',  # for fast speed on tiny objects
          'numpy', # common ndarray
          'matplotlib', # for drawing stuff
      ],
      url='https://github.com/inteplus/mtbase',
      project_urls={
          'Documentation': 'https://mtdoc.readthedocs.io/en/latest/mt.base/mt.base.html',
          'Source Code': 'https://github.com/inteplus/mtbase',
          }
      )
