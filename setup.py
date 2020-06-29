#!/usr/bin/env python3

from setuptools import setup, find_packages, find_namespace_packages
from mt.base.version import version

setup(name='basemt',
      version=version,
      description="The most fundamental Python modules for Minh-Tri Pham",
      author=["Minh-Tri Pham"],
      packages=find_packages() + find_namespace_packages(include=['mt.*']),
      install_requires=[
          'psutil',
          'colorama',  # for colored text
          'Cython',  # for fast speed on tiny objects
          'dask[distributed]',  # for simple multiprocessing jobs
      ],
      url='https://github.com/inteplus/basemt',
      project_urls={
          'Documentation': 'https://basemt.readthedocs.io/en/stable/',
          'Source Code': 'https://github.com/inteplus/basemt',
          }
      )
