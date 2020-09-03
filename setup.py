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
          'colorama',  # for colored text
          'Cython',  # for fast speed on tiny objects
          'tornado>=6.0.4', # 2020/08/27: tornado 6 is needed and distributed must be >= v2.24.0
          'dask>=2.24.0', # for simple multiprocessing jobs
          'distributed>=2.24.0',  # for simple multiprocessing jobs
      ],
      url='https://github.com/inteplus/mtbase',
      project_urls={
          'Documentation': 'https://mtdoc.readthedocs.io/en/latest/mt.base/mt.base.html',
          'Source Code': 'https://github.com/inteplus/mtbase',
          }
      )
