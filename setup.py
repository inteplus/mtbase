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
          'aiofiles', # for loading/saving files asynchronously
          'aiohttp', # for downloading http and https urls
          'aiobotocore', # for dealing with S3 files
          # 'boto3', # for fast uploading of massive files, only if the user uses mt.base.s3.put_files_boto3
          'filetype', # to determine the type of a file
          'halo', # for nice spinners
          'tqdm', # for nice progress bars
          #'matplotlib', # for drawing stuff, optional
      ],
      url='https://github.com/inteplus/mtbase',
      project_urls={
          'Documentation': 'https://mtdoc.readthedocs.io/en/latest/mt.base/mt.base.html',
          'Source Code': 'https://github.com/inteplus/mtbase',
          }
      )
