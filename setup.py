#!/usr/bin/env python3

import platform
from packaging import version as V
from setuptools import setup, find_namespace_packages
from mt.base.version import version

install_requires = [
    "packaging",  # for comparing versions
    "psutil",
    "getmac<0.9",  # a bug at getmac>=0.9 is stopping us from using get_mac_address properly
    "netifaces",
    "colorama",  # for colored text
    "Cython",  # for fast speed on tiny objects
    "numpy",  # common ndarray
    "aiofiles",  # for loading/saving files asynchronously
    "aiohttp",  # for downloading http and https urls
    "aiobotocore",  # for dealing with S3 files
    "s3transfer",  # for fast uploading of massive files
    "filetype",  # to determine the type of a file
    "halo",  # for nice spinners
    "tqdm",  # for nice progress bars
    "sshtunnel",  # for ssh tunnelling
    "func-timeout",  # for mt.base.path.exists with timeout
    #'matplotlib', # for drawing stuff, optional
    #'nest_asyncio', # for running asyncio inside an IPython environment
]

if V.parse(platform.python_version()) < V.parse("3.7"):
    install_requires.append("contextlib2")  # to have nullcontext
    install_requires.append("asyncio37")  # to have asyncio 3.7

setup(
    name="mtbase",
    version=version,
    description="The most fundamental Python modules for Minh-Tri Pham",
    author=["Minh-Tri Pham"],
    packages=find_namespace_packages(include=["mt.*"]),
    scripts=[
        "scripts/path_exists",
    ],
    install_requires=install_requires,
    python_requires=">=3.6",  # we still need to support TX2 modules coming with JetPack 4
    url="https://github.com/inteplus/mtbase",
    project_urls={
        "Documentation": "https://mtdoc.readthedocs.io/en/latest/mt.base/mt.base.html",
        "Source Code": "https://github.com/inteplus/mtbase",
    },
)
