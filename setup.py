#!/usr/bin/env python3

import os
import platform
from packaging import version as V
from setuptools import setup, find_namespace_packages

install_requires = [
    "mtuv",  # for Python package management
    "psutil",
    "colorama",  # for colored text
    "numpy",  # for numerical computing
    "aiofiles",  # for loading/saving files asynchronously
    "aiohttp",  # for downloading http and https urls
    "aioboto3",  # for dealing with S3 files
    "filetype",  # to determine the type of a file
    "halo",  # for nice spinners
    "tqdm",  # for nice progress bars
    "func-timeout",  # for mt.base.path.exists with timeout
    #'matplotlib', # for drawing stuff, optional
    #'nest_asyncio', # for running asyncio inside an IPython environment
]

python_version = platform.python_version()

if V.parse(python_version) < V.parse("3.7"):
    install_requires.append("contextlib2")  # to have nullcontext
    install_requires.append("asyncio37")  # to have asyncio 3.7

VERSION_FILE = os.path.join(os.path.dirname(__file__), "VERSION.txt")

setup(
    name="mtbase",
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
    setup_requires=["setuptools-git-versioning<2"],
    setuptools_git_versioning={
        "enabled": True,
        "version_file": VERSION_FILE,
        "count_commits_from_version_file": True,
        "template": "{tag}",
        "dev_template": "{tag}.dev{ccount}+{branch}",
        "dirty_template": "{tag}.post{ccount}",
    },
    license="MIT",
    license_files=["LICENSE"],
)
