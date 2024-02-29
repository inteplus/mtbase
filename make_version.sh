#!/bin/bash

# Version updating rules:
#
# - If you restructure the package to a large extent, raise the major version.
# - If you add or modify the API of a function or a class and you think it may affect other people, raise the minor version.
# - If you make bug fixes, docstring updates or anything that does not change the API of the package, just run the script to get the patch version updated.
#
# To compare version strings, use:
#
# ```
# from packaging import version
# print(version.parse("1.1.3") > version.parse("1.100.4a"))
# ```
SCRIPT_PATH="$( cd "$(dirname "$0")" ; pwd -P )"
VERSION_FILEPATH=${SCRIPT_PATH}/VERSION.txt

IFS=. read -r MAJOR_VERSION MINOR_VERSION PATCH_VERSION <<< $(cat ${VERSION_FILEPATH})
((PATCH_VERSION++))
FULL_VERSION="${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"

echo "Updating ${VERSION_FILEPATH}..."
echo "${FULL_VERSION}" > ${VERSION_FILEPATH}

# version.py
VERSION_DIRPATH=${SCRIPT_PATH}/mt/base
VERSION_FILEPATH=${VERSION_DIRPATH}/version.py

mkdir -p ${VERSION_DIRPATH}
echo "Updating ${VERSION_FILEPATH}..."

echo "MAJOR_VERSION = ${MAJOR_VERSION}"$'\r' > ${VERSION_FILEPATH}
echo "MINOR_VERSION = ${MINOR_VERSION}"$'\r' >> ${VERSION_FILEPATH}
echo "PATCH_VERSION = ${PATCH_VERSION}"$'\r' >> ${VERSION_FILEPATH}
echo "version = '{}.{}.{}'.format(MAJOR_VERSION, MINOR_VERSION, PATCH_VERSION)"$'\r' >> ${VERSION_FILEPATH}

echo "__all__  = ['MAJOR_VERSION', 'MINOR_VERSION', 'PATCH_VERSION', 'version']"$'\r' >> ${VERSION_FILEPATH}

git add -A
git commit -m "updated version.py to ${FULL_VERSION}"
git tag "${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"
echo "Repository has been tagged as version ${MAJOR_VERSION}.${MINOR_VERSION}.${PATCH_VERSION}"
