#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -u  # Treat unset variables as an error

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <mongodb-version>"
  echo "Example: $0 8.0.5"
  exit 1
fi

MONGODB="$1"

# Determine build name based on version
if [[ "${MONGODB}" =~ ^(7.0|8.0) ]]; then
  mongo_build="mongodb-linux-x86_64-ubuntu2004-${MONGODB}"
elif [[ "${MONGODB}" =~ ^(4.0|4.2|4.4|5.0|6.0) ]]; then
  mongo_build="mongodb-linux-x86_64-ubuntu1804-${MONGODB}"
elif [[ "${MONGODB}" =~ ^3.6 ]]; then
  mongo_build="mongodb-linux-x86_64-${MONGODB}"
else
  echo "Error: Unsupported MongoDB version: ${MONGODB}"
  usage
fi

download_url="http://fastdl.mongodb.org/linux/${mongo_build}.tgz"
tarball="${mongo_build}.tgz"

echo "Downloading MongoDB from ${download_url}..."
if ! wget --quiet "${download_url}"; then
  echo "Error: Failed to download MongoDB."
  exit 1
fi

echo "Extracting ${tarball}..."
if ! tar xzf "${tarball}"; then
  echo "Error: Failed to extract ${tarball}"
  exit 1
fi

mongodb_dir=$(find "${PWD}/" -type d -name "mongodb-linux-x86_64*" | head -n 1)
if [ -z "${mongodb_dir}" ]; then
  echo "Error: Could not find MongoDB directory after extraction."
  exit 1
fi

echo "MongoDB installed at: ${mongodb_dir}"
"${mongodb_dir}/bin/mongod" --version

# Cleanup
echo "Cleaning up..."
rm -f "${tarball}"
