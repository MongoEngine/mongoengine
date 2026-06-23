#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -u  # Treat unset variables as an error

if [ "$#" -ne 1 ]; then
  echo >&2 "Usage: $0 <mongodb-version>"
  echo >&2 "Example: $0 8.0.5"
  exit 1
fi

MONGODB="$1"
MONGOSH=2.5.1

# Determine build name based on version
if [[ "$MONGODB" =~ ^(6.0|7.0|8.0) ]]; then
  mongodb_build="mongodb-linux-x86_64-ubuntu2204-${MONGODB}"
elif [[ "$MONGODB" =~ ^(4.4|5.0) ]]; then
  mongodb_build="mongodb-linux-x86_64-ubuntu2004-${MONGODB}"
else
  echo >&2 "Error: Unsupported MongoDB version: ${MONGODB}"
  usage
fi

mongodb_tarball="${mongodb_build}.tgz"
mongodb_download_url="http://fastdl.mongodb.org/linux/${mongodb_tarball}"

mongosh_build="mongosh-${MONGOSH}-linux-x64"
mongosh_tarball="${mongosh_build}.tgz"
mongosh_download_url="https://github.com/mongodb-js/mongosh/releases/download/v${MONGOSH}/${mongosh_tarball}"

set -- \
  MongoDB "$mongodb_tarball" "$mongodb_download_url" \
  "MongoDB Shell" "$mongosh_tarball" "$mongosh_download_url"

while (( $# > 0 )) ; do
  name="$1"
  tarball="$2"
  download_url="$3"
  shift 3

  echo >&2 "Downloading ${name} from ${download_url}..."
  if ! wget --quiet "$download_url"; then
    echo >&2 "Error: Failed to download ${name}."
    exit 1
  fi

  echo >&2 "Extracting ${tarball}..."
  if ! tar xzf "${tarball}"; then
    echo >&2 "Error: Failed to extract ${tarball}"
    exit 1
  fi
done

mongodb_dir=$(find "${PWD}/" -type d -name "mongodb-linux-x86_64*" -print -quit)
if [ -z "$mongodb_dir" ]; then
  echo >&2 "Error: Could not find MongoDB directory after extraction."
  exit 1
fi

mongosh_dir=$(find "${PWD}/" -type d -name "$mongosh_build" -print -quit)
if [ ! -d "$mongosh_dir" ]; then
    echo >&2 "Failed to find extracted mongosh directory."
    rm -f "$TARBALL"
    exit 1
fi

# creating a ".path" file to make sure start_mongo is referring to the same binaries
# that this installation is referring to
echo >&2 "Creating mongo.path"
echo "export PATH='${mongodb_dir}/bin:${mongosh_dir}/bin:'"'$PATH' \
  | tee >&2 mongo.path

. mongo.path

echo >&2 "MongoDB is installed at: ${mongodb_dir}"
mongod >&2 --version

echo >&2 "MongoDB Shell is installed at: ${mongosh_dir}"
mongosh >&2 --version

# Cleanup
echo >&2 "Cleaning up..."
rm -f "$mongodb_tarball" "$mongosh_tarball"
