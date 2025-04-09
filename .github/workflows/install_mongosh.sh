#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -u  # Treat unset variables as an error

if [ $# -lt 2 ]; then
    echo "Usage: $0 <mongodb_version> <mongosh_version>"
    echo "Example: $0 8.0.5 2.4.2"
    exit 1
fi

MONGODB=$1
MONGOSH=$2

PLATFORM="linux-x64"

# Extract major version explicitly
MONGODB_MAJOR_VERSION=$(echo "$MONGODB" | cut -d'.' -f1)

# Check if MongoDB major version is greater than or equal to 6
if [ "$MONGODB_MAJOR_VERSION" -lt 6 ]; then
    echo "mongosh is not needed for MongoDB versions less than 6.0"
    exit 0
fi

DOWNLOAD_URL="https://downloads.mongodb.com/compass/mongosh-${MONGOSH}-${PLATFORM}.tgz"
TARBALL="mongosh-${MONGOSH}-${PLATFORM}.tgz"

echo "Downloading mongosh ${MONGOSH} for ${PLATFORM}..."
if ! wget --quiet "$DOWNLOAD_URL"; then
    echo "Failed to download mongosh. Please check the version and your internet connection."
    exit 1
fi

echo "Extracting mongosh..."
if ! tar xzf "$TARBALL"; then
    echo "Failed to extract mongosh."
    rm -f "$TARBALL"
    exit 1
fi

mongosh_dir=$(find "${PWD}/" -type d -name "mongosh-${MONGOSH}-${PLATFORM}" -print -quit)
if [ ! -d "$mongosh_dir" ]; then
    echo "Failed to find extracted mongosh directory."
    rm -f "$TARBALL"
    exit 1
fi

echo "Testing mongosh installation..."
if ! "$mongosh_dir/bin/mongosh" --version; then
    echo "Failed to run mongosh."
    exit 1
fi

echo "Cleaning up..."
rm -f "$TARBALL"
