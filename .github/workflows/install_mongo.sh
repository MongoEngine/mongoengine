#!/bin/bash

MONGODB=$1

# Mongo > 4.0 follows different name convention for download links
mongo_build=mongodb-linux-x86_64-${MONGODB}

if [[ "$MONGODB" == *"4."* ]] && [[ ! "$MONGODB" == *"4.0"* ]]; then
  echo "It's there."
  mongo_build=mongodb-linux-x86_64-ubuntu2004-v${MONGODB}-latest
fi

wget http://fastdl.mongodb.org/linux/$mongo_build.tgz
tar xzf $mongo_build.tgz
${PWD}/$mongo_build/bin/mongod --version
