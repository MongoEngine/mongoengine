#!/bin/bash

MONGODB=$1

# Mongo > 4.0 follows different name convention for download links
mongo_build=mongodb-linux-x86_64-${MONGODB}

if [[ "$MONGODB" == *"4.2"* ]]; then
  mongo_build=mongodb-linux-x86_64-ubuntu1804-v${MONGODB}-latest
elif [[ "$MONGODB" == *"4.4"* ]]; then
  mongo_build=mongodb-linux-x86_64-ubuntu1804-v${MONGODB}-latest
elif [[ "$MONGODB" == *"5.0"* ]]; then
  mongo_build=mongodb-linux-x86_64-ubuntu1804-v${MONGODB}-latest
elif [[ "$MONGODB" == *"6.0"* ]]; then
  mongo_build=mongodb-linux-x86_64-ubuntu1804-v${MONGODB}-latest
elif [[ "$MONGODB" == *"7.0"* ]]; then
  mongo_build=mongodb-linux-x86_64-ubuntu2004-v${MONGODB}-latest
fi

wget http://fastdl.mongodb.org/linux/$mongo_build.tgz
tar xzf $mongo_build.tgz

mongodb_dir=$(find ${PWD}/ -type d -name "mongodb-linux-x86_64*")
$mongodb_dir/bin/mongod --version
