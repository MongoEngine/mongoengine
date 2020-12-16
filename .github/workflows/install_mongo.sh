#!/bin/bash

MONGODB=$1

mongo_build=mongodb-linux-x86_64-${MONGODB}
wget http://fastdl.mongodb.org/linux/$mongo_build.tgz
tar xzf $mongo_build.tgz
${PWD}/$mongo_build/bin/mongod --version
