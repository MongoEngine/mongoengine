#!/bin/bash

MONGODB=$1
MONGOSH=$2

if (( $(echo "$MONGODB < 6.0" | bc -l) )); then
  echo "mongosh is not needed for MongoDB versions less than 6.0"
  exit 0
fi

wget https://downloads.mongodb.com/compass/mongosh-${MONGOSH}-linux-x64.tgz
tar xzf mongosh-${MONGOSH}-linux-x64.tgz

mongosh_dir=$(find ${PWD}/ -type d -name "mongosh-${MONGOSH}-linux-x64")
$mongosh_dir/bin/mongosh --version
