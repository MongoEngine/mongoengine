#!/bin/bash

MONGODB=$1

mongodb_dir=$(find ${PWD}/ -type d -name "mongodb-linux-x86_64*")

mkdir $mongodb_dir/data

$mongodb_dir/bin/mongod --dbpath $mongodb_dir/data --logpath $mongodb_dir/mongodb.log --fork --replSet mongoengine
if (( $(echo "$MONGODB < 6.0" | bc -l) )); then
mongo --verbose --eval "rs.initiate()"
mongo --quiet --eval "rs.status().ok"
else
mongosh --verbose --eval "rs.initiate()"
mongosh --quiet --eval "rs.status().ok"
fi
