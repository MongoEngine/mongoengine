#!/bin/bash

MONGODB=$1

mongodb_dir=$(find ${PWD}/ -type d -name "mongodb-linux-x86_64*")

mkdir $mongodb_dir/data

args=(--dbpath $mongodb_dir/data --logpath $mongodb_dir/mongodb.log --fork --replSet mongoengine)
if (( $(echo "$MONGODB > 3.8" | bc -l) )); then
    args+=(--setParameter maxTransactionLockRequestTimeoutMillis=1000)
fi

$mongodb_dir/bin/mongod "${args[@]}"

if (( $(echo "$MONGODB < 6.0" | bc -l) )); then
mongo --verbose --eval "rs.initiate()"
mongo --quiet --eval "rs.status().ok"
else
mongosh --verbose --eval "rs.initiate()"
mongosh --quiet --eval "rs.status().ok"
fi
