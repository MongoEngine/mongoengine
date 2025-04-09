#!/bin/bash

MONGODB=$1

mongodb_dir=$(find ${PWD}/ -type d -name "mongodb-linux-x86_64*")

mkdir $mongodb_dir/data

args=(--dbpath $mongodb_dir/data --logpath $mongodb_dir/mongodb.log --fork --replSet mongoengine)

# Parse version components
MAJOR=$(echo "$MONGODB" | cut -d'.' -f1)
MINOR=$(echo "$MONGODB" | cut -d'.' -f2)
if [ "$MAJOR" -gt 3 ] || ([ "$MAJOR" -eq 3 ] && [ "$MINOR" -ge 8 ]); then
    args+=(--setParameter maxTransactionLockRequestTimeoutMillis=1000)
fi

$mongodb_dir/bin/mongod "${args[@]}"

if [ "$MAJOR" -lt 6 ]; then
mongo --verbose --eval "rs.initiate()"
mongo --quiet --eval "rs.status().ok"
else
mongosh --verbose --eval "rs.initiate()"
mongosh --quiet --eval "rs.status().ok"
fi
