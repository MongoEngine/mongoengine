#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status
set -u  # Treat unset variables as an error

. mongo.path

MONGODB=$1

mongodb_dir=$(find ${PWD}/ -type d -name "mongodb-linux-x86_64*" -print -quit)

mkdir $mongodb_dir/data

args=(--dbpath $mongodb_dir/data --logpath $mongodb_dir/mongodb.log --fork --replSet mongoengine)

# Parse version components
MAJOR=$(echo "$MONGODB" | cut -d'.' -f1)
MINOR=$(echo "$MONGODB" | cut -d'.' -f2)
if [ "$MAJOR" -gt 3 ] || ([ "$MAJOR" -eq 3 ] && [ "$MINOR" -ge 8 ]); then
    args+=(--setParameter maxTransactionLockRequestTimeoutMillis=1000)
fi

mongod "${args[@]}"
mongosh --verbose --eval "rs.initiate()"
mongosh --quiet --eval "rs.status().ok"
