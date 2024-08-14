#!/bin/bash

MONGODB=$1

mongodb_dir=$(find ${PWD}/ -type d -name "mongodb-linux-x86_64*")

mkdir $mongodb_dir/data
$mongodb_dir/bin/mongod --dbpath $mongodb_dir/data --logpath $mongodb_dir/mongodb.log --fork

if (( $(echo "$MONGODB < 6.0" | bc -l) )); then
mongo --quiet --eval 'db.runCommand("ping").ok'    # Make sure mongo is awake
else
mongosh --quiet  --eval 'db.runCommand("ping").ok'  # Make sure mongo is awake
fi
