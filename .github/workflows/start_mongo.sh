#!/bin/bash

MONGODB=$1

mongodb_dir=$(find ${PWD}/ -type d -name "mongodb-linux-x86_64*")

mkdir $mongodb_dir/data
$mongodb_dir/bin/mongod --dbpath $mongodb_dir/data --logpath $mongodb_dir/mongodb.log --fork
mongo --eval 'db.version();'    # Make sure mongo is awake
