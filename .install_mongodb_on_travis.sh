#!/bin/bash

sudo apt-get remove mongodb-org-server
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10

if [ "$MONGODB" = "2.6" ]; then
    echo "deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen" | sudo tee /etc/apt/sources.list.d/mongodb.list
    sudo apt-get update
    sudo apt-get install mongodb-org-server=2.6.12
    # service should be started automatically
elif [ "$MONGODB" = "3.0" ]; then
    echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.0 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb.list
    sudo apt-get update
    sudo apt-get install mongodb-org-server=3.0.14
    # service should be started automatically
elif [ "$MONGODB" = "3.2" ]; then
    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv EA312927
    echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list
    sudo apt-get update
    sudo apt-get install mongodb-org-server=3.2.20
    # service should be started automatically
elif [ "$MONGODB" = "3.4" ]; then
    sudo apt-key adv --keyserver keyserver.ubuntu.com:80 --recv 0C49F3730359A14518585931BC711F9BA15703C6
    echo "deb http://repo.mongodb.org/apt/ubuntu trusty/mongodb-org/3.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.4.list
    sudo apt-get update
    sudo apt-get install mongodb-org-server=3.4.17
    # service should be started automatically
else
    echo "Invalid MongoDB version, expected 2.6, 3.0, 3.2 or 3.4."
    exit 1
fi;

mkdir db
1>db/logs mongod --dbpath=db &
