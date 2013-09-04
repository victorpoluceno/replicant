#!/bin/sh
sudo apt-get -y install couchdb htop vim python-virtualenv python-dev python-software-properties sqlite3 libevent1-dev
sudo add-apt-repository ppa:longsleep/couchdb
sudo apt-get -y update
sudo apt-get install -y couchdb