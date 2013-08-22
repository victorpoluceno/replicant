#!/bin/sh
sudo apt-get -y install couchdb htop vim python-virtualenv python-software-properties sqlite 3
sudo add-apt-repository ppa:longsleep/couchdb
sudo apt-get -y update
sudo apt-get install -y couchdb