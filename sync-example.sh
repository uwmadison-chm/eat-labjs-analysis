#!/bin/bash

SOURCE="/var/www/www_root/qet/data/data.sqlite"
DESTINATION="/study/EXAMPLE/raw_data/eat"
STUDY="EXAMPLE"

source /home/bashrc
set_study $STUDY > /dev/null

cp $SOURCE $DESTINATION
chmod 700 $DESTINATION/data.sqlite
cd "$(dirname "$0")"
python3 unpack-labjs-from-sqlite.py $DESTINATION/data.sqlite $DESTINATION
