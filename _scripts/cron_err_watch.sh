#!/bin/bash

#ADMIN should be the email address of the notifyee
ADMIN=your@email.com
#ERROR_DIR should be the path to the folder containing qlog copies, as set by the
#quantumfs flag -errorDir
QFS_PID=$1
QLOG_PATH=$2
ERROR_DIR=/var/log/quantumfs
MAX_FILES=3
COPY_NAME=$(date +%Y-%M-%d_%T.%N).qlog

if [ ! -d "$ERROR_DIR" ]; then
	echo "Error: please mkdir $ERROR_DIR"
	exit 1
fi

#copy the qlog
cp $QLOG_PATH $ERROR_DIR/$COPY_NAME

#trim the directory contents
cd $ERROR_DIR
MAX_FILES=$((MAX_FILES+1))
FILES=$(ls -1t | tail -n "+$MAX_FILES")

if [[ FILES != "" ]]; then
	echo "${FILES}" | xargs -d '\n' rm
fi

#send an email
HOSTNAME=$(hostname)
BODY="New QuantumFs errors have occurred.\n\
Please check $ERROR_DIR/$COPY_NAME on $HOSTNAME for new qlogs containing errors.\n"

printf "$BODY" | mail -s "QuantumFs encountered an error on $HOSTNAME." $ADMIN
echo "New errors found, sent email to $ADMIN"
