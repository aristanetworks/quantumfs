#!/bin/bash

#ADMIN should be the email address of the notifyee
ADMIN=your@email.com
#ERRORDIR should be the path to the folder containing qlog copies, as set by the
#quantumfs flag -errorDir
ERRORDIR=/var/log/quantumfs

if [ ! -d "$ERRORDIR" ]; then
	echo "Error: please mkdir $ERRORDIR"
	exit 1
fi

LS_STR=$(ls -1 $ERRORDIR)
LAST_LS=$(<last_ls)
HOSTNAME=$(hostname)
BODY="New QuantumFs errors have occurred.\n\
Please check $ERRORDIR on $HOSTNAME for new qlogs containing errors.\n\n\
Current contents of QuantumFs qlog error directory:\n$LS_STR\n"

if [[ (-n $LS_STR) && (-n $LAST_LS) ]]; then
	#If one is a substring of the other, then the logs have simply been pruned
	if [[ "$LS_STR" == *"$LAST_LS"* || "$LAST_LS" == *"$LS_STR"* ]]; then
		exit 0
	fi
else
	#We have to check things a little differently if one of the strings is empty
	if [[ "$LS_STR" == "$LAST_LS" ]]; then
		exit 0
	fi
fi

printf "$BODY" | mail -s "QuantumFs encountered an error on $HOSTNAME." $ADMIN
echo "New errors found, sent email to $ADMIN"

printf "$LS_STR" > ./last_ls
