#!/bin/bash
# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

ppid=$1
rootContainer=$ROOTDIRNAME

mountPath=`sudo mount -t fusectl | awk '{print $3}'`
if [ -z "$mountPath" ]; then
        mountPath=/sys/fs/fuse/connections
        sudo mount -t fusectl none $mountPath
fi

# Watch the output of the Makefile. If nothing is output for the timeout period,
# then it Make has hung. No single step should take longer than 2 minutes (some
# very slow systems may take 1 minute for daemon tests)
TIMEOUT_SEC=120
READERR=0
while true; do
	read -t $TIMEOUT_SEC line
	READERR=$?

	if [[ $READERR -ne 0 ]]; then
		break
	fi
done

# If read finished without a timeout error, READERR will be 142. EOF returns 1.
if [[ $READERR -eq 142 ]]; then
	echo "Make TIMED OUT"
	# Force to kill the parent process "make all" because it has hung too long
	for pid in `ps ux | grep --color=never 'make' | awk '{print $2}'`; do
		if [ $ppid -eq $pid ]; then
			echo "Sending SIGKILL to $pid"
			kill -9 $pid
		fi
	done
fi

# Prevent $rootContainer is accidentally set empty
if [[ -z ${rootContainer// } ]]; then
	echo "The temporary directory /dev/shm/$rootContainer is not properly named"
	exit 1
fi

# Clean up the rest left-over of mount point until no mount point left
while [ `mount | grep /dev/shm/$rootContainer | wc -l` -gt 0 ]; do
	# Remove the hanging mount point
	for abort in `grep /dev/shm/$rootContainer /proc/self/mountinfo | \
		sed 's/^.*0:\([0-9]\+\).*$/\1/'`; do
			echo 1 | sudo tee $mountPath/$abort/abort > /dev/null
	done

	# Remove the records in mount
	for abort in `mount | grep /dev/shm/$rootContainer | awk '{print $3}'`; do
		sudo fusermount -u $abort
	done
done

# Clean up the rootContainder in /dev/shm if it exists
if [[ -d "/dev/shm/$rootContainer" ]]; then
	echo "Removing /dev/shm/$rootContainer."
	sudo rm -r /dev/shm/$rootContainer
else
	echo "No /dev/shm folder to cleanup... finished."
fi
