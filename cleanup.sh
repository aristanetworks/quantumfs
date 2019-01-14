#!/bin/bash

# Copyright (c) 2017 Arista Networks, Inc.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.

pid=$1
rootContainer=$ROOTDIRNAME

mountPath=`sudo mount -t fusectl | awk '{print $3}'`
if [ -z "$mountPath" ]; then
        mountPath=/sys/fs/fuse/connections
        sudo mount -t fusectl none $mountPath
fi

# Watch the output of make. If nothing is printed for the timeout period,
# then it make has hung. No single step should take longer than 3 minutes.
TIMEOUT_SEC=${TIMEOUT_SEC:-180}
READERR=0
while true; do
	read -t $TIMEOUT_SEC line
	READERR=$?

        echo $line

	if [[ $READERR -ne 0 ]]; then
		break
	fi
done

# If read finished with a timeout error, READERR will be 142. EOF returns 1.
if [[ ( $READERR -eq 142 ) && !( -z "$pid" ) ]]; then
	echo "Make TIMED OUT... sending SIGQUIT to $pid"
	kill -QUIT $pid
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
