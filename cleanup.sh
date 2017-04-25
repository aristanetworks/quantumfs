#!/bin/bash
# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

ppid=$1
rootContainer=$ROOTDIRNAME
mountPath=/sys/fs/fuse/connections

# The go-test should be no longer than 3 min; otherwise, it is hanging
sleep 180

# Force to kill the parent process "make all" because it has hung too long
for pid in `ps ux | grep --color=never 'make' | awk '{print $2}'`; do
	if [ $ppid -eq $pid ]; then
		kill -9 $pid
	fi
done

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

# Clean up the rootContainder in /dev/shm
sudo rm -r /dev/shm/$rootContainer
