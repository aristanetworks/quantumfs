#!/bin/bash

# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

ppid=$1
rootContainer=/dev/shm/$2
mountPath=/sys/fs/fuse/connections

# The go-test should be no longer than 3 min; otherwise, it is hanging
sleep 180

# Force to kill the parent process "make all" because it has hung too long
for pid in `ps ux | grep --color=never 'make all' | awk '{print $2}'`; do
	if [ $ppid -eq $pid ]; then
		kill -9 $pid
	fi
done

# Clean up the rest left-over of quantumfsTest
for l in `ls -l $rootContainer | grep -i 'quantumfsTest' | awk '{print $9}'`; do
	# Remove the hanging mount point
	for i in `grep $l /proc/self/mountinfo | sed 's/^.*0:\([0-9]\+\).*$/\1/'`; do
		echo 1 | sudo tee $mountPath/$i/abort > /dev/null
	done

	# Remove the records in mount
	for i in `mount | grep $l | awk '{print $3}'`; do
		sudo fusermount -u $i
	done
done

# Clean up the rootContainder in /dev/shm
sudo rm -rf $rootContainer
