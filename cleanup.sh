#!/bin/bash

mkdir History

# The go-test should be no longer than 2 min; otherwise, it is hanging
sleep 120

currPath=`pwd`
mountPath=/sys/fs/fuse/connections

systemRecord=$currPath/History/systemRecord
chrootRecord=$currPath/History/chrootRecord

devRecord=$currPath/History/devRecord
tmpRecord=$currPath/History/tmpRecord

# Force to kill the "make all" process because it has hung too long
for pid in `ps ux | grep --color=never 'make all' | awk '{print $2}'`
do if [ `ps -p $pid|wc -l` -eq 2 ]
	then kill -9 $pid
	fi
done

# Clean up the left-over qfs-client-test in /tmp
for dir in `ls -l /tmp | grep $USER | grep 'qfs-client-test' | awk '{print $9}'`
do sudo rm -rf /tmp/$dir
done

# Clean up the left-over systemlocalTest in /tmp
while IFS= read line
do
	if ls $line* 1> /dev/null 2>&1
	then sudo rm -rf $line*
	fi
done <$systemRecord

# Clean up the left-over TestChroot in /tmp
while IFS= read line
do
	if ls $line* 1> /dev/null 2>&1
	then sudo rm -rf $line*
	fi
done <$chrootRecord

# Clean up the left-over quantumfsTest

# Clean up the left-over temporary workspace in /dev/shm
while IFS= read line
do sudo rm -rf $line
done <$devRecord

# Clean up the rest left-over of quantumfsTest
while IFS= read line
do
	# Remove the hanging mount point
	for i in `grep $line /proc/self/mountinfo | sed 's/^.*0:\([0-9]\+\).*$/\1/'`
	do echo 1 | sudo tee $mountPath/$i/abort > /dev/null
	done

	# Remove the records in mount
	for i in `mount|grep $line|awk '{print $3}'`
	do sudo fusermount -u $i
	done

	# Remove the directory in /tmp
	if ls $line* 1> /dev/null 2>&1
	then sudo rm -rf $line*
	fi
done <$tmpRecord

# Remove the directory of cleanupHistory
rm -rf History
