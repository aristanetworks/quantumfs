#!/bin/sh

cd /sys/fs/fuse/connections/
sleep 1
for i in `grep --color=never sidquantumfsTest /proc/self/mountinfo | \
	         sed 's/^.*0:\([0-9]\+\).*$/\1/'`;
do
   echo 1 > $i/abort;
done

sleep 1
mount | awk '/siqduantumfs/{print $3}' | xargs -n 1 fusermount -u
