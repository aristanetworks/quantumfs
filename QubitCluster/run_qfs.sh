#!/bin/bash
# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

QFSCONFIG=/etc/quantumfsd.conf

# The error output isn't very clear for the following cases so it's best to
# check for them ourselves.
if [ ! -f /.dockerenv ]; then
  echo "ERROR: This file should only be run in a docker environment"
  exit 1
elif ! ip link add dummy0 type dummy; then
  echo "ERROR: Container must be run in privileged mode (--privileged)"
  exit 1
elif [ ! -f $QFSCONFIG ]; then
  echo "ERROR: A config file must be mounted at $QFSCONFIG"
  exit 1
elif [ $(df -k --output=avail /dev/shm | tail -n 1) -lt 8388608 ]; then
  echo "ERROR: Minimum of 8GB shared memory required (--shm-size=8g)"
  exit 1
fi

# Start the QuantumFS daemon in the background.
quantumfsd \
  -datastore ether.cql -datastoreconf $QFSCONFIG \
  -workspaceDB ether.cql -workspaceDBconf $QFSCONFIG &

function cleanup() {
  # Unmount QFS and stop the daemon.
  fusermount -u /qfs
}
trap cleanup EXIT

# Execute whatever is passed as an argument.
# Note this must remain at the end if the command's error code is to become the
# script's error code.
eval $@
