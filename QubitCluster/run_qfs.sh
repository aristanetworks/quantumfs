#!/bin/bash

QFSCONFIG=/etc/quantumfsd.conf

# The error output isn't very clear for the following cases so it's best to
# check for them ourselves.
if [ ! -f /.dockerenv ]; then
  echo "ERROR: This file should only be run in a docker environment"
  exit
elif [ ! ip link add dummy0 type dummy ]; then
  echo "ERROR: Container must be run in privileged mode (--privileged)"
  exit
elif [ ! -f $QFSCONFIG ]; then
  echo "ERROR: A config file must be mounted at $QFSCONFIG"
  exit
elif [ $(df -k --output=avail /dev/shm | tail -n 1) -lt 4194304 ]; then
  echo "ERROR: Minimum of 4GB shared memory required (--shm-size=4g)"
  exit
fi

# Start the QuantumFS daemon in the background.
quantumfsd \
  -datastore ether.cql -datastoreconf $QFSCONFIG \
  -workspaceDB ether.cql -workspaceDBconf $QFSCONFIG &

# Execute whatever is passed as an argument.
eval $@

# Unmount QFS and stop the daemon.
fusermount -u /qfs
