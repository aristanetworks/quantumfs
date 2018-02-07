#!/bin/bash
# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

set -xe

if [ "$1" == "" ]; then
  echo "First parameter must be QFS version (e.g. 0.13.0-1)"
  exit 1
fi

RPM_STORE=http://dist/storage/QuantumFS
TOOL_RPM=QuantumFS-tool-${1}.x86_64.rpm
MAIN_RPM=QuantumFS-${1}.x86_64.rpm

# Install dependencies
yum install -y supervisor fuse wget

# Only some versions of QFS have an accompanying tool RPM.
# If there is a matching version then install it.
if wget -nv $RPM_STORE/$TOOL_RPM; then
  yum --nogpgcheck -y localinstall $TOOL_RPM
  rm $TOOL_RPM
fi

# Install main QFS RPM
wget -nv $RPM_STORE/$MAIN_RPM
yum --nogpgcheck -y localinstall $MAIN_RPM
rm $MAIN_RPM

mkdir /qfs
