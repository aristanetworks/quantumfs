#!/bin/bash
# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

# This script creates a new QuantumFS release. This includes creating the tag,
# prompting for release notes and building the RPM.
#
# The remaining manual steps include:
# - Testing the release for suitability
# - Pushing the release tag. Use the command: git push origin <tagname>
# - Publishing the resulting RPM

if [ -z "$1" ]; then
        echo "Usage: $0 <version>"
        echo "ie $0 0.2.1"
        exit
fi

version="v$1"

gut switch master
gut sync

git tag -a $version
make clean
make rpm
