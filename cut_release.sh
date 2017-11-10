#!/bin/bash
# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

# This script creates a new QuantumFS release. This includes creating the tag,
# prompting for release notes and building the RPM. You must ensure the current
# branch/working tree is precisely what you want to release.
#
# The remaining manual steps include:
# - Testing the release for suitability
# - Pushing the release tag. Use the command: git push origin <tagname>
# - Publishing the resulting RPM

if [ -z "$2" ]; then
        echo "Usage: $0 <versionName> <commitID>"
        echo "ie $0 0.2.1 f66159e18"
        exit
fi

version="v$1"
commit=$2

gut sync
gut switch $commit

git tag -a $version
make clean
TIMEOUT_SEC=1800 make rpm
