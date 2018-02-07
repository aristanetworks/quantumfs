#!/bin/bash
# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

set -xe

QFS_VERSION=$1
# If no version is provided then use the latest tagged release.
if [ -z "$QFS_VERSION" ]; then
  GIT_TAG=$(git describe --abbrev=0 --tags)
  QFS_VERSION=${GIT_TAG#"v"}  # v0.13.0 -> 0.13.0
fi

# Build and tag the image
docker build -t qfs:$QFS_VERSION --build-arg QFS_RPM_VERSION=$QFS_VERSION-1 .

