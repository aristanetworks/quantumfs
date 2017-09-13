#!/bin/bash
# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

# This script builds quantumfs, starts quantumfsd daemon inside a
# container and does a newtree of a eos-trunc clone and then
# runs a4 configure and a4 make on the EosKernel package
set -ex

# qfshostdir must be a directory on a 'shared' mountpoint
qfshostdir=/share/cqfs
workspaceName=x/y/z
workdir=wsr

docker build -t qfs .
sudo mkdir -p $qfshostdir
make

cid=$(docker run -d --shm-size 8589934592 --privileged \
   -v $GOPATH/bin/:/root/go/bin:rw \
   -v ${qfshostdir}:/qfs:shared \
   qfs /root/go/bin/quantumfsd)

docker exec $cid qfs branch _/_/_ $workspaceName
docker exec $cid qfs enableRootWrite $workspaceName

if [ ! -f "${qfshostdir}/api" ]
then
   echo "did not find api file on the host"
   exit 1
fi

pushd .

cd $qfshostdir/$workspaceName
a4 newtree $workdir eos-trunk
cd $workdir

function testPkg() {
   a4 chroot a4 configure $1
   a4 chroot a4 make -p $1
}

testPkg EosKernel

popd

sudo umount $qfshostdir
sudo rmdir $qfshostdir
docker stop $container
