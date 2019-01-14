#!/bin/bash

# Copyright (c) 2017 Arista Networks, Inc.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.

# This script builds quantumfs, starts quantumfsd daemon inside a
# container and does a newtree of a eos-trunk clone and then
# runs a4 configure and a4 make on the EosKernel package
set -ex

# qfshostdir must be a directory on a 'shared' mountpoint
qfshostdir=/share/cqfs
workspaceName=x/y/z
workdir=wsr
docker_run_flags="--shm-size 8589934592 --privileged"

docker build -t qfs .
sudo mkdir -p $qfshostdir

cid=$(docker run -d $docker_run_flags \
   qfs rm /root/go/src/github.com/aristanetworks/quantumfs/cmd/qfs/QfsChroot_test.go)
docker wait $cid
docker commit $cid qfs:rm

cid=$(docker run -d $docker_run_flags \
   -w /root/go/src/github.com/aristanetworks/quantumfs \
   qfs:rm make)
docker wait $cid
# Even when make fails, we are going to proceed to the next step
# This will prevent unittest failures from interfering with running
# the integration test as long as the quantumfsd binary can be built

docker commit $cid qfs:make

cid=$(docker run -d $docker_run_flags \
   -v ${qfshostdir}:/qfs:shared \
   qfs:make /root/go/bin/quantumfsd)

while [ ! -f "${qfshostdir}/api" ]
do
   echo "did not find api file on the host"
   sleep 1
done


docker exec $cid bash -c "cd /qfs && qfs branch _/_/_ $workspaceName"
docker exec $cid bash -c "cd /qfs && qfs enableRootWrite $workspaceName"

pushd .

cd $qfshostdir/$workspaceName
a4 newtree $workdir eos-trunk
cd $workdir

function testPkg() {
   a4 chroot a4 rpmbuild $1
}

testPkg EosKernel

popd

sudo umount $qfshostdir
sudo rmdir $qfshostdir
docker stop $container
