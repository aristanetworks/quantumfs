# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

FROM fedora:latest

LABEL maintainer="shayan@arista.com"

# NOTE:
#    This will build a Docker image based on HEAD committed to http://gut/repos/quantumfs
#    The C/C++ dependencies will be built/installed based on the vendor versions
#    from that HEAD commit.
#
#    The image can be run using the HEAD version, or you can also volume mount your local
#    work repository and build/test based on the Go code it contains.
#
# To test or use this docker image:
#
# 1) Go to the root of the quantumfs repo and:
#    $ docker build --no-cache -t qfs .
#
# 2) To launch the container with the default HEAD from quantumfs, run:
#    $ docker run --shm-size 4294967296 --privileged -it --rm qfs /bin/bash
#
#    Alternatively, volume mount your local quantumfs repository instead using:
#    $ docker run -v $GOPATH/src/github.com/aristanetworks/quantumfs:/root/go/src/github.com/aristanetworks/quantumfs \
#        --shm-size 4294967296 --privileged -it --rm qfs /bin/bash
#
# 3) Finally inside the container try a compile/test:
#    # cd /root/go/src/github.com/aristanetworks/quantumfs && make quantumfs/daemon
#
# (The tests usually fail for various reasons).

ENV USER root
ENV GOPATH /root/go
ENV PATH="$GOPATH/bin:${PATH}"

RUN dnf install -y go \
       git \
       gcc-c++ \
       automake \
       findutils \
       procps \
       python \
       sudo \
       gtest-devel \
       jansson-devel \
       openssl-devel \
       capnproto \
       fuse \
       which \
       protobuf-devel \
       rsync \
       file

ENV ANET_PATH $GOPATH/src/github.com/aristanetworks

# Work-around required so dep can fetch from gerrit
RUN git config --global url."https://gerrit.corp.arista.io/".insteadOf "https://gerrit.corp.arista.io/a/"

RUN mkdir -p $ANET_PATH && \
    cd $ANET_PATH && \
    git clone http://gut/repos/quantumfs && \
    cd quantumfs && \
    make fetch

ENV VENDOR_PATH $ANET_PATH/quantumfs/vendor

RUN cd $VENDOR_PATH/cityhash && \
    ./configure --enable-sse4.2 && \
    make all check CXXFLAGS="-g -O3 -msse4.2" && \
    make install

RUN cd $VENDOR_PATH/github.com/glycerine/go-capnproto && \
   make

