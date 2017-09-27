FROM fedora:latest

MAINTAINER shayan@arista.com

# To test or use this docker image:
#
# 1) Go to the root of the quantumfs repo and:
#    $ docker build --no-cache -t qfs .
#
# 2) Then launch a container with:
#    $ docker run --shm-size 4294967296 --privileged -v $GOPATH/src/github.com/aristanetworks/quantumfs:/root/go/src/github.com/aristanetworks/quantumfs -it --rm qfs /bin/bash
#
# 3) Finally inside the container make sure it compiles:
#    # cd /root/go/src/github.com/aristanetworks/quantumfs && make quantumfs/daemon
#
# (The tests usually fail for various reasons).

ENV USER root
ENV GOPATH /root/go
ENV PATH="$GOPATH/bin:${PATH}"

RUN dnf install -y make go \
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
       rsync

RUN go get github.com/google/cityhash || ls $GOPATH/src/github.com/google/cityhash

RUN cd $GOPATH/src/github.com/google/cityhash && \
    ./configure --enable-sse4.2 && \
    make all check CXXFLAGS="-g -O3 -msse4.2" && \
    make install

RUN go get -u -t github.com/glycerine/go-capnproto && \
   cd $GOPATH/src/github.com/glycerine/go-capnproto && \
   make

RUN mkdir -p $GOPATH/src/github.com/aristanetworks && \
    cd $GOPATH/src/github.com/aristanetworks && \
    git clone http://gut/repos/quantumfs  && \
    git clone http://gerrit/ether  && \
    git clone http://gut/repos/gut && \
    git clone http://gut/repos/influxlib && \
    cd quantumfs && \
    make fetch
