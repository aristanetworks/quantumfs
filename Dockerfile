FROM fedora:rawhide

MAINTAINER shayan@arista.com

ENV GOPATH /go
ENV PATH="$GOPATH/bin:${PATH}"
ENV USER root

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
       which

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
