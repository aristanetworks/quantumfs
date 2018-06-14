# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

# this is the directory that this makefile lives in and is used
# instead of '.' to reference any sources or other files that
# we expect in the same directory as this makefile
d           := $(dir $(realpath $(lastword $(MAKEFILE_LIST))))
ppid        := $(shell ps -o ppid= $$$$)
DIRNAME     := $(shell if [ -z $(ROOTDIRNAME) ]; then echo \
            "$(USER)-RootContainer-$(ppid)" | tr -d '[:space:]'; fi)
ifndef ROOTDIRNAME
        ROOTDIRNAME := $(DIRNAME)
        export ROOTDIRNAME=$(DIRNAME)
endif

TARGET      := $(d)/libqfsclient.so
TEST_TARGET := $(d)/qfs_client_test

SRCS      := $(d)/qfs_client_implementation.cc $(d)/qfs_client_util.cc
OBJS      := $(SRCS:.cc=.o)
HDRS      := $(d)/qfs_client_implementation.h $(d)/qfs_client_util.h $(d)/qfs_client_data.h $(d)/qfs_client.h
TEST_SRCS := $(d)/qfs_client_test.cc $(d)/qfs_client_util_test.cc
TEST_OBJS := $(TEST_SRCS:.cc=.o)
TEST_HDRS := $(d)/qfs_client_test.h

CXX_FLAGS      := -xc++ -I.. -I. -I$(d) -fPIC -g -Werror -std=c++11
LD_FLAGS       := -L$(d)/.. -shared -Wl,-rpath,.
TEST_LD_FLAGS  := -Wl,-rpath,. -L$(d) -L$(d)/.. -lqfsclient -lgtest -ljansson -lcrypto
LIBS           := -Wl,-Bdynamic -lqfs

all: test

qfsclient: $(TARGET)

qfsclienttest: $(TEST_TARGET)

clean: qfsc-clean

qfsc-clean:
	rm -f $(TARGET) $(OBJS) $(TEST_TARGET) $(TEST_OBJS)

%.o: %.cc
	$(CXX) $(CXX_FLAGS) -c -o $@ $<

$(OBJS): $(HDRS) libqfs.so

$(TEST_OBJS): $(TEST_HDRS) libqfs.so

$(TARGET): $(OBJS)
	$(CXX) $(LD_FLAGS) -o $@ $^ $(LIBS)

gotests: qfsclient
	sudo LD_LIBRARY_PATH="$(d):$(d)/.." CGO_LDFLAGS="-L$(d) -L$(d)/.." CGO_CFLAGS="-I$(d)" \
		go test github.com/aristanetworks/quantumfs/qfsclientc

# Use the ramfs root container as the temp dir for the client tests when
# run from this Makefile.
# If the tests are run in some other way (eg on macOS for stand-alone
# client library testing), the tests will use the system tempdir instead.
test: cleanuplocal $(TEST_TARGET) gotests
	mkdir -p /dev/shm/$(ROOTDIRNAME)
	LD_LIBRARY_PATH=$(d) TMPDIR=/dev/shm/$(ROOTDIRNAME) $(TEST_TARGET)

$(TEST_TARGET): $(TEST_OBJS) $(TARGET)
	$(CXX) $(TEST_LD_FLAGS) -o $@ $(TEST_OBJS)

cleanuplocal:
	if [[ "$(ROOTDIRNAME)" == "$(DIRNAME)" ]]; then \
		../cleanup.sh $(ppid) & \
	fi

.PHONY: all test cleanuplocal gotests
