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
TEST_TARGET := $(d)/qfsclient_test

SRCS      := $(d)/qfs_client_implementation.cc $(d)/qfs_client_util.cc
OBJS      := $(SRCS:.cc=.o)
DEPS      := $(SRCS:.cc=.d)
TEST_SRCS := $(d)/qfs_client_test.cc $(d)/qfs_client_util_test.cc
TEST_OBJS := $(TEST_SRCS:.cc=.o)

CXX_FLAGS      := -xc++ -I. -fPIC -g -Werror -std=c++11
LD_FLAGS       := -shared -Wl,-rpath,.
TEST_LD_FLAGS  := -Wl,-rpath,. -L$(d) -lqfsclient -lgtest -ljansson -lcrypto

all: $(TARGET) test

qfsclient: $(TARGET) test

clean: qfsc-clean

qfsc-clean:
	rm -f $(TARGET) $(OBJS) $(DEPS) $(TEST_TARGET) $(TEST_OBJS)

$(OBJS): $(DEPS)

%.d: %.cc
	$(CXX) -M $(CXX_FLAGS) $< > $@

%.o: %.cc
	$(CXX) $(CXX_FLAGS) -c -o $@ $<

$(TARGET): $(OBJS)
	$(CXX) $(LD_FLAGS) -o $@ $^

test: cleanuplocal $(TEST_TARGET)
	LD_LIBRARY_PATH=$(d) $(TEST_TARGET)

$(TEST_TARGET): $(TEST_OBJS) $(TARGET)
	$(CXX) $(TEST_LD_FLAGS) -o $@ $(TEST_OBJS)

cleanuplocal:
	if [[ "$(ROOTDIRNAME)" == "$(DIRNAME)" ]]; then \
		../cleanup.sh $(ppid) & \
	fi

.PHONY: all test cleanuplocal
