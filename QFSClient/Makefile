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

SRCS      := $(d)/qfs_client_implementation.cc $(d)/qfs_client_util.cc
OBJS      := $(SRCS:.cc=.o)
DEPS      := $(SRCS:.cc=.d)

CXX_FLAGS      := -xc++ -I. -fPIC -g -Werror -std=c++11
LD_FLAGS       := -shared -Wl,-rpath,.

all: $(TARGET) test

qfsclient: $(TARGET) test

clean: qfsc-clean

qfsc-clean:
	rm -f $(TARGET) $(OBJS) $(DEPS)

$(OBJS): $(DEPS)

%.d: %.cc
	$(CXX) -M $(CXX_FLAGS) $< > $@

%.o: %.cc
	$(CXX) $(CXX_FLAGS) -c -o $@ $<

$(TARGET): $(OBJS)
	$(CXX) $(LD_FLAGS) -o $@ $^

test: cleanuplocal
	sudo LD_LIBRARY_PATH="$(d)" CGO_LDFLAGS="-L$(d)" CGO_CFLAGS="-I$(d)" \
		go test github.com/aristanetworks/quantumfs/qfsclientc

cleanuplocal:
	if [[ "$(ROOTDIRNAME)" == "$(DIRNAME)" ]]; then \
		../cleanup.sh $(ppid) & \
	fi

.PHONY: all test cleanuplocal
