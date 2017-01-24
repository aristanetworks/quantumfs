# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

TARGET      := libqfsclient.so
TEST_TARGET := qfsclient_test

SRCS      := qfs_client.cc qfs_client_util.cc
OBJS      := $(SRCS:.cc=.o)
DEPS      := $(SRCS:.cc=.d)
TEST_SRCS := qfs_client_test.cc qfs_client_util_test.cc
TEST_OBJS := $(TEST_SRCS:.cc=.o)

CXX_FLAGS      := -xc++ -I. -fPIC -g -Werror -std=c++11
LD_FLAGS       := -shared -Wl,-rpath,.
TEST_LD_FLAGS  := -Wl,-rpath,. -L. -lqfsclient -lgtest

all: $(TARGET) test

clean:
	rm -f $(TARGET) $(OBJS) $(DEPS) $(TEST_TARGET) $(TEST_OBJS)

$(OBJS): $(DEPS)

%.d: %.cc
	$(CXX) -M $(CXX_FLAGS) $< > $@

%.o: %.cc
	$(CXX) $(CXX_FLAGS) -c -o $@ $<

$(TARGET): $(OBJS)
	$(CXX) $(LD_FLAGS) -o $@ $^

test: $(TEST_TARGET)
	./$(TEST_TARGET)

$(TEST_TARGET): $(TEST_OBJS) $(TARGET)
	$(CXX) $(TEST_LD_FLAGS) -o $@ $(TEST_OBJS)

.PHONY: all test clean
