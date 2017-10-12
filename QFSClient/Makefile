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

CXX_FLAGS      := -xc++ -I.. -I. -fPIC -g -Werror -std=c++11
LD_FLAGS       := -shared -Wl,-rpath,.
TEST_LD_FLAGS  := -Wl,-rpath,. -L$(d) -lqfsclient -lgtest -ljansson -lcrypto

all: $(TARGET) test

qfsclient: $(TARGET)

clean: qfsc-clean

qfsc-clean:
	rm -f $(TARGET) $(OBJS) $(TEST_TARGET) $(TEST_OBJS) $(d)/*.rpm

%.o: %.cc
	$(CXX) $(CXX_FLAGS) -c -o $@ $<

$(OBJS): $(HDRS)

$(TEST_OBJS): $(TEST_HDRS)

$(TARGET): $(OBJS)
	$(CXX) $(LD_FLAGS) -o $@ $^

gotests: qfsclient
	sudo LD_LIBRARY_PATH="$(d)" CGO_LDFLAGS="-L$(d)" CGO_CFLAGS="-I$(d)" \
		go test github.com/aristanetworks/quantumfs/qfsclientc

test: cleanuplocal $(TEST_TARGET) gotests
	LD_LIBRARY_PATH=$(d) $(TEST_TARGET)

$(TEST_TARGET): $(TEST_OBJS) $(TARGET)
	$(CXX) $(TEST_LD_FLAGS) -o $@ $(TEST_OBJS)

cleanuplocal:
	if [[ "$(ROOTDIRNAME)" == "$(DIRNAME)" ]]; then \
		../cleanup.sh $(ppid) & \
	fi

.PHONY: all test cleanuplocal gotests

#-----------------------------------------------------------------------
# RPM generation

# It's common practice to tag with a 'v' prefix (I think it is to be able
# to distinguish hashes from tags).  However, it has to be removed from
# the RPM version.
#
# We can only use numeric version info for RPM.  We'll use the
# long format from 'git describe', and split it into two parts.
#
# So, git tags must start with 'v' followed by one or more dot-separated
# decimal numbers, optionally followed by a hyphen and any release suffix,
# for example eg 1.2 or 1.3-alpha
#
# We'll split them into:
# - the leading X.Y.Z which we'll use as the RPM version
# - a separating hyphen
# - the trailing [optional-]N-gXXXXXXX which we'll use as the RPM release,
#   after translating any non-alnum to '.'
#
# If there is a tag at the most recent commit, then the trailing
# piece will be [optional-]0-gXXXXXXX, but that's OK.
# Regardless, we'll have the abbreviated commit hash in the
# release field for future reference.

GIT_RAW_VERSION := $(shell git describe --tags --always --match "v[0-9.]*" --long HEAD | sed "s/^v//")
RPM_RAW_VERSION := $(shell echo -n "$(GIT_RAW_VERSION)" | sed "s/\([0-9.]\+\)-\(.\+\)/\1/")
RPM_RAW_RELEASE := $(shell echo -n "$(GIT_RAW_VERSION)" | sed "s/\([0-9.]\+\)-\(.\+\)/\2/")
ifneq '$(GIT_RAW_VERSION)' '$(RPM_RAW_VERSION)-$(RPM_RAW_RELEASE)'
$(error Is git tag '$(GIT_RAW_VERSION)' correctly formatted? Got version='$(RPM_RAW_VERSION)', release='$(RPM_RAW_RELEASE)')
endif

RPM_VERSION := $(shell echo -n "$(GIT_RAW_VERSION)" | sed "s/\([0-9.]\+\)-\(.\+\)/\1/")
RPM_RELEASE := $(shell echo -n "$(RPM_RAW_RELEASE)" | tr -c [:alnum:] .)

# Default to x86_64 location; we'll override when building via rpm32 target
# (using (uname -m) isn't reliable when using mock)
RPM_LIBDIR ?= /usr/lib64

RPM_BASENAME_CLIENT := QuantumFS-client
RPM_BASENAME_CLIENT_DEVEL := QuantumFS-client-devel

RPM_FILE_PREFIX_CLIENT := $(RPM_BASENAME_CLIENT)-$(RPM_VERSION)-$(RPM_RELEASE)
RPM_FILE_PREFIX_CLIENT_DEVEL := $(RPM_BASENAME_CLIENT_DEVEL)-$(RPM_VERSION)-$(RPM_RELEASE)

rpm-ver:
	@echo "git raw tag='$(GIT_RAW_VERSION)'"
	@echo "RPM version='$(RPM_VERSION)'"
	@echo "RPM release='$(RPM_RELEASE)'"

# check-fpm is for the benefit of the outer x64 environment.
# The mock env already has it installed
check-fpm:
	fpm --help &> /dev/null || \
	(echo "Installing fpm" && \
		sudo yum install -y gcc libffi-devel ruby-devel rubygems && \
		sudo gem install  --no-ri --no-rdoc fpm \
	)

clientRPM: check-fpm $(TARGET) $(d)/qfs_client.h
	fpm --force -s dir -t rpm -n $(RPM_BASENAME_CLIENT) \
		--maintainer 'quantumfs-dev@arista.com' \
		--license='Arista Proprietary' \
		--vendor='Arista Networks' \
		--url http://gut/repos/quantumfs \
		--description='QuantumFS client API' \
		--depends jansson \
		--depends openssl \
		--depends libstdc++ \
		--version $(RPM_VERSION) \
		--iteration $(RPM_RELEASE) \
		$(TARGET)=$(RPM_LIBDIR)/libqfsclient.so
	fpm --force -s dir -t rpm -n $(RPM_BASENAME_CLIENT_DEVEL) \
		--maintainer 'quantumfs-dev@arista.com' \
		--license='Arista Proprietary' \
		--vendor='Arista Networks' \
		--url http://gut/repos/quantumfs \
		--description='Development files for QuantumFS client API' \
		--depends $(RPM_BASENAME_CLIENT) \
		--version $(RPM_VERSION) \
		--iteration $(RPM_RELEASE) \
		$(d)/qfs_client.h=/usr/include/qfs_client.h

clientRPM32:
	@echo "Building i686 RPMs using mock. This can take several minutes"
	{ \
		set -e ; \
		MOCKLOCK=/tmp/fedora-18-i386.lock ; \
		trap 'rm -f $$MOCKLOCK' EXIT ; \
		(flock 9 || exit 1 ; \
			mock -r fedora-18-i386 --init ; \
			mock -r fedora-18-i386 --install sudo procps-ng git gtest-devel jansson-devel openssl-devel libffi-devel ruby-devel rubygems ; \
			mock -r fedora-18-i386 --shell "sudo gem install --no-ri --no-rdoc fpm" ; \
			mock -r fedora-18-i386 --copyin $(d)/../ /quantumfs ; \
			mock -r fedora-18-i386 --shell "export PATH=$$PATH:/usr/local/bin && cd /quantumfs/QFSClient && make clean clientRPM RPM_LIBDIR=/usr/lib" ; \
			mock -r fedora-18-i386 --copyout /quantumfs/QFSClient/$(RPM_FILE_PREFIX_CLIENT).i686.rpm . ; \
			mock -r fedora-18-i386 --copyout /quantumfs/QFSClient/$(RPM_FILE_PREFIX_CLIENT_DEVEL).i686.rpm . ; \
			mock -r fedora-18-i386 --clean ; \
		) 9>$$MOCKLOCK ; \
	}

rpm: clientRPM clientRPM32

RPM_FILES_I686 := $(RPM_FILE_PREFIX_CLIENT).i686.rpm $(RPM_FILE_PREFIX_CLIENT_DEVEL).i686.rpm
RPM_FILES_X86_64 := $(RPM_FILE_PREFIX_CLIENT).x86_64.rpm $(RPM_FILE_PREFIX_CLIENT_DEVEL).x86_64.rpm

push-rpms: $(RPM_FILES_I686) $(RPM_FILES_X86_64)
	a4 scp $(RPM_FILES_I686) dist:/dist/release/ToolsV2/repo/i386/RPMS
	a4 ssh dist /usr/bin/createrepo --update /dist/release/ToolsV2/repo/i386/RPMS
	a4 scp $(RPM_FILES_X86_64) dist:/dist/release/ToolsV2/repo/x86_64/RPMS
	a4 ssh dist /usr/bin/createrepo --update /dist/release/ToolsV2/repo/x86_64/RPMS
	@echo
	@echo "If you're refreshing existing RPMs, then on machines which use this repo you should:"
	@echo "   sudo yum clean all"
	@echo "   sudo yum makecache"

.PHONY: check-fpm rpm-ver clientRPM clientRPM32 rpm rpm32 push-rpms
