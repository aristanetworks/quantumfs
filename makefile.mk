# Configure which features to build QuantumFS with. If you change these you should
# run 'make fetch' to ensure that all the necessary dependencies are available.
#
# See the files in the features directory for details.
FEATURES=

COMMANDS=quantumfsd qfs qparse emptykeys qupload qwalker qloggerdb wsdbhealthcheck
COMMANDS+=cqlWalkerd cqlWalker
COMMANDS386=qfs-386 qparse-386
COMMANDS_STATIC=quantumfsd-static qupload-static
PKGS_TO_TEST=quantumfs quantumfs/daemon quantumfs/qlog
PKGS_TO_TEST+=quantumfs/backends/systemlocal
PKGS_TO_TEST+=quantumfs/backends/processlocal quantumfs/walker
PKGS_TO_TEST+=quantumfs/backends/cql
PKGS_TO_TEST+=quantumfs/utils/aggregatedatastore
PKGS_TO_TEST+=quantumfs/utils/excludespec quantumfs/backends/grpc
PKGS_TO_TEST+=quantumfs/backends/grpc/server quantumfs/qlogstats
PKGS_TO_TEST+=quantumfs/cmd/qupload quantumfs/cmd/cqlWalkerd
LIBRARIES=libqfs.so libqfs.h libqfs32.so libqfs32.h

# It's common practice to use a 'v' prefix on tags, but the prefix should be
# removed when making the RPM version string.
#
# Use "git describe" as the basic RPM version data.  If there are no tags
# yet, simulate a v0 tag on the initial/empty repo and a "git describe"-like
# tag (eg v0-12-gdeadbee) so there's a legitimate, upgradeable RPM version.
#
# Include "-dirty" on the end if there are any uncommitted changes.
#
# Replace hyphens with underscores; RPM uses them to separate version/release.
version := $(shell git describe --dirty --abbrev=8 --match "v[0-9]*" 2>/dev/null || echo "v0-`git rev-list --count HEAD`-g`git describe --dirty --always`")
RPM_VERSION := $(shell echo "$(version)" | sed -e "s/^v//" -e "s/-/_/g")
RPM_RELEASE := 1

.PHONY: all clean check-dep-installed fetch update vet lockcheck cppstyle $(COMMANDS) $(COMMANDS386) $(PKGS_TO_TEST) $(COMMANDS_STATIC)

all: lockcheck cppstyle vet $(COMMANDS) $(COMMANDS386) $(PKGS_TO_TEST) wsdbservice qfsclient

clean:
	rm -f $(COMMANDS) $(COMMANDS386) $(COMMANDS_STATIC) $(LIBRARIES)

# Vendored dependency management
#
# fetch fetches dependencies based on the recorded versions in Gopkg.lock.
#
# update checks for newer versions of dependencies and resolves version constraints
# if updates are available.
#
# Cityhash contains no go code, so dep currently won't handle it.
# Clone it under the vendor dir for safe keeping (should we begin committing vendor)
# and reset to a known commit.

define fetch-cityhash =
	-rm -rf vendor/cityhash
	git clone https://github.com/google/cityhash vendor/cityhash
	cd vendor/cityhash && git reset 8af9b8c2b889d80c22d6bc26ba0df1afb79a30db   # Wed Jul 31 23:34:41 2013
	rm -rf vendor/cityhash/.git
endef

check-dep-installed:
	dep version &>/dev/null || go get -u github.com/golang/dep/cmd/dep

Gopkg.toml: makefile.mk Gopkg.tomlbase features/*/Gopkg
	cp Gopkg.tomlbase Gopkg.toml
	for feature in `grep ^FEATURES makefile.mk | sed 's/^.*=//'`; do \
		cat features/$$feature/Gopkg >> Gopkg.toml; \
	done

fetch: check-dep-installed Gopkg.toml
	dep ensure -v
	$(fetch-cityhash)

update: check-dep-installed Gopkg.toml
	dep ensure -v --update
	$(fetch-cityhash)
	@echo "Please review and commit any changes to Gopkg.tomlbase and Gopkg.lock"

vet:
	go vet `find . -path ./vendor -prune -o -path ./.git -prune -o -path ./utils/dangerous -prune -o -path ./qfsclientc -prune -o -path ./QFSClient -prune -o -path ./QubitCluster -prune -o -path ./configs -prune -o -path ./_scripts -prune -o -path ./features -prune -o -path ./cmd -true -o -type d -print | grep -v "cql/scripts" | grep -v cql/cluster | grep -v cqlWalker`

lockcheck:
	./lockcheck.sh

cppstyle:
	./cpplint.py QFSClient/*.cc QFSClient/*.h

encoding/metadata.capnp.go: encoding/metadata.capnp
	@if which capnp &>/dev/null; then \
		cd encoding; capnp compile -ogo metadata.capnp; \
	else \
		echo "Error: capnp not found. If you didn't modify encoding/metadata.capnp try 'touch encoding/metadata.capnp.go' to fix the build."; \
		exit 1; \
	fi

backends/grpc/rpc/rpc.pb.go: backends/grpc/rpc/rpc.proto
	protoc -I backends/grpc/rpc/ backends/grpc/rpc/rpc.proto --go_out=plugins=grpc:backends/grpc/rpc

libqfs32.so:
	CGO_ENABLED=1 GOARCH=386 go build -tags "$(FEATURES)" -buildmode=c-shared -o libqfs32.so libqfs/wrapper/libqfs.go

libqfs.so: libqfs/wrapper/libqfs.go
	CGO_ENABLED=1 go build -tags "$(FEATURES)" -buildmode=c-shared -o libqfs.so libqfs/wrapper/libqfs.go

$(COMMANDS): encoding/metadata.capnp.go
	go build -tags "$(FEATURES)" -gcflags '-e' -ldflags "-X main.version=$(version)" github.com/aristanetworks/quantumfs/cmd/$@
	mkdir -p $(GOPATH)/bin
	cp -r $(GOPATH)/src/github.com/aristanetworks/quantumfs/$@ $(GOPATH)/bin/$@
	sudo -E go test github.com/aristanetworks/quantumfs/cmd/$@

$(COMMANDS_STATIC): encoding/metadata.capnp.go
	go build -tags "$(FEATURES)" -gcflags '-e' -o $@ -ldflags "-X main.version=$(version) -extldflags -static" github.com/aristanetworks/quantumfs/cmd/$(subst -static,,$@)


$(COMMANDS386): encoding/metadata.capnp.go
	GOARCH=386 go build -tags "$(FEATURES)" -gcflags '-e' -o $@ -ldflags "-X main.version=$(version)" github.com/aristanetworks/quantumfs/cmd/$(subst -386,,$@)

wsdbservice:
	go build -tags "$(FEATURES)" -gcflags '-e' -o cmd/wsdbservice/wsdbservice -ldflags "-X main.version=$(version) -extldflags -static" github.com/aristanetworks/quantumfs/cmd/wsdbservice

dockerWsdb: wsdbservice
	cd cmd/wsdbservice; docker build -t registry.docker.sjc.aristanetworks.com:5000/qubit-tools/wsdbservice:$(version) .

uploadDocker: dockerWsdb
	cd cmd/wsdbservice; docker push registry.docker.sjc.aristanetworks.com:5000/qubit-tools/wsdbservice:$(version)

# Disable the golang test cache with '-count 1' because not all of these tests are
# entirely deterministic and we want to get test coverage of timing differences.
$(PKGS_TO_TEST): encoding/metadata.capnp.go backends/grpc/rpc/rpc.pb.go
	sudo -E go test -tags "$(FEATURES)" $(QFS_GO_TEST_ARGS) -gcflags '-e' -count 1 github.com/aristanetworks/$@

rpm-ver:
	@echo "version='$(version)'"
	@echo "RPM version='$(RPM_VERSION)'"
	@echo "RPM release='$(RPM_RELEASE)'"

check-fpm:
	fpm --help &> /dev/null || \
	(echo "Installing fpm" && \
		sudo yum install -y gcc libffi-devel ruby-devel rubygems && \
		sudo gem install --no-ri --no-rdoc fpm \
	)

define RPM_COMMON_CONTACT
-m 'quantumfs-dev@arista.com' \
--license='Arista Proprietary' \
--vendor='Arista Networks' \
--url http://gut/repos/quantumfs
endef

RPM_COMMON_VERSION := --version $(RPM_VERSION) --iteration $(RPM_RELEASE)

FPM := fpm -f -s dir -t rpm $(RPM_COMMON_CONTACT) $(RPM_COMMON_VERSION)

RPM_BASENAME_QUPLOAD := QuantumFS-qupload
RPM_FILE_QUPLOAD := $(RPM_BASENAME_QUPLOAD)-$(RPM_VERSION)-$(RPM_RELEASE)
quploadRPM: check-fpm $(COMMANDS)
	$(FPM) -n $(RPM_BASENAME_QUPLOAD) \
		--description='A tool to upload directory hierarchy into datastore' \
		--no-depends \
		./qupload=/usr/bin/qupload

quantumfsRPM: check-fpm $(COMMANDS)
	$(FPM) -n QuantumFS \
		--description='A distributed filesystem optimized for large scale software development' \
		--depends libstdc++ \
		--depends fuse \
		--depends QuantumFS-tool \
		--after-install systemd_reload \
		--after-remove systemd_reload \
		--after-upgrade systemd_reload \
		./quantumfsd=/usr/sbin/quantumfsd \
		./qloggerdb=/usr/sbin/qloggerdb \
		./qloggerdb_system_unit=/usr/lib/systemd/system/qloggerdb.service \
		./systemd_unit=/usr/lib/systemd/system/quantumfs.service

qfsRPM: check-fpm $(COMMANDS)
	$(FPM) -n QuantumFS-tool \
		--description='A distributed filesystem optimized for large scale software development' \
		--no-depends \
		./qfs=/usr/bin/qfs \
		./qparse=/usr/sbin/qparse

qfsRPMi686: check-fpm $(COMMANDS386)
	$(FPM) -n QuantumFS-tool \
		-a i686 \
		--description='A distributed filesystem optimized for large scale software development' \
		--no-depends \
		./qfs-386=/usr/bin/qfs \
		./qparse-386=/usr/sbin/qparse

healthCheckRpm: check-fpm $(COMMANDS)
	$(FPM) -n QuantumFS-wsdbhealthcheck \
		--description='Utility to confirm healthy operation of the wsdbservice' \
		--no-depends \
		./wsdbhealthcheck=/usr/bin/wsdbhealthcheck

# Default to x86_64 location; we'll override when building via mock
RPM_LIBDIR ?= /usr/lib64

RPM_BASENAME_CLIENT := QuantumFS-client
RPM_BASENAME_CLIENT_DEVEL := QuantumFS-client-devel
RPM_FILE_PREFIX_CLIENT := $(RPM_BASENAME_CLIENT)-$(RPM_VERSION)-$(RPM_RELEASE)
RPM_FILE_PREFIX_CLIENT_DEVEL := $(RPM_BASENAME_CLIENT_DEVEL)-$(RPM_VERSION)-$(RPM_RELEASE)

RPM_FILES_TOOLSV2_I686 += $(RPM_FILE_PREFIX_CLIENT).i686.rpm $(RPM_FILE_PREFIX_CLIENT_DEVEL).i686.rpm
RPM_FILES_TOOLSV2_X86_64 += $(RPM_FILE_PREFIX_CLIENT).x86_64.rpm $(RPM_FILE_PREFIX_CLIENT_DEVEL).x86_64.rpm
RPM_FILES_TOOLSV2_X86_64 += $(RPM_FILE_QUPLOAD).x86_64.rpm

clientRPM: check-fpm qfsclient
	$(FPM) -n $(RPM_BASENAME_CLIENT) \
		--description='QuantumFS client API' \
		--depends jansson \
		--depends openssl \
		--depends libstdc++ \
		QFSClient/libqfsclient.so=$(RPM_LIBDIR)/libqfsclient.so \
		libqfs.so=$(RPM_LIBDIR)/libqfs.so
	$(FPM) -n $(RPM_BASENAME_CLIENT_DEVEL) \
		--description='Development files for QuantumFS client API' \
		--depends $(RPM_BASENAME_CLIENT) \
		QFSClient/qfs_client.h=/usr/include/qfs_client.h \
		libqfs.h=/usr/include/libqfs.h

clientRPM32: check-fpm libqfs32.so
	@echo "Building i686 RPMs using mock. This can take several minutes"
	{ \
		set -e ; \
		MOCKLOCK=/tmp/fedora-18-i386.lock ; \
		trap 'rm -f $$MOCKLOCK' EXIT ; \
		(flock 9 || exit 1 ; \
			mock -r fedora-18-i386 --init ; \
			mock -r fedora-18-i386 --install sudo procps-ng git gtest-devel jansson-devel openssl-devel ruby-devel rubygems ; \
			mock -r fedora-18-i386 --shell "sudo gem install --no-ri --no-rdoc fpm" ; \
			mock -r fedora-18-i386 --copyin . /quantumfs ; \
			mock -r fedora-18-i386 --shell "cd /quantumfs && make clean" ; \
			mock -r fedora-18-i386 --copyin ./libqfs32.so /quantumfs/libqfs.so ; \
			mock -r fedora-18-i386 --copyin ./libqfs32.h /quantumfs/libqfs.h ; \
			mock -r fedora-18-i386 --shell "export PATH=$$PATH:/usr/local/bin && cd /quantumfs && make clientRPM RPM_LIBDIR=/usr/lib" ; \
			mock -r fedora-18-i386 --copyout /quantumfs/$(RPM_FILE_PREFIX_CLIENT).i686.rpm . ; \
			mock -r fedora-18-i386 --copyout /quantumfs/$(RPM_FILE_PREFIX_CLIENT_DEVEL).i686.rpm . ; \
			mock -r fedora-18-i386 --clean ; \
		) 9>$$MOCKLOCK ; \
	}

rpm: $(COMMANDS) quantumfsRPM qfsRPM qfsRPMi686 quploadRPM clientRPM clientRPM32 healthCheckRpm

push-rpms: $(RPM_FILES_TOOLSV2_I686) $(RPM_FILES_TOOLSV2_X86_64)
	a4 scp $(RPM_FILES_TOOLSV2_I686) dist:/dist/release/ToolsV2/repo/i386/RPMS
	a4 ssh dist /usr/bin/createrepo --update /dist/release/ToolsV2/repo/i386/RPMS
	a4 scp $(RPM_FILES_TOOLSV2_X86_64) dist:/dist/release/ToolsV2/repo/x86_64/RPMS
	a4 ssh dist /usr/bin/createrepo --update /dist/release/ToolsV2/repo/x86_64/RPMS
	@echo
	@echo "If you're refreshing existing RPMs, then on machines which use this repo you should:"
	@echo "   sudo yum clean all"
	@echo "   sudo yum makecache"

.PHONY: check-fpm rpm-ver qfsRPM quploadRPM clientRPM clientRPM32 rpm push-rpms

include QFSClient/Makefile
