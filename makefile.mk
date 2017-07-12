COMMANDS=quantumfsd qfs qparse emptykeys qupload qwalker qloggerdb
PKGS_TO_TEST=quantumfs quantumfs/daemon quantumfs/qlog
PKGS_TO_TEST+=quantumfs/thirdparty_backends quantumfs/systemlocal
PKGS_TO_TEST+=quantumfs/processlocal quantumfs/walker
PKGS_TO_TEST+=quantumfs/utils/aggregatedatastore
PKGS_TO_TEST+=quantumfs/utils/excludespec

version:=$(shell git describe || echo "dev-`git rev-parse HEAD`")

.PHONY: all $(COMMANDS) $(PKGS_TO_TEST)

all: lockcheck cppstyle $(COMMANDS) $(PKGS_TO_TEST)

clean:
	rm -f $(COMMANDS)

fetch:
	for cmd in $(COMMANDS); do \
		echo "Fetching $$cmd"; \
		go get github.com/aristanetworks/quantumfs/cmd/$$cmd; \
	done

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

$(COMMANDS): encoding/metadata.capnp.go
	go build -gcflags '-e' -ldflags "-X main.version=$(version)" github.com/aristanetworks/quantumfs/cmd/$@
	mkdir -p $(GOPATH)/bin
	cp -r $(GOPATH)/src/github.com/aristanetworks/quantumfs/$@ $(GOPATH)/bin/$@
	sudo -E go test github.com/aristanetworks/quantumfs/cmd/$@

$(PKGS_TO_TEST): encoding/metadata.capnp.go
	sudo -E go test -gcflags '-e' github.com/aristanetworks/$@

quploadRPM: $(COMMANDS)
	fpm -f -s dir -t rpm -m 'quantumfs-dev@arista.com' -n QuantumFS-upload --no-depends \
		--license='Arista Proprietary' \
		--vendor='Arista Networks' \
		--url http://gut/repos/quantumfs \
		--description='A tool to upload directory hierarchy into datastore' \
		--version $(version) \
		./qupload=/usr/bin/qupload

qfsRPM: $(COMMANDS)
	fpm -f -s dir -t rpm -m 'quantumfs-dev@arista.com' -n QuantumFS --no-depends \
		--license='Arista Proprietary' \
		--vendor='Arista Networks' \
		--url http://gut/repos/quantumfs \
		--description='A distributed filesystem optimized for large scale software development' \
		--depends libstdc++ \
		--depends fuse \
		--after-install systemd_reload \
		--after-remove systemd_reload \
		--after-upgrade systemd_reload \
		--version $(version) \
		./quantumfsd=/usr/sbin/quantumfsd \
		./qfs=/usr/bin/qfs \
		./qparse=/usr/sbin/qparse \
		./systemd_unit=/usr/lib/systemd/system/quantumfs.service

rpm: $(COMMANDS) qfsRPM quploadRPM

include QFSClient/Makefile