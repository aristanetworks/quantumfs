COMMANDS=quantumfsd qfs qparse
PKGS_TO_TEST=daemon qlog thirdparty_backends systemlocal processlocal walker

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
	cd encoding; capnp compile -ogo metadata.capnp

$(COMMANDS): encoding/metadata.capnp.go
	go build -gcflags '-e' github.com/aristanetworks/quantumfs/cmd/$@
	mkdir -p $(GOPATH)/bin
	cp -r $(GOPATH)/src/github.com/aristanetworks/quantumfs/$@ $(GOPATH)/bin/$@
	sudo go test github.com/aristanetworks/quantumfs/cmd/$@

$(PKGS_TO_TEST): encoding/metadata.capnp.go
	sudo go test -gcflags '-e' github.com/aristanetworks/quantumfs/$@

include QFSClient/Makefile
