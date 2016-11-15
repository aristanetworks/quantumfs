COMMANDS_WITHOUT_TEST=quantumfsd qparse
COMMANDS_WITH_TEST=qfs
PKGS_TO_TEST=daemon qlog thirdparty_backends systemlocal processlocal

.PHONY: all $(COMMANDS_WITHOUT_TEST) $(PKGS_TO_TEST) $(COMMANDS_WITH_TEST)

all: lockcheck $(COMMANDS_WITHOUT_TEST) $(PKGS_TO_TEST) $(COMMANDS_WITH_TEST)

clean:
	rm -f $(COMMANDS)

fetch:
	for cmd in $(COMMANDS); do \
		echo "Fetching $$cmd"; \
		go get github.com/aristanetworks/quantumfs/cmd/$$cmd; \
	done

lockcheck:
	./lockcheck.sh

encoding/metadata.capnp.go: encoding/metadata.capnp
	cd encoding; capnp compile -ogo metadata.capnp

$(COMMANDS_WITHOUT_TEST): encoding/metadata.capnp.go
	go build github.com/aristanetworks/quantumfs/cmd/$@;

$(PKGS_TO_TEST): encoding/metadata.capnp.go
	sudo go test github.com/aristanetworks/quantumfs/$@;

$(COMMANDS_WITH_TEST): encoding/metadata.capnp.go
	go build github.com/aristanetworks/quantumfs/cmd/$@;\
	go install github.com/aristanetworks/quantumfs/cmd/$@;\
	sudo go test github.com/aristanetworks/quantumfs/cmd/$@;
