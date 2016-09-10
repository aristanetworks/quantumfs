COMMANDS=quantumfsd qfs qparse
PKGS_TO_TEST=daemon qlog thirdparty_backends systemlocal

.PHONY: all $(COMMANDS) $(PKGS_TO_TEST)

all: $(COMMANDS) $(PKGS_TO_TEST)

clean:
	rm -f $(COMMANDS)

fetch:
	for cmd in $(COMMANDS); do \
		echo "Fetching $$cmd"; \
		go get github.com/aristanetworks/quantumfs/cmd/$$cmd; \
	done

encoding/metadata.capnp.go: encoding/metadata.capnp
	cd encoding; capnp compile -ogo metadata.capnp

$(COMMANDS): encoding/metadata.capnp.go
	go build github.com/aristanetworks/quantumfs/cmd/$@

$(PKGS_TO_TEST): encoding/metadata.capnp.go
	sudo go test github.com/aristanetworks/quantumfs/$@
