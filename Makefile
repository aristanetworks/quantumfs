COMMANDS=quantumfsd qfs qparse
PKGS_TO_TEST=daemon qlog

.PHONY: all $(COMMANDS) $(PKGS_TO_TEST)

all: $(COMMANDS) $(PKGS_TO_TEST)

clean:
	rm -f $(COMMANDS)

fetch:
	for cmd in $(COMMANDS); do \
		echo "Fetching $$cmd"; \
		go get github.com/aristanetworks/quantumfs/cmd/$$cmd; \
	done

$(COMMANDS):
	go build github.com/aristanetworks/quantumfs/cmd/$@

$(PKGS_TO_TEST):
	go test github.com/aristanetworks/quantumfs/$@
