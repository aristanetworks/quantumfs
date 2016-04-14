COMMANDS=quantumfsd qfs contentcheck
PKGS_TO_TEST=daemon

.PHONY: all $(COMMANDS) $(PKGS_TO_TEST)
.NOTPARALLEL:

all: $(COMMANDS) $(PKGS_TO_TEST)

clean:
	rm -f $(COMMANDS)

$(COMMANDS):
	go get arista.com/quantumfs/cmd/$@
	go build arista.com/quantumfs/cmd/$@

$(PKGS_TO_TEST):
	go test arista.com/quantumfs/$@ 2>&1
