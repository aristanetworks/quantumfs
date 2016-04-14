COMMANDS=quantumfsd qfs contentcheck
PKGS_TO_TEST=daemon

.PHONY: all $(COMMANDS) $(PKGS_TO_TEST) review codechecks
.NOTPARALLEL:

all: $(COMMANDS) $(PKGS_TO_TEST)

clean:
	rm -f $(COMMANDS)

$(COMMANDS):
	go get arista.com/quantumfs/cmd/$@
	go build arista.com/quantumfs/cmd/$@

$(PKGS_TO_TEST):
	go test arista.com/quantumfs/$@ 2>&1

codechecks: contentcheck
	./contentcheck

# A utility command to produce a review after running all the necessary checks
review: all codechecks
	rbt post
