COMMANDS=quantumfsd qfs

.PHONY: all $(COMMANDS)
.NOTPARALLEL:

all: $(COMMANDS)

clean:
	rm -f $(COMMANDS)

$(COMMANDS):
	go get arista.com/quantumfs/cmd/$@
	go build arista.com/quantumfs/cmd/$@
