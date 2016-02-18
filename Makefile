COMMANDS=quantumfsd

.PHONY: all $(COMMANDS)
.NOTPARALLEL:

all: $(COMMANDS)

clean:
	rm -f $(COMMANDS)

$(COMMANDS):
	go build arista.com/quantumfs/cmd/$@
