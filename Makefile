TARGETMAKEFILE="makefile.mk"
SHELL = /bin/bash -o pipefail

ppid:=$(shell ps -o ppid= $$$$)
ROOTDIRNAME:=$(shell echo -e "$(USER)-RootContainer-$(ppid)" | tr -d '[:space:]')
export ROOTDIRNAME

all:
	$(MAKE) -f $(TARGETMAKEFILE) | tee /dev/tty | ./cleanup.sh $(ppid)

%:
	$(MAKE) -f $(TARGETMAKEFILE) $@ | tee /dev/tty | ./cleanup.sh $(ppid)
