TARGETMAKEFILE="makefile.mk"
SHELL = /bin/bash -o pipefail

ppid:=$(shell ps -o ppid= $$$$)
ROOTDIRNAME:=$(shell echo -e "$(USER)-RootContainer-$(ppid)" | tr -d '[:space:]')
export ROOTDIRNAME

all:
	make -f $(TARGETMAKEFILE) | tee /dev/tty | ./cleanup.sh $(ppid)

%:
	make -f $(TARGETMAKEFILE) $@ | tee /dev/tty | ./cleanup.sh $(ppid)
