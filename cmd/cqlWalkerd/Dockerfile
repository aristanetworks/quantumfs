# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

FROM alpine:3.6
MAINTAINER ether-dev <ether-dev@arista.com>

COPY qubit-walkerd /usr/sbin/qubit-walkerd
COPY qwalker/config/ether.dev.walker.cfg /etc/qubit-walkerd.d/ether.dev.walker.cfg
COPY qwalker/config/ether.prod.walker.cfg /etc/qubit-walkerd.d/ether.prod.walker.cfg

ENTRYPOINT [ "/usr/sbin/qubit-walkerd" ]
# CMD ARGS are in the k8s service file
