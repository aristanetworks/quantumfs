# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

FROM scratch

MAINTAINER quantumfs-dev@arista.com

ADD wsdbservice /wsdbservice
ADD ether.conf /ether.conf
EXPOSE 2222

# It is assumed ether.conf will be injected by Kubernetes
ENTRYPOINT ["/wsdbservice", "-logPath", "/", "-workspaceDB", "ether.cql", "-workspaceDBconf", "ether.conf"]
