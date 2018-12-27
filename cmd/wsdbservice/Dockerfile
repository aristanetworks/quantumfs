# Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

FROM scratch

MAINTAINER quantumfs-dev@arista.com

ADD wsdbservice /wsdbservice
EXPOSE 2222

# It is assumed cql.conf will be injected by Kubernetes
ENTRYPOINT ["/wsdbservice", "-logPath", "/", "-workspaceDB", "cql", "-workspaceDBconf", "/etc/config/cql.conf"]
