#!/bin/bash

# Copyright (c) 2017 Arista Networks, Inc.
# Use of this source code is governed by the Apache License 2.0
# that can be found in the COPYING file.

# Pull scylla docker, run container
# docker pull scylladb/scylla
# docker run --name some-scylla -p 9042:9042 -d scylladb/scylla  --broadcast-address 127.0.0.1  --listen-address 0.0.0.0  --broadcast-rpc-address 127.0.0.1

# setup schema
CFG_DIR="${GOPATH}/src/github.com/aristanetworks/quantumfs/backends/cql/cluster_configs"
TESTID=`echo $$$$`

# Run the test
CQL_CONFIG="${CFG_DIR}/dkr_CqlIntegTest"  \
CFNAME_PREFIX="intg${TESTID}" \
go test -v  -timeout 200m -p 1 -tags longrunningtests -run Integ github.com/aristanetworks/quantumfs/backends/cql


# // Smoke
# sudo  ./quantumfsd -datastore cql -datastoreconf $QFS_CONFIG -workspaceDB cql -workspaceDBconf $QFS_CONFIG


