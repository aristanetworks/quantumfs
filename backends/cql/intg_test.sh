#!/bin/bash

# Pull scylla docker, run container


# setup schema
CFG_DIR="${GOPATH}/src/github.com/aristanetworks/quantumfs/backends/cql/cluster_configs"
TESTID=`echo $$$$`

# docker run --name some-scylla -p 9042:9042 -d scylladb/scylla --broadcast-address 127.0.0.1 --listen-address 0.0.0.0 --broadcast-rpc-address 127.0.0.1

# Run the test
ETHER_CQL_CONFIG="${CFG_DIR}/dkr_EtherIntegTest"  \
CFNAME_PREFIX="intg${TESTID}" \
go test -v -race -p 1 -tags integration -run Integ github.com/aristanetworks/quantumfs/backends/cql
