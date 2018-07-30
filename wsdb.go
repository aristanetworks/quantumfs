// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"time"

	"github.com/aristanetworks/ether/qubit/wsdb"
)

// The workspace DB API endpoint is instantiated here.
// Current API endpoint uses cache but in future the endpoint
// may be cache enabled or uncached depending
// on workspace DB configuration parameter.

// NewUncachedWorkspaceDB creates an uncached
// workspace DB API endpoint.
func NewUncachedWorkspaceDB(confName string) wsdb.WorkspaceDB {
	cfg, err := readCqlConfig(confName)
	if err != nil {
		fmt.Println("Error reading CQL config: ", err)
		panic(err.Error())
	}
	cluster := NewRealCluster(&cfg.Cluster)
	var wsdb wsdb.WorkspaceDB
	if wsdb, err = newNoCacheWsdb(cluster, cfg); err != nil {
		panic(fmt.Sprintf("Error %q during NewNoCacheWsdb", err))
	}
	return wsdb
}

// NewWorkspaceDB creates a cached workspace DB API endpoint.
// This endpoint caches objects like
// typespaces, namespaces, workspaces etc. The cache
// can be tuned using WSDB section in config file.
func NewWorkspaceDB(confName string) wsdb.WorkspaceDB {
	cfg, err := readCqlConfig(confName)
	if err != nil {
		fmt.Println("Error reading CQL config: ", err)
		panic(err.Error())
	}

	wsdb := NewUncachedWorkspaceDB(confName)
	return newCacheWsdb(wsdb, cfg.WsDB)
}

// GetUniqueNonce provides a unique nonce
var GetUniqueNonce = getTimeBasedNonce

func getTimeBasedNonce() wsdb.WorkspaceNonce {
	return wsdb.WorkspaceNonce{
		Id:          time.Now().UnixNano(),
		PublishTime: time.Now().UnixNano(),
	}
}
