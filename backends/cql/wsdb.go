// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package cql

import (
	"fmt"
	"time"
)

// The workspace DB API endpoint is instantiated here.
// Current API endpoint uses cache but in future the endpoint
// may be cache enabled or uncached depending
// on workspace DB configuration parameter.

// NewUncachedWorkspaceDB creates an uncached
// workspace DB API endpoint.
func NewUncachedWorkspaceDB(confName string) WorkspaceDB {
	cfg, err := readCqlConfig(confName)
	if err != nil {
		fmt.Println("Error reading CQL config: ", err)
		panic(err.Error())
	}
	cluster := NewRealCluster(&cfg.Cluster)
	var wsdb WorkspaceDB
	if wsdb, err = newNoCacheWsdb(cluster, cfg); err != nil {
		panic(fmt.Sprintf("Error %q during NewNoCacheWsdb", err))
	}
	return wsdb
}

// NewWorkspaceDB creates a cached workspace DB API endpoint.
// This endpoint caches objects like
// typespaces, namespaces, workspaces etc. The cache
// can be tuned using WSDB section in config file.
func NewWorkspaceDB(confName string) WorkspaceDB {
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

func getTimeBasedNonce() WorkspaceNonce {
	return WorkspaceNonce{
		Id:          time.Now().UnixNano(),
		PublishTime: time.Now().UnixNano(),
	}
}
