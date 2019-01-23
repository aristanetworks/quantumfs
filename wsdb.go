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

// NewWorkspaceDB creates a workspace DB API endpoint
func NewWorkspaceDB(confName string) wsdb.WorkspaceDB {

	cfg, err := readCqlConfig(confName)
	if err != nil {
		fmt.Println("Error reading CQL config: ", err)
		panic(err.Error())
	}
	cluster := NewRealCluster(cfg.Cluster)

	var wsdb wsdb.WorkspaceDB
	if wsdb, err = newNoCacheWsdb(cluster, cfg); err != nil {
		panic(fmt.Sprintf("Error %q during NewNoCacheWsdb", err))
	}

	return newCacheWsdb(wsdb, cfg.WsDB)
}

// GetUniqueNonce provides a unique nonce
var GetUniqueNonce = getTimeBasedNonce

func getTimeBasedNonce() wsdb.WorkspaceNonce {
	return wsdb.WorkspaceNonce{uint64(time.Now().UnixNano()), 0}
}
