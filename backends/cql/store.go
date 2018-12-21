// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"sync"

	"github.com/aristanetworks/quantumfs/backends/cql/utils"
)

var scyllaUsername = "ether"
var scyllaPassword = "ether"

// Config struct holds the info needed to connect to a cql cluster
// and knobs for the different APIs
type Config struct {
	Cluster   ClusterConfig   `json:"cluster"`
	BlobStore BlobStoreConfig `json:"blobstore"`
	WsDB      WsDBConfig      `json:"wsdb"`
	BlobMap   BlobMapConfig   `json:"blobmap"`
}

// ClusterConfig struct holds the info needed to connect to a cql cluster
type ClusterConfig struct {
	Nodes              []string `json:"nodes"`
	ClusterName        string   `json:"clusterName"`
	NumConns           int      `json:"numconnections"`
	QueryNumRetries    int      `json:"querynumretries"`
	KeySpace           string   `json:"keyspace"`
	ConnTimeoutSec     int      `json:"conntimeoutsec"`
	Username           string   `json:"username"`
	Password           string   `json:"password"`
	CheckSchemaRetries int      `json:"checkschemaretries"`
}

// BlobStoreConfig holds config values specific to BlobStore API
type BlobStoreConfig struct {
	SomeConfig string `json:"someconfig"`
}

// DontExpireWsdbCache disables cache timeouts
const DontExpireWsdbCache = -1

// WsDBConfig holds config values specfic to WorkspaceDB API
type WsDBConfig struct {
	// CacheTimeoutSecs if set to DontExpireWsdbCache disables cache timeouts
	CacheTimeoutSecs int `json:"cachetimeoutsecs"`
}

// BlobMapConfig holds config values specific to BlobMap API
type BlobMapConfig struct {
	SomeConfig string `json:"someconfig"`
}

type cqlStoreGlobal struct {
	initMutex sync.RWMutex
	cluster   Cluster
	session   Session
	sem       utils.Semaphore
}

var globalCqlStore cqlStoreGlobal

type cqlStore struct {
	cluster Cluster
	session Session

	sem *utils.Semaphore
}

// Note: This routine is called by Init/New APIs
//       in Ether and only one global initialization is done.

// TBD: Need more investigation to see which parts of the
//      config can be dynamically updated
func initCqlStore(cluster Cluster) (cqlStore, error) {
	globalCqlStore.initMutex.Lock()
	defer globalCqlStore.initMutex.Unlock()

	if globalCqlStore.session == nil {
		session, err := cluster.CreateSession()
		if err != nil {
			err = fmt.Errorf("error in initCqlStore: %v", err)
			return cqlStore{}, err
		}
		// The semaphore limits the number of concurrent
		// inserts and queries to scyllaDB, otherwise we get timeouts
		// from ScyllaDB. Timeouts are unavoidable since its possible
		// to generate much faster rate of traffic than Scylla can handle.
		// The number 100, has been emperically determined.
		globalCqlStore.sem = make(utils.Semaphore, 100)
		globalCqlStore.cluster = cluster
		globalCqlStore.session = session
	}

	var cStore cqlStore
	cStore.cluster = globalCqlStore.cluster
	cStore.session = globalCqlStore.session
	cStore.sem = &globalCqlStore.sem

	return cStore, nil
}

// mostly used by tests
func resetCqlStore() {
	globalCqlStore.initMutex.Lock()
	defer globalCqlStore.initMutex.Unlock()
	if globalCqlStore.session != nil {
		globalCqlStore.session.Close()
	}
	globalCqlStore.cluster = nil
	globalCqlStore.session = nil
	globalCqlStore.sem = nil
}
