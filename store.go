// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"sync"
	"time"

	"github.com/aristanetworks/ether/utils"
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
	Nodes       []string      `json:"nodes"`
	ClusterName string        `json:"clusterName"`
	NumConns    int           `json:"numconnections"`
	NumRetries  int           `json:"numretries"`
	KeySpace    string        `json:"keyspace"`
	Timeout     time.Duration `json:"timeout"`
	Username    string        `json:"username"`
	Password    string        `json:"password"`
}

// BlobStoreConfig holds config values specific to BlobStore API
type BlobStoreConfig struct {
	SomeConfig string `json:"someconfig"`
}

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
	initOnce  sync.Once
	resetOnce sync.Once
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

// TBD: Based on use-cases revisit this singleton cluster
//      and session context design
// TBD: Need more investigation to see which parts of the
//      config can be dynamically updated
func initCqlStore(cluster Cluster) (cqlStore, error) {

	var err error
	globalCqlStore.initOnce.Do(func() {
		globalCqlStore.cluster = cluster
		globalCqlStore.resetOnce = sync.Once{}
		// The semaphore limits the number of concurrent
		// inserts and queries to scyllaDB, otherwise we get timeouts
		// from ScyllaDB. Timeouts are unavoidable since its possible
		// to generate much faster rate of traffic than Scylla can handle.
		// The number 100, has been emperically determined.
		globalCqlStore.sem = make(utils.Semaphore, 100)
		globalCqlStore.session, err = globalCqlStore.cluster.CreateSession()
		if err != nil {
			err = fmt.Errorf("error in initCqlStore: %v", err)
			return
		}
	})

	var cStore cqlStore
	cStore.cluster = globalCqlStore.cluster
	cStore.session = globalCqlStore.session
	cStore.sem = &globalCqlStore.sem

	return cStore, err
}

// mostly used by tests
func resetCqlStore() {
	globalCqlStore.resetOnce.Do(func() {
		if globalCqlStore.session != nil {
			globalCqlStore.session.Close()
		}
		// we cannot do globalCqlStore = cqlStore{}
		// since we are inside resetOnce
		globalCqlStore.cluster = nil
		globalCqlStore.session = nil
		globalCqlStore.sem = nil
		globalCqlStore.initOnce = sync.Once{}
	})
}
