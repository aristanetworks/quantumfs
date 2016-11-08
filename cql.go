// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/gocql/gocql"
)

// CqlConfig struct holds the info needed to connect to a cql cluster
type CqlConfig struct {
	Nodes      []string `json:"nodes"`
	NumConns   int      `json:"numConnections"`
	NumRetries int      `json:"numRetries"`
	StubCQL    bool     `json:"stubCQL"` // used in tests to skip connection to datastore
}

type cqlStore struct {
	config    CqlConfig
	initOnce  sync.Once
	resetOnce sync.Once
	cluster   *gocql.ClusterConfig
	session   *gocql.Session
}

var globalCqlStore cqlStore

// WriteCqlConfig convert the CqlConfig struct to a file
func WriteCqlConfig(fileName string, config *CqlConfig) error {

	file, err := os.Create(fileName)
	if err != nil {
		return err
	}

	err = json.NewEncoder(file).Encode(config)
	if err != nil {
		file.Close()
		return err
	}

	err = file.Close()
	return err
}

func readCqlConfig(fileName string) (*CqlConfig, error) {
	var config CqlConfig

	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		file.Close()
		return nil, err
	}

	err = file.Close()
	return &config, err
}

// Note: This routine is called by Init/New APIs
//       in Ether and only one global initialization is done.

// TBD: Based on use-cases revisit this singleton cluster
//      and session context design
// TBD: Need more investigation to see which parts of the
//      config can be dynamically updated
func initCqlStore(confName string) {
	globalCqlStore.initOnce.Do(func() {
		var err error
		var cfg *CqlConfig

		cfg, err = readCqlConfig(confName)
		if err != nil {
			fmt.Println("Error reading CQL config: ", err)
			panic(err.Error())
		}
		globalCqlStore.config = *cfg

		globalCqlStore.cluster = gocql.NewCluster(globalCqlStore.config.Nodes...)
		globalCqlStore.cluster.Keyspace = "qfs"
		globalCqlStore.cluster.ProtoVersion = 3
		globalCqlStore.cluster.Consistency = gocql.Quorum
		globalCqlStore.cluster.RetryPolicy =
			&gocql.SimpleRetryPolicy{NumRetries: globalCqlStore.config.NumRetries}
		globalCqlStore.cluster.PoolConfig.HostSelectionPolicy =
			gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())

		if !globalCqlStore.config.StubCQL {
			globalCqlStore.session, err = globalCqlStore.cluster.CreateSession()
			if err != nil {
				fmt.Println("CreateSession error: ", err)
				panic(err.Error())
			}
		}

		globalCqlStore.resetOnce = sync.Once{}
	})
}

// mostly used by tests
func resetCqlStore() {
	globalCqlStore.resetOnce.Do(func() {
		if globalCqlStore.session != nil {
			globalCqlStore.session.Close()
		}
		// we cannot do globalCqlStore = cqlStore{}
		// since we are inside resetOnce
		globalCqlStore.config = CqlConfig{}
		globalCqlStore.cluster = nil
		globalCqlStore.session = nil
		globalCqlStore.initOnce = sync.Once{}
	})
}
