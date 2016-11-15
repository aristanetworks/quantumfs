// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/json"
	"os"
	"sync"
)

// Config struct holds the info needed to connect to a cql cluster
type Config struct {
	Nodes      []string `json:"nodes"`
	NumConns   int      `json:"numConnections"`
	NumRetries int      `json:"numRetries"`
}

type cqlStoreGlobal struct {
	config    Config
	initOnce  sync.Once
	resetOnce sync.Once
	cluster   Cluster
	session   Session
}

var globalCqlStore cqlStoreGlobal

type cqlStore struct {
	config  Config
	cluster Cluster
	session Session
}

// WriteCqlConfig converts the Config struct to a JSON file
func writeCqlConfig(fileName string, config *Config) error {

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

func readCqlConfig(fileName string) (*Config, error) {
	var config Config

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
func initCqlStore(cluster Cluster, mocking bool) (cqlStore, error) {

	var err error
	globalCqlStore.initOnce.Do(func() {

		globalCqlStore.cluster = cluster
		globalCqlStore.session, err = globalCqlStore.cluster.CreateSession()
		if err != nil {
			return
		}
		globalCqlStore.resetOnce = sync.Once{}
	})

	var cStore cqlStore
	cStore.config = globalCqlStore.config
	cStore.cluster = globalCqlStore.cluster
	cStore.session = globalCqlStore.session

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
		globalCqlStore.config = Config{}
		globalCqlStore.cluster = nil
		globalCqlStore.session = nil
		globalCqlStore.initOnce = sync.Once{}
	})
}
