// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/aristanetworks/ether/utils"
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
	sem       utils.Semaphore
}

var globalCqlStore cqlStoreGlobal

type cqlStore struct {
	config  Config
	cluster Cluster
	session Session
	sem     *utils.Semaphore
}

// WriteCqlConfig converts the Config struct to a JSON file
func writeCqlConfig(fileName string, config *Config) error {

	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("error writing cql config file %q: %v", fileName, err)
	}

	err = json.NewEncoder(file).Encode(config)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("error encoding cql config file %q: %v", fileName, err)
	}

	return nil
}

func readCqlConfig(fileName string) (*Config, error) {
	var config Config

	file, err := os.Open(fileName)
	if err != nil {
		return nil, fmt.Errorf("error opening cql config file %q: %v", fileName, err)
	}

	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("error decoding cql config file %q: %v", fileName, err)
	}

	file.Close()
	return &config, nil
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
		globalCqlStore.sem = make(utils.Semaphore, 100)
		globalCqlStore.session, err = globalCqlStore.cluster.CreateSession()
		if err != nil {
			err = fmt.Errorf("error in initCqlStore: %v", err)
			return
		}
	})

	var cStore cqlStore
	cStore.config = globalCqlStore.config
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
		globalCqlStore.config = Config{}
		globalCqlStore.cluster = nil
		globalCqlStore.session = nil
		globalCqlStore.sem = nil
		globalCqlStore.initOnce = sync.Once{}
	})
}
