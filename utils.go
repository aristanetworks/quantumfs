// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
)

func checkCfNamePrefix(prefix string) error {
	if prefix == "" ||
		(prefix != "" && len(prefix) >= maxKsTblPrefixLen) ||
		(prefix != "" && !isValidCqlKsTableName(prefix)) {
		return fmt.Errorf("Invalid prefix %q", prefix)
	}
	return nil
}

func prefixToTblNames(prefix string) (bsName string, wsdbName string) {
	bsName = fmt.Sprintf("%sblobStore", prefix)
	wsdbName = fmt.Sprintf("%sworkspacedb", prefix)
	return
}

// isValidCqlKsTableName checks if name is valid
// name for keyspace or table as per CQL grammar
// defined at https://cassandra.apache.org/doc/latest/cql/ddl.html
func isValidCqlKsTableName(name string) bool {
	isCql := regexp.MustCompile(`^\w{1,48}$`).MatchString
	if !isCql(name) {
		return false
	}
	return true
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
