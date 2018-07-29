// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
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

const schemaCheckSleep = 5 * time.Second

// Check if the table is present. Loop (1 + cfg.Cluster.CheckSchemaRetries) times
func isTablePresent(store *cqlStore, cfg *Config, keySpace, tableName string) error {
	var err error
	for retry := 0; retry <= cfg.Cluster.CheckSchemaRetries; retry++ {
		if err = schemaOk(store, cfg, keySpace, tableName); err != nil {
			fmt.Fprintf(os.Stderr, "schemaCheck error: %v\n", err)
			if retriesLeft := cfg.Cluster.CheckSchemaRetries - retry; retriesLeft > 0 {
				fmt.Printf("schemaCheck will be retried after %s\n", schemaCheckSleep)
				time.Sleep(schemaCheckSleep)
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("schemaCheck on %s.%s failed. Error: %s", cfg.Cluster.KeySpace, tableName, err)
}

// system tables are available after the 9042 port is up, so
// read system tables to check following -
// - appropriate table exists
// - appropriate permissions exist
//
// NOTE: "describe" is a cqlsh command and not a CQL protocol command
//       so can't use it to check schema
func schemaOk(store *cqlStore, cfg *Config, keySpace, tableName string) error {
	return schemaCheckV2(store, cfg, keySpace, tableName)
}

// supports cassandraV2 system table schema
func schemaCheckV2(store *cqlStore, cfg *Config, keySpace, tableName string) error {
	// In CQL, all the keyspace and table names are always in lower case
	checkTableQuery := fmt.Sprintf("SELECT count(*) FROM system.schema_columns "+
		"WHERE keyspace_name='%s' AND columnfamily_name='%s'",
		strings.ToLower(keySpace), strings.ToLower(tableName))
	checkPermsQuery := fmt.Sprintf("SELECT permissions FROM system_auth.permissions "+
		"WHERE username='%s' AND resource='data/%s'",
		cfg.Cluster.Username, // username is case-sensitive
		strings.ToLower(keySpace))

	var countCols int
	if err := store.session.Query(checkTableQuery).Scan(&countCols); err != nil {
		return fmt.Errorf("schema column check failed: %s", err.Error())
	}

	if countCols == 0 {
		return errors.New("table/columns not available")
	}

	var actualPermsList []string
	if err := store.session.Query(checkPermsQuery).Scan(&actualPermsList); err != nil {
		// skip the permission check if the system_auth table does not exist
		if strings.Contains(err.Error(), "system_auth does not exist") {
			return nil
		}
		return fmt.Errorf("permission check failed: %s", err.Error())
	}

	expectedPermsList := []string{"SELECT", "MODIFY"}
	return containsExpectedPerms(actualPermsList, expectedPermsList)
}

// returns an error if any of the expected permission is absent
// in the actual list of permissions
func containsExpectedPerms(actual, expected []string) error {
	// use map to test for existence of each expected perm
	actualPerms := make(map[string]struct{}, len(actual))
	for _, aperm := range actual {
		actualPerms[aperm] = struct{}{}
	}

	for _, eperm := range expected {
		if _, exists := actualPerms[eperm]; !exists {
			return fmt.Errorf("unexpected permissions found. Exp (%s) Actual (%s)",
				expected, actual)
		}
	}

	return nil
}
