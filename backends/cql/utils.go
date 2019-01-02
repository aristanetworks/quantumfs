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

// These errors indicate that the system tables are available
// but the application schema and/or permissions aren't available.
// Any other error indicates an issue with system tables and is a hint
// to use other version of system tables to perform schema and perms
// check.
var noAppSchemaErr = errors.New("app schema not available")
var unexpectedAppPermsErr = errors.New("unexpected app permissions found")

func tryNextSystemTableVersion(err error) bool {
	switch err {
	case nil:
	case noAppSchemaErr:
	case unexpectedAppPermsErr:
	default:
		return true
	}
	return false
}

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

// checks in different Cassandra/Scylla system table versions
// for app schema and permissions.
var checks = []func(*cqlStore, *Config, string, string) error{
	// the checks must be ordered from the lowest
	// supported version
	schemaCheckV2,
	schemaCheckV3,
}

// system tables are available after the 9042 port is up, so
// read system tables to check following -
// - appropriate table exists
// - appropriate permissions exist
//
// NOTE: "describe" is a cqlsh command and not a CQL protocol command
//       so can't use it to check schema
func schemaOk(store *cqlStore, cfg *Config, keySpace, tableName string) error {
	// The schema check query on system tables may end up on any host
	// (token-aware selection of host cannot be used for this query,
	// since system tables are local, round-robin host selection is used by
	// GoCQL driver). It is possible that there are multiple versions of Cassandra/Scylla
	// active in the cluster at any time (eg: in the middle of a rolling
	// upgrade). Since a session represents connectivity to multiple
	// hosts there is no notion of cassandra version of a session.
	// Hence we need to check starting from the least supported version.
	var err error
	for _, check := range checks {
		err = check(store, cfg, keySpace, tableName)
		if tryNextSystemTableVersion(err) {
			continue
		}
		break
	}
	return err
}

// Instead of having a single schema check for different versions of Cassandra/Scylla
// system table format, we use separate routines even though some of the code is common.
// This keeps addition of more newer versions clean.

// schemaCheckV2 supports cassandraV2 system table schema
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
		return fmt.Errorf("v2: schema column check failed: %s", err.Error())
	}

	if countCols == 0 {
		return noAppSchemaErr
	}

	var actualPermsList []string
	if err := store.session.Query(checkPermsQuery).Scan(&actualPermsList); err != nil {
		// skip the permission check if the system_auth table does not exist
		if strings.Contains(err.Error(), "system_auth does not exist") {
			return nil
		}
		return fmt.Errorf("v2: permission check failed: %s", err.Error())
	}

	expectedPermsList := []string{"SELECT", "MODIFY"}
	return containsExpectedPerms(actualPermsList, expectedPermsList)
}

// schemaCheckV3 supports cassandraV3 system table schema
func schemaCheckV3(store *cqlStore, cfg *Config, keySpace, tableName string) error {
	// In CQL, all the keyspace and table names are always in lower case
	checkTableQuery := fmt.Sprintf("SELECT count(*) FROM system_schema.columns "+
		"WHERE keyspace_name='%s' AND table_name='%s'",
		strings.ToLower(keySpace), strings.ToLower(tableName))
	checkPermsQuery := fmt.Sprintf("SELECT permissions FROM system_auth.permissions "+
		"WHERE username='%s' AND resource='data/%s'",
		cfg.Cluster.Username, // username is case-sensitive
		strings.ToLower(keySpace))

	var countCols int
	if err := store.session.Query(checkTableQuery).Scan(&countCols); err != nil {
		return fmt.Errorf("v3: schema column check failed: %s", err.Error())
	}

	if countCols == 0 {
		return noAppSchemaErr
	}

	var actualPermsList []string
	if err := store.session.Query(checkPermsQuery).Scan(&actualPermsList); err != nil {
		// skip the permission check if the system_auth table does not exist
		if strings.Contains(err.Error(), "system_auth does not exist") ||
			strings.Contains(err.Error(), "unconfigured table permissions") {
			return nil
		}
		return fmt.Errorf("v3: permission check failed: %s", err.Error())
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
			return unexpectedAppPermsErr
		}
	}

	return nil
}
