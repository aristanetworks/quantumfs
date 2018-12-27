// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"os"
)

const testKey = "Hello"
const testValue = "W0rld"
const unknownKey = "H3llo"
const testKey2 = "D@rth"
const testValue2 = "Vad3r"

var testKey2Metadata = map[string]string{}
var unitTestCqlCtx = DefaultCtx
var integTestCqlCtx = DefaultCtx

var tstUsername = scyllaUsername
var tstKeyspace = "cql"
var cqlConfFile string

// CqlConfFile returns the full path to the cql's configuration file
// based on environment variable ETHER_CQL_CONFIG.
func CqlConfFile() (string, error) {

	if cqlConfFile == "" {
		cqlEnv := os.Getenv("ETHER_CQL_CONFIG")
		if cqlEnv == "" {
			return "", fmt.Errorf("Env varibale ETHER_CQL_CONFIG " +
				"should be set to a config " +
				"file name in directory cql/cluster_configs")
		}
		cqlConfFile = cqlEnv
	}
	return cqlConfFile, nil
}
