// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const testKey = "Hello"
const testValue = "W0rld"
const unknownKey = "H3llo"
const testKey2 = "D@rth"
const testValue2 = "Vad3r"

var testKey2Metadata = map[string]string{}

var etherConfFile string

// EtherConfFile is returns the full path to the Ether's configuration file.
func EtherConfFile() (string, error) {

	if etherConfFile == "" {
		etherEnv := os.Getenv("ETHER_CQL_CONFIG")
		if etherEnv == "" {
			return "", fmt.Errorf("Env varibale ETHER_CQL_CONFIG should be set to a config" +
				" file name in directory ether/cluster_configs")
		}
		_, path, _, _ := runtime.Caller(0)
		path = filepath.Join(filepath.Dir(path), "..", "cluster_configs", etherEnv)
		etherConfFile = path
	}
	return etherConfFile, nil
}
