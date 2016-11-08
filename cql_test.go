// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSetupStubbedCustomCqlStore(t *testing.T) {
	var config CqlConfig
	var name string

	config.StubCQL = true
	config.Nodes = []string{"node1", "node2"}

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	require.NoError(t, err, "Tempfile creation failed")
	name = file.Name()
	file.Close()
	defer os.Remove(name)

	err = WriteCqlConfig(name, &config)
	require.NoError(t, err, "CQL config file write failed")

	initCqlStore(name)
	defer resetCqlStore()
	require.Equal(t, globalCqlStore.cluster.Hosts, config.Nodes,
		"Nodes in GoCQL Cluster != config.Nodes")
}
