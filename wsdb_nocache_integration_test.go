// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build integration

package cql

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/stretchr/testify/suite"
)

type wsdbNoCacheIntegTestSuite struct {
	suite.Suite
	common *wsdbCommonIntegTest
}

func (suite *wsdbNoCacheIntegTestSuite) SetupTest() {
	confFile, err := EtherConfFile()
	suite.Require().NoError(err, "error in getting ether configuration file")
	var cfg *Config
	cfg, err = readCqlConfig(confFile)
	suite.Require().NoError(err, "Error during configuration read")
	err = SetupTestSchema(confFile)
	suite.Require().NoError(err, "SetupSchema returned an error")

	var wsdb quantumfs.WorkspaceDB
	cluster := NewRealCluster(cfg.Cluster)
	wsdb, err = newNoCacheWsdb(cluster, cfg)
	suite.Require().NoError(err, "Error during configuration read")

	suite.common = &wsdbCommonIntegTest{
		req: suite.Require(),
		db:  wsdb,
	}
}

func (suite *wsdbNoCacheIntegTestSuite) TestCacheIntegEmptyDB() {
	suite.common.TestIntegEmptyDB()
}

func (suite *wsdbNoCacheIntegTestSuite) TestCacheIntegBranching() {
	suite.common.TestIntegBranching()
}

func (suite *wsdbNoCacheIntegTestSuite) TestCacheIntegAdvanceOk() {
	suite.common.TestIntegAdvanceOk()
}

func (suite *wsdbNoCacheIntegTestSuite) TearDownTest() {
	confFile, err := EtherConfFile()
	suite.Require().NoError(err, "error in getting ether configuration file")
	_ = TearDownTestSchema(confFile)
	resetCqlStore()
}

func (suite *wsdbNoCacheIntegTestSuite) TearDownSuite() {
}

func TestWSDBNoCacheIntegTests(t *testing.T) {
	suite.Run(t, new(wsdbNoCacheIntegTestSuite))
}
