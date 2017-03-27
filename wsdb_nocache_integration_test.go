// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build integration

package cql

import (
	"testing"

	"github.com/aristanetworks/ether/qubit/wsdb"
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

	var wsdb wsdb.WorkspaceDB
	cluster := NewRealCluster(cfg.Cluster)
	wsdb, err = newNoCacheWsdb(cluster, cfg)
	suite.Require().NoError(err, "Error during configuration read")

	suite.common = &wsdbCommonIntegTest{
		req: suite.Require(),
		db:  wsdb,
	}
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegEmptyDB() {
	suite.common.TestIntegEmptyDB()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegBranching() {
	suite.common.TestIntegBranching()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegAdvanceOk() {
	suite.common.TestIntegAdvanceOk()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegDeleteNullTypespace() {
	suite.common.TestIntegDeleteNullTypespace()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegDeleteWorkspaceOK() {
	suite.common.TestIntegDeleteWorkspaceOK()
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
