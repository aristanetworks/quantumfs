// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build integration

package cql

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type wsdbNoCacheIntegTestSuite struct {
	suite.Suite
	common *wsdbCommonIntegTest
}

func (suite *wsdbNoCacheIntegTestSuite) SetupTest() {
	confFile, err := CqlConfFile()
	suite.Require().NoError(err, "error in getting ether configuration file")
	var cfg *Config
	cfg, err = readCqlConfig(confFile)
	suite.Require().NoError(err, "Error during configuration read")
	err = SetupIntegTestKeyspace(confFile)
	suite.Require().NoError(err, "SetupIntegTestKeyspace returned an error")
	err = DoTestSchemaOp(confFile, SchemaCreate)
	suite.Require().NoError(err, "DoTestSchemaOp SchemaCreate returned an error")

	var wsdb WorkspaceDB
	cluster := NewRealCluster(&cfg.Cluster)
	wsdb, err = newNoCacheWsdb(cluster, cfg)
	suite.Require().NoError(err, "Error during configuration read")

	err = wsdb.CreateWorkspace(integTestCqlCtx, NullSpaceName,
		NullSpaceName, NullSpaceName,
		WorkspaceNonceInvalid, []byte(nil))
	suite.Require().NoError(err, "Error during CreateWorkspace")

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

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegWorkspaceLastWriteTime() {
	suite.common.TestIntegWorkspaceLastWriteTime()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegDeleteWorkspaceOK() {
	suite.common.TestIntegDeleteWorkspaceOK()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegWorkspaceNonce() {
	suite.common.TestIntegWorkspaceNonce()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegSetWorkspaceImmutable() {
	suite.common.TestIntegSetWorkspaceImmutable()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegSetWorkspaceImmutableErr() {
	suite.common.TestIntegSetWorkspaceImmutableError()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegWorkspaceIsImmutable() {
	suite.common.TestIntegWorkspaceIsImmutable()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegWorkspaceIsImmutableError() {
	suite.common.TestIntegWorkspaceIsImmutableError()
}

func (suite *wsdbNoCacheIntegTestSuite) TestNoCacheIntegDeleteImmutableSet() {
	suite.common.TestIntegDeleteImmutableSet()
}

func (suite *wsdbNoCacheIntegTestSuite) TearDownTest() {
	confFile, err := CqlConfFile()
	suite.Require().NoError(err, "error in getting ether configuration file")
	_ = DoTestSchemaOp(confFile, SchemaDelete)
	resetCqlStore()
}

func (suite *wsdbNoCacheIntegTestSuite) TearDownSuite() {
}

func TestWSDBNoCacheIntegTests(t *testing.T) {
	suite.Run(t, new(wsdbNoCacheIntegTestSuite))
}
