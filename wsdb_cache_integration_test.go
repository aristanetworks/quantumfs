// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build integration

package cql

import (
	"testing"

	qwsdb "github.com/aristanetworks/ether/qubit/wsdb"
	"github.com/stretchr/testify/suite"
)

type wsdbCacheIntegTestSuite struct {
	suite.Suite
	common *wsdbCommonIntegTest
}

func (suite *wsdbCacheIntegTestSuite) SetupTest() {
	confFile, err := EtherConfFile()
	suite.Require().NoError(err, "error in getting ether configuration file")
	err = SetupTestSchema(confFile)
	suite.Require().NoError(err, "SetupSchema returned an error")

	wsdb := NewWorkspaceDB(confFile)

	err = wsdb.CreateWorkspace(integTestEtherCtx, qwsdb.NullSpaceName, qwsdb.NullSpaceName, qwsdb.NullSpaceName, 0, []byte(nil))
	suite.Require().NoError(err, "Error during CreateWorkspace")

	suite.common = &wsdbCommonIntegTest{
		req: suite.Require(),
		db:  wsdb,
	}
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegEmptyDB() {
	suite.common.TestIntegEmptyDB()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegBranching() {
	suite.common.TestIntegBranching()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegAdvanceOk() {
	suite.common.TestIntegAdvanceOk()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegDeleteNullTypespace() {
	suite.common.TestIntegDeleteNullTypespace()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegDeleteWorkspaceOK() {
	suite.common.TestIntegDeleteWorkspaceOK()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegWorkspaceLastWriteTime() {
	suite.common.TestIntegWorkspaceLastWriteTime()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegDeleteWorkspaceNumOK() {
	err := suite.common.db.BranchWorkspace(integTestEtherCtx, qwsdb.NullSpaceName,
		qwsdb.NullSpaceName, qwsdb.NullSpaceName,
		"ts1", "ns1", "ws1")
	suite.Require().NoError(err,
		"Error branching null workspace: %v", err)

	count, err1 := suite.common.db.NumTypespaces(integTestEtherCtx)
	suite.Require().NoError(err1,
		"Error NumTypespaces: %v", err1)
	suite.Require().Equal(2, count,
		"Unexpected count of typespaces. Exp: 2 Actual: %d",
		count)

	delErr := suite.common.db.DeleteWorkspace(integTestEtherCtx, "ts1", "ns1", "ws1")
	suite.Require().NoError(delErr,
		"Error DeleteWorkspace: %v", delErr)

	count, err1 = suite.common.db.NumTypespaces(integTestEtherCtx)
	suite.Require().NoError(err1,
		"Error NumTypespaces: %v", err1)
	suite.Require().Equal(1, count,
		"Unexpected count of typespaces. Exp: 1 Actual: %d",
		count)
}

func (suite *wsdbCacheIntegTestSuite) TearDownTest() {
	confFile, err := EtherConfFile()
	suite.Require().NoError(err, "error in getting ether configuration file")
	_ = TearDownTestSchema(confFile)
	resetCqlStore()
}

func (suite *wsdbCacheIntegTestSuite) TearDownSuite() {
}

func TestWSDBCacheIntegTests(t *testing.T) {
	suite.Run(t, new(wsdbCacheIntegTestSuite))
}
