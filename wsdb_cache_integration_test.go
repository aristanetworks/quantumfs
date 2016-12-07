// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build integration

package cql

import (
	"testing"

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
