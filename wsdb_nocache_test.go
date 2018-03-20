// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type wsdbNoCacheTestSuite struct {
	suite.Suite
	common *wsdbCommonUnitTest
}

func (suite *wsdbNoCacheTestSuite) SetupSuite() {
	resetCqlStore()
}

// setup an empty workspace DB instance (along with its cache)
func (suite *wsdbNoCacheTestSuite) SetupTest() {
	mockCluster := new(MockCluster)
	mockSession := new(MockSession)
	mockSession.On("Close").Return(nil)
	mockCluster.On("CreateSession").Return(mockSession, nil)
	mockSchemaOk(mockSession, "workspacedb", nil)

	mockCfg := setupMockConfig()
	wsdb, err := newNoCacheWsdb(mockCluster, mockCfg)
	suite.Require().NoError(err, "Failed %q workspaceDB initialization", err)

	suite.common = &wsdbCommonUnitTest{
		req:      suite.Require(),
		wsdb:     wsdb,
		mockSess: mockSession,
		cfg:      mockCfg,
	}
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheEmptyDB() {
	suite.common.TestEmptyDB()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheBranching() {
	suite.common.TestBranching()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheAdvanceOk() {
	suite.common.TestAdvanceOk()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheAdvanceOutOfDateKey() {
	suite.common.TestAdvanceOutOfDateKey()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheAdvanceOutOfDateNonce() {
	suite.common.TestAdvanceOutOfDateNonce()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheAdvanceNotExist() {
	suite.common.TestAdvanceNotExist()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheLockedBranching() {
	suite.common.TestLockedBranchWorkspace()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheLockedAdvance() {
	suite.common.TestLockedAdvanceWorkspace()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheInitialAdvanceWorkspace() {
	suite.common.TestInitialAdvanceWorkspace()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheDeleteNullTypespace() {
	suite.common.TestDeleteNullTypespace()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheDeleteWorkspaceOK() {
	suite.common.TestDeleteWorkspaceOK()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheWorkspaceLastWriteTime() {
	suite.common.TestWorkspaceLastWriteTime()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheCreateWorkspaceNoKey() {
	suite.common.TestCreateWorkspaceNoKey()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheCreateWorkspaceDiffKey() {
	suite.common.TestCreateWorkspaceDiffKey()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheCreateWorkspaceSameKey() {
	suite.common.TestCreateWorkspaceSameKey()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheSetWorkspaceImmutable() {
	suite.common.TestSetWorkspaceImmutable()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheSetWorkspaceImmutableError() {
	suite.common.TestSetWorkspaceImmutableError()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheWorkspaceIsImmutable() {
	suite.common.TestWorkspaceIsImmutable()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheWorkspaceIsImmutableError() {
	suite.common.TestWorkspaceIsImmutableError()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheDeleteImmutableSet() {
	suite.common.TestDeleteImmutableSet()
}

func (suite *wsdbNoCacheTestSuite) TearDownTest() {
	resetCqlStore()
}

func (suite *wsdbNoCacheTestSuite) TearDownSuite() {
}

func TestWsdbNoCacheUnitTests(t *testing.T) {
	suite.Run(t, new(wsdbNoCacheTestSuite))
}
