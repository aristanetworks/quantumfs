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

	mockWsdbKeyPut(mockSession, "_null", "null", []byte(nil), nil)

	mockCfg := &Config{
		Cluster: ClusterConfig{
			KeySpace: "ether",
		},
	}

	wsdb, err := newNoCacheWsdb(mockCluster, mockCfg)
	suite.Require().NoError(err, "Failed %q workspaceDB initialization", err)

	suite.common = &wsdbCommonUnitTest{
		req:      suite.Require(),
		wsdb:     wsdb,
		mockSess: mockSession,
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

func (suite *wsdbNoCacheTestSuite) TestNoCacheAdvanceOutOfDate() {
	suite.common.TestAdvanceOutOfDate()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheAdvanceNotExist() {
	suite.common.TestAdvanceNotExist()
}

func (suite *wsdbNoCacheTestSuite) TestNoCacheNamespaceNotExist() {
	suite.common.TestNamespaceNotExist()
}

func (suite *wsdbNoCacheTestSuite) TearDownTest() {
	resetCqlStore()
}

func (suite *wsdbNoCacheTestSuite) TearDownSuite() {
}

func TestWsdbNoCacheUnitTests(t *testing.T) {
	suite.Run(t, new(wsdbNoCacheTestSuite))
}
