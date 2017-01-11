// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/suite"
)

type wsdbCacheTestSuite struct {
	suite.Suite
	common *wsdbCommonUnitTest
	cache  *entityCache
}

func (suite *wsdbCacheTestSuite) SetupSuite() {
	resetCqlStore()
}

// setup an empty workspace DB instance (along with its cache)
func (suite *wsdbCacheTestSuite) SetupTest() {
	mockCluster := new(MockCluster)
	mockSession := new(MockSession)
	mockSession.On("Close").Return(nil)
	mockCluster.On("CreateSession").Return(mockSession, nil)

	mockWsdbKeyPut(mockSession, "_null", "null",
		[]byte(nil), nil)

	mockCfg := &Config{
		Cluster: ClusterConfig{
			KeySpace: "ether",
		},
	}

	noCacheWsdb, err := newNoCacheWsdb(mockCluster, mockCfg)
	suite.Require().NoError(err, "Failed %q newNoCacheWsdb", err)
	wsdb := newCacheWsdb(noCacheWsdb, mockCfg.WsDB)
	cwsdb, ok := wsdb.(*cacheWsdb)
	suite.Require().True(ok, "Incorrect type from newCacheWsdb")

	suite.cache = cwsdb.cache

	suite.common = &wsdbCommonUnitTest{
		req:      suite.Require(),
		wsdb:     wsdb,
		mockSess: mockSession,
	}
}

func (suite *wsdbCacheTestSuite) TestCacheEmptyDB() {
	suite.common.TestEmptyDB()
}

func (suite *wsdbCacheTestSuite) TestCacheBranching() {
	suite.common.TestBranching()
}

func (suite *wsdbCacheTestSuite) TestCacheAdvanceOk() {
	suite.common.TestAdvanceOk()
}

func (suite *wsdbCacheTestSuite) TestCacheAdvanceOutOfDate() {
	suite.common.TestAdvanceOutOfDate()
}

func (suite *wsdbCacheTestSuite) TestCacheAdvanceNotExist() {
	suite.common.TestAdvanceNotExist()
}

func (suite *wsdbCacheTestSuite) TestCacheNamespaceNotExist() {
	suite.common.TestNamespaceNotExist()
}

func (suite *wsdbCacheTestSuite) TestCacheAfterEmptyDB() {

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, "_null")

	nsCount, err1 := suite.common.wsdb.NumNamespaces()
	suite.Require().NoError(err1,
		"NumNamespaces failed: %s", err1)
	suite.Require().Equal(1, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	nsList, err2 := suite.common.wsdb.NamespaceList()
	suite.Require().NoError(err2,
		"NamespaceList failed: %s", err2)
	suite.Require().Contains(nsList, "_null",
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces("_null")
	suite.Require().NoError(err3,
		"NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	wsList, err4 := suite.common.wsdb.WorkspaceList("_null")
	suite.Require().NoError(err4,
		"WorkspaceList failed: %s", err4)
	suite.Require().Equal([]string{"null"}, wsList,
		"Incorrect workspaces in cache")

}

func (suite *wsdbCacheTestSuite) TestCacheWithRemoteInserts() {
	// test if remotely inserted namespace and workspaces
	// are seen in the cache

	nsRows := mockDbRows{[]interface{}{"_null"}, []interface{}{"remoteNS"}}
	nsIter := new(MockIter)
	nsVals := []interface{}(nil)

	wsRows := mockDbRows{{"remoteWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"remoteNS"}

	mockWsdbCacheNamespaceFetch(suite.common.mockSess, nsRows, nsVals,
		nsIter, nil)
	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, nil)

	suite.cache.enableCqlRefresh()

	nsCount, err1 := suite.common.wsdb.NumNamespaces()
	suite.Require().NoError(err1, "NumNamespace failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	// needs to be done after DB fetch of
	// namespace since the fetch inserts remote
	// inserted namespaces into cache with default age
	suite.cache.enableCqlRefresh("remoteNS")

	nsIter.Reset()
	wsIter.Reset()

	nsList, err2 := suite.common.wsdb.NamespaceList()
	suite.Require().NoError(err2, "NamespaceList failed: %s", err2)
	suite.Require().Contains(nsList, "_null",
		"Incorrect namespaces in cache")
	suite.Require().Contains(nsList, "remoteNS",
		"Incorrect namespaces in cache")

	nsIter.Reset()
	wsIter.Reset()

	wsCount, err3 := suite.common.wsdb.NumWorkspaces("remoteNS")
	suite.Require().NoError(err3, "NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	nsIter.Reset()
	wsIter.Reset()

	wsList, err4 := suite.common.wsdb.WorkspaceList("remoteNS")
	suite.Require().NoError(err4, "WorkspaceList failed: %s", err4)
	suite.Require().Equal([]string{"remoteWS"}, wsList,
		"Incorrect workspaces in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheWithRemoteDeletes() {
	// test if remotely deleted namespace and workspaces
	// drain out from the cache

	// first ensure that remote entries are injected into cache
	nsRows := mockDbRows{[]interface{}{"_null"}, []interface{}{"remoteNS"}}
	nsIter := new(MockIter)
	nsVals := []interface{}(nil)

	wsRows := mockDbRows{{"remoteWS1"}, {"remoteWS2"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"remoteNS"}

	mockWsdbCacheNamespaceFetch(suite.common.mockSess, nsRows, nsVals,
		nsIter, nil)
	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, nil)

	suite.cache.enableCqlRefresh()

	nsCount, err1 := suite.common.wsdb.NumNamespaces()
	suite.Require().NoError(err1, "NumNamespaces failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	// needs to be done after DB fetch of
	// namespace since the fetch inserts remote
	// inserted namespaces into cache with default age
	suite.cache.enableCqlRefresh("remoteNS")

	nsIter.Reset()
	wsIter.Reset()

	wsCount, err2 := suite.common.wsdb.NumWorkspaces("remoteNS")
	suite.Require().NoError(err2, "NumWorkspaces failed: %s", err2)
	suite.Require().Equal(2, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	nsIter.Reset()
	wsIter.Reset()

	// cache must now contain
	// namespaces: _null, remoteNS
	// workspaces for remoteNS: remoteWS

	// mock workspace DB fetch returning no workspaces for "remoteNS" namespace
	wsRows = mockDbRows{{"remoteWS2"}}
	wsVals = []interface{}{"remoteNS"}
	wsIter.SetRows(wsRows)

	suite.cache.enableCqlRefresh("remoteNS")

	wsList, err3 := suite.common.wsdb.WorkspaceList("remoteNS")
	suite.Require().NoError(err3, "WorkspaceList failed: %s", err3)
	suite.Require().Equal([]string{"remoteWS2"}, wsList,
		"Incorrect workspaces in cache")

}

func (suite *wsdbCacheTestSuite) TestCacheAfterBranching() {

	// newly branched workspace must be in cache
	mockBranchWorkspace(suite.common.mockSess, "_null", "null",
		"test", "a", []byte(nil), gocql.ErrNotFound)

	err := suite.common.wsdb.BranchWorkspace("_null", "null", "test", "a")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, "test")

	nsCount, err1 := suite.common.wsdb.NumNamespaces()
	suite.Require().NoError(err1, "NumNamespaces failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)
	nsList, err2 := suite.common.wsdb.NamespaceList()
	suite.Require().NoError(err2, "NamespaceList failed: %s", err2)
	// use Contains since order of elements within a list can change
	// as map traversal doesn't ensure order
	suite.Require().Contains(nsList, "_null",
		"Incorrect namespaces in cache")
	suite.Require().Contains(nsList, "test",
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces("test")
	suite.Require().NoError(err3, "NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)
	wsList, err4 := suite.common.wsdb.WorkspaceList("test")
	suite.Require().NoError(err4, "WorkspaceList failed: %s", err4)
	suite.Require().Equal([]string{"a"}, wsList,
		"Incorrect workspaces in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheConcInsertsRefresh() {

	// trigger a workspace refresh and pause it
	// once it is paused, perform local insert
	// upause the fetch
	// check that cache has remote as well as new local insert
	// even though fetched data didn't include the local insert

	suite.cache.InsertEntities("_null", "specialWS")
	wsRows := mockDbRows{{"null"}, {"specialWS"}}

	wsIter := new(MockIter)
	wsVals := []interface{}{"_null"}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.enableCqlRefresh("_null")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("_null")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for _null
	// namespace
	mockBranchWorkspace(suite.common.mockSess, "_null", "null",
		"_null", "ws1", []byte(nil), gocql.ErrNotFound)

	// causes a local insert of ws1 workspace for _null namespace
	err := suite.common.wsdb.BranchWorkspace("_null", "null", "_null", "ws1")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for workspace API to complete
	wsWg.Wait()

	suite.Require().Contains(wsList, "null",
		"Expected workspace null not in cache")
	suite.Require().Contains(wsList, "ws1",
		"Expected workspace ws1 not in cache")
	suite.Require().Contains(wsList, "specialWS",
		"Expected workspace specialWS not in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheConcDeletesRefresh() {

	// trigger a workspace refresh and pause it
	// once it is paused, perform local delete
	// upause the fetch
	// check that cache has remote as well as new local insert
	// even though fetched data didn't include the local insert

	suite.cache.InsertEntities("_null", "specialWS")
	wsRows := mockDbRows{{"null"}, {"specialWS"}}

	wsIter := new(MockIter)
	wsVals := []interface{}{"_null"}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.enableCqlRefresh("_null")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("_null")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for _null
	// namespace

	// TODO: currently workspaceDB API doesn't contain deletes
	//       so do a cache delete here
	suite.cache.DeleteEntities("_null", "specialWS")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Contains(wsList, "null",
		"Expected workspace null not in cache")
	suite.Require().NotContains(wsList, "specialWS",
		"Unexpected workspace specialWS in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheSameInsDelDuringRefresh() {
	suite.cache.InsertEntities("_null", "specialWS")
	wsRows := mockDbRows{{"null"}, {"specialWS"}}

	wsIter := new(MockIter)
	wsVals := []interface{}{"_null"}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.enableCqlRefresh("_null")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("_null")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for _null
	// namespace
	suite.cache.InsertEntities("_null", "newWS")
	suite.cache.DeleteEntities("_null", "newWS")

	suite.cache.DeleteEntities("_null", "specialWS")
	suite.cache.InsertEntities("_null", "specialWS")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Contains(wsList, "null",
		"Expected workspace null not in cache")
	suite.Require().Contains(wsList, "specialWS",
		"Unexpected workspace specialWS in cache")
	suite.Require().NotContains(wsList, "newWS",
		"Unexpected workspace newWS in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheGroupDeleteDuringRefresh() {

	mockBranchWorkspace(suite.common.mockSess, "_null", "null",
		"test", "a", []byte(nil), gocql.ErrNotFound)

	mockBranchWorkspace(suite.common.mockSess, "_null", "null",
		"test", "b", []byte(nil), gocql.ErrNotFound)

	err := suite.common.wsdb.BranchWorkspace("_null", "null", "test", "a")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	err = suite.common.wsdb.BranchWorkspace("_null", "null", "test", "b")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	wsRows := mockDbRows{{"a"}, {"b"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"test"}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.enableCqlRefresh("test")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("test")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete all workspace within test namespace which
	// also deletes the test namespace itself
	suite.cache.DeleteEntities("test", "a")
	suite.cache.DeleteEntities("test", "b")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Equal(0, len(wsList),
		"workspace list in cache is not empty")
}

func (suite *wsdbCacheTestSuite) TestCacheParentDeleteDuringRefresh() {
	// while refresh is underway as part of getting list of
	// workspaces for "_null" namespace, delete "_null" namespace.
	// The list of workspaces must be empty due to parent detachment
	// detection
	mockBranchWorkspace(suite.common.mockSess, "_null", "null",
		"test", "a", []byte(nil), gocql.ErrNotFound)

	err := suite.common.wsdb.BranchWorkspace("_null", "null", "test", "a")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	wsRows := mockDbRows{{"null"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"_null"}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.enableCqlRefresh("_null")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("_null")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete all namespaces
	suite.cache.DeleteEntities("_null")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Equal(0, len(wsList),
		"workspace list in cache is not empty")
}

// TODO: ancestor delete is possible when type is introduced in wsdb
// TODO: child delete is possible when type is introduced in wsdb
// TODO: once the APIs return errors, add appropriate test cases

func (suite *wsdbCacheTestSuite) TearDownTest() {
	resetCqlStore()
}

func (suite *wsdbCacheTestSuite) TearDownSuite() {
}

func TestWsdbCacheUnitTests(t *testing.T) {
	suite.Run(t, new(wsdbCacheTestSuite))
}
