// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"sync"
	"testing"
	"time"

	"github.com/aristanetworks/ether/qubit/wsdb"
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

	mockWsdbKeyPut(mockSession, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte(nil), nil)

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

func (suite *wsdbCacheTestSuite) TestCacheTypespaceNotExist() {
	suite.common.TestTypespaceNotExist()
}

func (suite *wsdbCacheTestSuite) TestCacheAfterEmptyDB() {

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, wsdb.NullSpaceName)
	suite.cache.disableCqlRefresh(1*time.Hour, wsdb.NullSpaceName,
		wsdb.NullSpaceName)

	nsCount, err1 := suite.common.wsdb.NumNamespaces(wsdb.NullSpaceName)
	suite.Require().NoError(err1,
		"NumNamespaces failed: %s", err1)
	suite.Require().Equal(1, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	nsList, err2 := suite.common.wsdb.NamespaceList(wsdb.NullSpaceName)
	suite.Require().NoError(err2,
		"NamespaceList failed: %s", err2)
	suite.Require().Contains(nsList, wsdb.NullSpaceName,
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces(wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	suite.Require().NoError(err3,
		"NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	wsList, err4 := suite.common.wsdb.WorkspaceList(wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	suite.Require().NoError(err4,
		"WorkspaceList failed: %s", err4)
	suite.Require().Equal([]string{wsdb.NullSpaceName}, wsList,
		"Incorrect workspaces in cache")

}

func (suite *wsdbCacheTestSuite) TestCacheWithRemoteInserts() {
	// test if remotely inserted namespace and workspaces
	// are seen in the cache

	tsRows := mockDbRows{[]interface{}{"remoteTS"}}
	tsIter := new(MockIter)
	tsVals := []interface{}(nil)

	nsRows := mockDbRows{[]interface{}{wsdb.NullSpaceName},
		[]interface{}{"remoteNS"}}
	nsIter := new(MockIter)
	nsVals := []interface{}{"remoteTS"}

	wsRows := mockDbRows{{"remoteWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"remoteTS", "remoteNS"}

	mockWsdbCacheTypespaceFetch(suite.common.mockSess, tsRows, tsVals,
		tsIter, nil)
	mockWsdbCacheNamespaceFetch(suite.common.mockSess, nsRows, nsVals,
		nsIter, nil)
	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, nil)

	suite.cache.enableCqlRefresh()

	nsCount, err1 := suite.common.wsdb.NumNamespaces("remoteTS")
	suite.Require().NoError(err1, "NumNamespace failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	// needs to be done after DB fetch of
	// namespace since the fetch inserts remote
	// inserted namespaces into cache with default age
	suite.cache.enableCqlRefresh("remoteTS")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	nsList, err2 := suite.common.wsdb.NamespaceList("remoteTS")
	suite.Require().NoError(err2, "NamespaceList failed: %s", err2)
	suite.Require().Contains(nsList, wsdb.NullSpaceName,
		"Incorrect namespaces in cache")
	suite.Require().Contains(nsList, "remoteNS",
		"Incorrect namespaces in cache")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	wsCount, err3 := suite.common.wsdb.NumWorkspaces("remoteTS", "remoteNS")
	suite.Require().NoError(err3, "NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	wsList, err4 := suite.common.wsdb.WorkspaceList("remoteTS", "remoteNS")
	suite.Require().NoError(err4, "WorkspaceList failed: %s", err4)
	suite.Require().Equal([]string{"remoteWS"}, wsList,
		"Incorrect workspaces in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheWithRemoteDeletes() {
	// test if remotely deleted namespace and workspaces
	// drain out from the cache

	tsRows := mockDbRows{[]interface{}{"remoteTS"}}
	tsIter := new(MockIter)
	tsVals := []interface{}(nil)

	// first ensure that remote entries are injected into cache
	nsRows := mockDbRows{[]interface{}{wsdb.NullSpaceName},
		[]interface{}{"remoteNS"}}
	nsIter := new(MockIter)
	nsVals := []interface{}{"remoteTS"}

	wsRows := mockDbRows{{"remoteWS1"}, {"remoteWS2"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"remoteTS", "remoteNS"}

	mockWsdbCacheTypespaceFetch(suite.common.mockSess, tsRows, tsVals,
		tsIter, nil)
	mockWsdbCacheNamespaceFetch(suite.common.mockSess, nsRows, nsVals,
		nsIter, nil)
	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, nil)

	suite.cache.enableCqlRefresh()

	nsCount, err1 := suite.common.wsdb.NumNamespaces("remoteTS")
	suite.Require().NoError(err1, "NumNamespaces failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	// needs to be done after DB fetch of
	// namespace since the fetch inserts remote
	// inserted namespaces into cache with default age
	suite.cache.enableCqlRefresh("remoteTS")
	suite.cache.enableCqlRefresh("remoteTS", "remoteNS")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	wsCount, err2 := suite.common.wsdb.NumWorkspaces("remoteTS", "remoteNS")
	suite.Require().NoError(err2, "NumWorkspaces failed: %s", err2)
	suite.Require().Equal(2, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	// cache must now contain
	// typespaces: remoteTS
	// namespaces: null, remoteNS
	// workspaces for remoteNS: remoteWS

	// mock workspace DB fetch returning no workspaces for "remoteNS" namespace
	wsRows = mockDbRows{{"remoteWS2"}}
	wsVals = []interface{}{"remoteTS", "remoteNS"}
	wsIter.SetRows(wsRows)

	suite.cache.enableCqlRefresh("remoteTS")
	suite.cache.enableCqlRefresh("remoteTS", "remoteNS")

	wsList, err3 := suite.common.wsdb.WorkspaceList("remoteTS", "remoteNS")
	suite.Require().NoError(err3, "WorkspaceList failed: %s", err3)
	suite.Require().Equal([]string{"remoteWS2"}, wsList,
		"Incorrect workspaces in cache")

}

func (suite *wsdbCacheTestSuite) TestCacheAfterBranching() {

	// newly branched workspace must be in cache
	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1",
		[]byte(nil), gocql.ErrNotFound)
	err := suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws2",
		[]byte(nil), gocql.ErrNotFound)
	err = suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws2")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, "ts1")
	suite.cache.disableCqlRefresh(1*time.Hour, "ts1", "ns1")

	nsCount, err1 := suite.common.wsdb.NumNamespaces("ts1")
	suite.Require().NoError(err1, "NumNamespaces failed: %s", err1)
	suite.Require().Equal(1, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)
	nsList, err2 := suite.common.wsdb.NamespaceList("ts1")
	suite.Require().NoError(err2, "NamespaceList failed: %s", err2)
	// use Contains since order of elements within a list can change
	// as map traversal doesn't ensure order
	suite.Require().Contains(nsList, "ns1",
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces("ts1", "ns1")
	suite.Require().NoError(err3, "NumWorkspaces failed: %s", err3)
	suite.Require().Equal(2, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)
	wsList, err4 := suite.common.wsdb.WorkspaceList("ts1", "ns1")
	suite.Require().NoError(err4, "WorkspaceList failed: %s", err4)
	suite.Require().Contains(wsList, "ws1",
		"Incorrect workspace in cache")
	suite.Require().Contains(wsList, "ws2",
		"Incorrect workspace in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheConcInsertsRefresh() {

	// trigger a workspace refresh and pause it
	// once it is paused, perform local insert
	// upause the fetch
	// check that cache has remote as well as new local insert
	// even though fetched data didn't include the local insert

	suite.cache.InsertEntities("ts1", "ns1", "specialWS")

	wsRows := mockDbRows{{"specialWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts1", "ns1"}

	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, "ts1")
	suite.cache.enableCqlRefresh("ts1", "ns1")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("ts1", "ns1")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for the null
	// namespace
	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1",
		[]byte(nil), gocql.ErrNotFound)

	// causes a local insert of ws1 workspace for the null namespace
	err := suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for workspace API to complete
	wsWg.Wait()

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

	suite.cache.InsertEntities(wsdb.NullSpaceName, wsdb.NullSpaceName,
		"specialWS")

	wsRows := mockDbRows{{wsdb.NullSpaceName}, {"specialWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{wsdb.NullSpaceName, wsdb.NullSpaceName}

	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, wsdb.NullSpaceName)
	suite.cache.enableCqlRefresh(wsdb.NullSpaceName, wsdb.NullSpaceName)

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList(wsdb.NullSpaceName,
			wsdb.NullSpaceName)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for the null
	// namespace

	// TODO: currently workspaceDB API doesn't contain deletes
	//       so do a cache delete here
	suite.cache.DeleteEntities(wsdb.NullSpaceName, wsdb.NullSpaceName,
		"specialWS")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Contains(wsList, wsdb.NullSpaceName,
		"Expected null workspace not in cache")
	suite.Require().NotContains(wsList, "specialWS",
		"Unexpected workspace specialWS in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheSameInsDelDuringRefresh() {
	suite.cache.InsertEntities(wsdb.NullSpaceName, wsdb.NullSpaceName,
		"specialWS")

	wsRows := mockDbRows{{wsdb.NullSpaceName}, {"specialWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{wsdb.NullSpaceName, wsdb.NullSpaceName}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, wsdb.NullSpaceName)
	suite.cache.enableCqlRefresh(wsdb.NullSpaceName, wsdb.NullSpaceName)

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList(wsdb.NullSpaceName,
			wsdb.NullSpaceName)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for null
	// namespace
	suite.cache.InsertEntities(wsdb.NullSpaceName, wsdb.NullSpaceName, "newWS")
	suite.cache.DeleteEntities(wsdb.NullSpaceName, wsdb.NullSpaceName, "newWS")

	suite.cache.DeleteEntities(wsdb.NullSpaceName, wsdb.NullSpaceName,
		"specialWS")
	suite.cache.InsertEntities(wsdb.NullSpaceName, wsdb.NullSpaceName,
		"specialWS")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Contains(wsList, wsdb.NullSpaceName,
		"Expected null workspace not in cache")
	suite.Require().Contains(wsList, "specialWS",
		"Unexpected workspace specialWS in cache")
	suite.Require().NotContains(wsList, "newWS",
		"Unexpected workspace newWS in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheGroupDeleteDuringRefresh() {

	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts2", "ns2", "a",
		[]byte(nil), gocql.ErrNotFound)

	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts2", "ns2", "b",
		[]byte(nil), gocql.ErrNotFound)

	err := suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts2", "ns2", "a")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	err = suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts2", "ns2", "b")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	wsRows := mockDbRows{{"a"}, {"b"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts2", "ns2"}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, "ts2")
	suite.cache.enableCqlRefresh("ts2", "ns2")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("ts2", "ns2")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete all workspace within test namespace which
	// also deletes the test namespace itself
	suite.cache.DeleteEntities("ts2", "ns2", "a")
	suite.cache.DeleteEntities("ts2", "ns2", "b")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Equal(0, len(wsList),
		"workspace list in cache is not empty")
}

// TestCacheParentDeleteDuringRefresh tests scenario of
// delete "parentNS" namespace while DB refresh is underway
// as part of getting list of workspaces for "parentNS" namespace
// to ensure that list of workspaces is empty
func (suite *wsdbCacheTestSuite) TestCacheParentDeleteDuringRefresh() {
	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts", "parentNS",
		"childWS", []byte(nil), gocql.ErrNotFound)

	err := suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts", "parentNS",
		"childWS")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	wsRows := mockDbRows{[]interface{}{"childWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts", "parentNS"}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, "ts")
	suite.cache.enableCqlRefresh("ts", "parentNS")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("ts", "parentNS")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete null namespace
	suite.cache.DeleteEntities("ts", "parentNS")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Equal(0, len(wsList),
		"workspace list in cache is not empty")
}

// TestCacheAncestorDeleteDuringRefresh tests the scenario of
// delete "null" typespace while DB refresh is underway as part of
// getting list of workspaces for "parentNS" namespace to
// ensure list of workspaces is empty
func (suite *wsdbCacheTestSuite) TestCacheAncestorDeleteDuringRefresh() {
	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts", "parentNS",
		"childWS", []byte(nil), gocql.ErrNotFound)

	err := suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts", "parentNS",
		"childWS")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	wsRows := mockDbRows{[]interface{}{"childWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts", "parentNS"}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, "ts")
	suite.cache.enableCqlRefresh("ts", "parentNS")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList("ts", "parentNS")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete null typespace (and all children)
	suite.cache.DeleteEntities("ts")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Equal(0, len(wsList),
		"workspace list in cache is not empty")
}

// TestCacheChildDeleteDuringRefresh tests the scenario of
// delete "childWS" workspace when DB refresh is
// underway as part of getting list of namespaces for "ts" typespace
// to ensure the list of namespaces is eempty
func (suite *wsdbCacheTestSuite) TestCacheChildDeleteDuringRefresh() {
	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts", "parentNS",
		"childWS", []byte(nil), gocql.ErrNotFound)

	err := suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts", "parentNS",
		"childWS")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	nsRows := mockDbRows{[]interface{}{"parentNS"}}
	nsIter := new(MockIter)
	nsVals := []interface{}{"ts"}
	var nsList []string
	var nsWg sync.WaitGroup
	nsFetchPause := make(chan bool)

	mockWsdbCacheNamespaceFetch(suite.common.mockSess, nsRows, nsVals,
		nsIter, nsFetchPause)

	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.enableCqlRefresh("ts")

	nsWg.Add(1)
	go func() {
		defer nsWg.Done()
		nsList, _ = suite.common.wsdb.NamespaceList("ts")
	}()

	// wait for fetch to stall
	<-nsFetchPause

	// delete childWS which causes parentNS to also get
	// deleted
	suite.cache.DeleteEntities("ts", "parentNS", "childWS")

	// unpause the DB fetch
	nsFetchPause <- true

	// wait for NamespaceList API to complete
	nsWg.Wait()

	suite.Require().Equal(0, len(nsList),
		"namespace list has elements")
}

func (suite *wsdbCacheTestSuite) TestCacheLockedBranching() {
	suite.common.TestLockedBranchWorkspace()
}

func (suite *wsdbCacheTestSuite) TestCacheLockedAdvance() {
	suite.common.TestLockedAdvanceWorkspace()
}

func (suite *wsdbCacheTestSuite) TestCacheInitialAdvanceWorkspace() {
	suite.common.TestInitialAdvanceWorkspace()
}

func (suite *wsdbCacheTestSuite) TestCacheDeleteNullTypespace() {
	suite.common.TestDeleteNullTypespace()
}

func (suite *wsdbCacheTestSuite) TestCacheDeleteWorkspaceOK() {
	suite.common.TestDeleteWorkspaceOK()
}

// TestCacheDeleteWorkspaceNumOK tests if the cache is updated
// properly when a workspace is deleted
func (suite *wsdbCacheTestSuite) TestCacheDeleteWorkspaceNumOK() {
	mockBranchWorkspace(suite.common.mockSess,
		wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", "ns1", "ws1", []byte(nil),
		gocql.ErrNotFound)
	err := suite.common.wsdb.BranchWorkspace(wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1")
	suite.Require().NoError(err,
		"Error branching "+wsdb.NullSpaceName+" workspace: %v", err)

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(1 * time.Hour)
	suite.cache.disableCqlRefresh(1*time.Hour, wsdb.NullSpaceName)
	suite.cache.disableCqlRefresh(1*time.Hour, wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	suite.cache.disableCqlRefresh(1*time.Hour, "ts1")
	suite.cache.disableCqlRefresh(1*time.Hour, "ts1", "ns1")

	// cached APIs
	tsCount, err1 := suite.common.wsdb.NumTypespaces()
	suite.Require().NoError(err1, "NumTypespaces failed: %s", err1)
	suite.Require().Equal(2, tsCount,
		"Incorrect number of Typespaces. Exp: 2, Actual: %d", tsCount)

	mockWsdbKeyDel(suite.common.mockSess, "ts1", "ns1", "ws1", nil)
	err = suite.common.wsdb.DeleteWorkspace("ts1", "ns1", "ws1")
	suite.Require().NoError(err, "Error in DeleteWorkspace: %v", err)

	tsCount, err1 = suite.common.wsdb.NumTypespaces()
	suite.Require().NoError(err1, "NumTypespaces failed: %s", err1)
	// locally branched workspace should get deleted from cache
	suite.Require().Equal(1, tsCount,
		"Incorrect number of Typespaces. Exp: 1, Actual: %d", tsCount)
}

// TODO: once the APIs return errors, add appropriate test cases

func (suite *wsdbCacheTestSuite) TearDownTest() {
	resetCqlStore()
}

func (suite *wsdbCacheTestSuite) TearDownSuite() {
}

func TestWsdbCacheUnitTests(t *testing.T) {
	suite.Run(t, new(wsdbCacheTestSuite))
}
