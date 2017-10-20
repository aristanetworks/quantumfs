// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"errors"
	"io/ioutil"
	"os"
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

func (suite *wsdbCacheTestSuite) TestWsdbConfigDefault() {
	var config Config
	var config2 *Config

	config.Cluster.Nodes = []string{"node1", "node2"}
	// no wsdb configuration

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	suite.Require().NoError(err, "Tempfile creation failed")
	name := file.Name()
	file.Close()
	defer os.Remove(name)

	err = writeCqlConfig(name, &config)
	suite.Require().NoError(err, "CQL config file write failed")

	config2, err = readCqlConfig(name)
	suite.Require().NoError(err, "CQL config file read failed")

	mockCluster := new(MockCluster)
	mockCluster.On("CreateSession").Return(suite.common.mockSess, nil)

	noCacheWsdb, err := newNoCacheWsdb(mockCluster, config2)
	suite.Require().NoError(err, "Failed %q newNoCacheWsdb", err)

	wdb := newCacheWsdb(noCacheWsdb, config2.WsDB)
	cwsdb, ok := wdb.(*cacheWsdb)
	suite.Require().True(ok, "Incorrect type from newCacheWsdb")
	suite.Require().True(cwsdb.cache.expiryDuration ==
		time.Duration(defaultCacheTimeoutSecs)*time.Second,
		"bad default found %s", cwsdb.cache.expiryDuration)

}

func (suite *wsdbCacheTestSuite) TestCacheBadConfig() {
	timeout := -100

	mockCfg := &Config{
		Cluster: ClusterConfig{
			KeySpace: "ether",
		},
		WsDB: WsDBConfig{
			CacheTimeoutSecs: timeout,
		},

	}

	mockCluster := new(MockCluster)
	noCacheWsdb, err := newNoCacheWsdb(mockCluster, mockCfg)
	suite.Require().NoError(err, "Failed %q newNoCacheWsdb", err)

	suite.Require().Panics(func() {
		newCacheWsdb(noCacheWsdb, mockCfg.WsDB)
	})
}

func (suite *wsdbCacheTestSuite) TestCacheWithoutExpiry() {
	timeout := DontExpireWsdbCache

	mockCluster := new(MockCluster)
	mockCluster.On("CreateSession").Return(suite.common.mockSess, nil)

	mockCfg := &Config{
		Cluster: ClusterConfig{
			KeySpace: "ether",
		},
		WsDB: WsDBConfig{
			CacheTimeoutSecs: timeout,
		},

	}

	noCacheWsdb, err := newNoCacheWsdb(mockCluster, mockCfg)
	suite.Require().NoError(err, "Failed %q newNoCacheWsdb", err)

	wdb := newCacheWsdb(noCacheWsdb, mockCfg.WsDB)
	cwsdb, ok := wdb.(*cacheWsdb)
	suite.Require().True(ok, "Incorrect type from newCacheWsdb")

	// check that cache refresh is disabled
	suite.Require().True(cwsdb.cache.neverExpires,
		"Cache expiry enabled even when explicity disabled")
	cwsdb.cache.InsertEntities(unitTestEtherCtx, "ts1")
	group := cwsdb.cache.getLastEntityGroup(unitTestEtherCtx, cwsdb.cache.root)
	suite.Require().True(group != nil, "group not found after insert")

	// check that refresh is not needed for this group
	suite.Require().False(group.refreshNeeded(unitTestEtherCtx),
		"group need refresh when cache expiry disabled")

	time1 := group.expiresAt

	// in this test we check refresh of typespaces only
	// so disable refresh of workspace and namespace
	// namespaces and workspaces are also entityGroups and hence
	// separate test isn't needed to test for their expiry
	cwsdb.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, wsdb.NullSpaceName,
		wsdb.NullSpaceName)

	cwsdb.cache.CountEntities(unitTestEtherCtx)
	group = cwsdb.cache.getLastEntityGroup(unitTestEtherCtx, cwsdb.cache.root)
	suite.Require().True(group != nil, "group not found after Count")

	// check that refresh is not needed for this group
	suite.Require().False(group.refreshNeeded(unitTestEtherCtx),
		"group need refresh when cache expiry disabled")

	time2 := group.expiresAt

	// check that there is no change in expiryTime of the group
	suite.Require().Equal(time1, time2,
		"unexpected expiry times, time1: %s time2: %s", time1, time2)

}

func (suite *wsdbCacheTestSuite) TestCacheTimeout() {

	timeout := 120
	timeoutSecs := time.Duration(timeout) * time.Second

	mockCluster := new(MockCluster)
	mockCluster.On("CreateSession").Return(suite.common.mockSess, nil)

	// setup typespace info in mockDB
	tsRows := mockDbRows{[]interface{}{"ts1"}, []interface{}{wsdb.NullSpaceName}}
	tsIter := new(MockIter)
	tsVals := []interface{}(nil)
	mockWsdbCacheTypespaceFetch(suite.common.mockSess, tsRows, tsVals,
		tsIter, nil)

	mockCfg := &Config{
		Cluster: ClusterConfig{
			KeySpace: "ether",
		},
		WsDB: WsDBConfig{
			CacheTimeoutSecs: timeout,
		},

	}

	noCacheWsdb, err := newNoCacheWsdb(mockCluster, mockCfg)
	suite.Require().NoError(err, "Failed %q newNoCacheWsdb", err)

	wdb := newCacheWsdb(noCacheWsdb, mockCfg.WsDB)
	cwsdb, ok := wdb.(*cacheWsdb)
	suite.Require().True(ok, "Incorrect type from newCacheWsdb")

	// check that cache is setup with expiry as per configuration
	suite.Require().True(cwsdb.cache.expiryDuration == timeoutSecs,
		"Configured timeout %s, actual %s", timeoutSecs, cwsdb.cache.expiryDuration)

	var time1, time2 time.Time
	// create an entity group with expired entity
	cwsdb.cache.InsertEntities(unitTestEtherCtx, "ts1")
	group := cwsdb.cache.getLastEntityGroup(unitTestEtherCtx, cwsdb.cache.root)
	suite.Require().True(group != nil, "group not found after insert")
	time1 = group.expiresAt

	// in this test we check refresh of typespaces only
	// so disable refresh of workspace and namespace
	// namespaces and workspaces are also entityGroups and hence
	// separate test isn't needed to test for their expiry
	cwsdb.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, wsdb.NullSpaceName,
		wsdb.NullSpaceName)

	// causes refresh and next expiry to be setup based on cache timeout
	cwsdb.cache.CountEntities(unitTestEtherCtx)
	group = cwsdb.cache.getLastEntityGroup(unitTestEtherCtx, cwsdb.cache.root)
	suite.Require().True(group != nil, "group not found after Count")
	time2 = group.expiresAt

	// check that the expiry setup during refresh is >= timeout
	expiry := time2.Sub(time1)
	suite.Require().True(expiry >= timeoutSecs, "unexpected expiry %s found", expiry)
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
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, wsdb.NullSpaceName)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, wsdb.NullSpaceName,
		wsdb.NullSpaceName)

	nsCount, err1 := suite.common.wsdb.NumNamespaces(unitTestEtherCtx,
		wsdb.NullSpaceName)
	suite.Require().NoError(err1,
		"NumNamespaces failed: %s", err1)
	suite.Require().Equal(1, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	nsList, err2 := suite.common.wsdb.NamespaceList(unitTestEtherCtx,
		wsdb.NullSpaceName)
	suite.Require().NoError(err2,
		"NamespaceList failed: %s", err2)
	suite.Require().Contains(nsList, wsdb.NullSpaceName,
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces(unitTestEtherCtx,
		wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	suite.Require().NoError(err3,
		"NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	wsList, err4 := suite.common.wsdb.WorkspaceList(unitTestEtherCtx,
		wsdb.NullSpaceName,
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

	suite.cache.enableCqlRefresh(unitTestEtherCtx)

	nsCount, err1 := suite.common.wsdb.NumNamespaces(unitTestEtherCtx, "remoteTS")
	suite.Require().NoError(err1, "NumNamespace failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	// needs to be done after DB fetch of
	// namespace since the fetch inserts remote
	// inserted namespaces into cache with default age
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "remoteTS")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	nsList, err2 := suite.common.wsdb.NamespaceList(unitTestEtherCtx, "remoteTS")
	suite.Require().NoError(err2, "NamespaceList failed: %s", err2)
	suite.Require().Contains(nsList, wsdb.NullSpaceName,
		"Incorrect namespaces in cache")
	suite.Require().Contains(nsList, "remoteNS",
		"Incorrect namespaces in cache")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	wsCount, err3 := suite.common.wsdb.NumWorkspaces(unitTestEtherCtx, "remoteTS", "remoteNS")
	suite.Require().NoError(err3, "NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	wsList, err4 := suite.common.wsdb.WorkspaceList(unitTestEtherCtx, "remoteTS", "remoteNS")
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

	suite.cache.enableCqlRefresh(unitTestEtherCtx)

	nsCount, err1 := suite.common.wsdb.NumNamespaces(unitTestEtherCtx, "remoteTS")
	suite.Require().NoError(err1, "NumNamespaces failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	// needs to be done after DB fetch of
	// namespace since the fetch inserts remote
	// inserted namespaces into cache with default age
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "remoteTS")
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "remoteTS", "remoteNS")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	wsCount, err2 := suite.common.wsdb.NumWorkspaces(unitTestEtherCtx, "remoteTS",
		"remoteNS")
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

	suite.cache.enableCqlRefresh(unitTestEtherCtx, "remoteTS")
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "remoteTS", "remoteNS")

	wsList, err3 := suite.common.wsdb.WorkspaceList(unitTestEtherCtx, "remoteTS",
		"remoteNS")
	suite.Require().NoError(err3, "WorkspaceList failed: %s", err3)
	suite.Require().Equal([]string{"remoteWS2"}, wsList,
		"Incorrect workspaces in cache")

}

func (suite *wsdbCacheTestSuite) TestCacheAfterBranching() {

	// newly branched workspace must be in cache
	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1",
		[]byte(nil), gocql.ErrNotFound)
	err := suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws2",
		[]byte(nil), gocql.ErrNotFound)
	err = suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws2")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, "ts1")
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, "ts1", "ns1")

	nsCount, err1 := suite.common.wsdb.NumNamespaces(unitTestEtherCtx, "ts1")
	suite.Require().NoError(err1, "NumNamespaces failed: %s", err1)
	suite.Require().Equal(1, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)
	nsList, err2 := suite.common.wsdb.NamespaceList(unitTestEtherCtx, "ts1")
	suite.Require().NoError(err2, "NamespaceList failed: %s", err2)
	// use Contains since order of elements within a list can change
	// as map traversal doesn't ensure order
	suite.Require().Contains(nsList, "ns1",
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces(unitTestEtherCtx, "ts1", "ns1")
	suite.Require().NoError(err3, "NumWorkspaces failed: %s", err3)
	suite.Require().Equal(2, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)
	wsList, err4 := suite.common.wsdb.WorkspaceList(unitTestEtherCtx, "ts1", "ns1")
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

	suite.cache.InsertEntities(unitTestEtherCtx, "ts1", "ns1", "specialWS")

	wsRows := mockDbRows{{"specialWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts1", "ns1"}

	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, "ts1")
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "ts1", "ns1")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList(unitTestEtherCtx, "ts1", "ns1")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for the null
	// namespace
	mockBranchWorkspace(suite.common.mockSess, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1",
		[]byte(nil), gocql.ErrNotFound)

	// causes a local insert of ws1 workspace for the null namespace
	err := suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
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

	suite.cache.InsertEntities(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "specialWS")

	wsRows := mockDbRows{{wsdb.NullSpaceName}, {"specialWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{wsdb.NullSpaceName, wsdb.NullSpaceName}

	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, wsdb.NullSpaceName)
	suite.cache.enableCqlRefresh(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName)

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList(unitTestEtherCtx, wsdb.NullSpaceName,
			wsdb.NullSpaceName)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for the null
	// namespace

	// TODO: currently workspaceDB API doesn't contain deletes
	//       so do a cache delete here
	suite.cache.DeleteEntities(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
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
	suite.cache.InsertEntities(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "specialWS")

	wsRows := mockDbRows{{wsdb.NullSpaceName}, {"specialWS"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{wsdb.NullSpaceName, wsdb.NullSpaceName}
	var wsList []string
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, wsdb.NullSpaceName)
	suite.cache.enableCqlRefresh(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName)

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList(unitTestEtherCtx,
			wsdb.NullSpaceName, wsdb.NullSpaceName)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for null
	// namespace
	suite.cache.InsertEntities(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "newWS")
	suite.cache.DeleteEntities(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "newWS")

	suite.cache.DeleteEntities(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "specialWS")
	suite.cache.InsertEntities(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "specialWS")

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

	err := suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts2", "ns2", "a")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	err = suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
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

	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, "ts2")
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "ts2", "ns2")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList(unitTestEtherCtx, "ts2", "ns2")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete all workspace within test namespace which
	// also deletes the test namespace itself
	suite.cache.DeleteEntities(unitTestEtherCtx, "ts2", "ns2", "a")
	suite.cache.DeleteEntities(unitTestEtherCtx, "ts2", "ns2", "b")

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

	err := suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
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

	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, "ts")
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "ts", "parentNS")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList(unitTestEtherCtx, "ts", "parentNS")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete null namespace
	suite.cache.DeleteEntities(unitTestEtherCtx, "ts", "parentNS")

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

	err := suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
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

	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, "ts")
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "ts", "parentNS")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		wsList, _ = suite.common.wsdb.WorkspaceList(unitTestEtherCtx, "ts",
			"parentNS")
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete null typespace (and all children)
	suite.cache.DeleteEntities(unitTestEtherCtx, "ts")

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

	err := suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
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

	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.enableCqlRefresh(unitTestEtherCtx, "ts")

	nsWg.Add(1)
	go func() {
		defer nsWg.Done()
		nsList, _ = suite.common.wsdb.NamespaceList(unitTestEtherCtx, "ts")
	}()

	// wait for fetch to stall
	<-nsFetchPause

	// delete childWS which causes parentNS to also get
	// deleted
	suite.cache.DeleteEntities(unitTestEtherCtx, "ts", "parentNS", "childWS")

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

func (suite *wsdbCacheTestSuite) TestCacheWorkspaceLastWriteTime() {
	suite.common.TestWorkspaceLastWriteTime()
}

func (suite *wsdbCacheTestSuite) TestPanicDuringFetch() {
	mockWsdbCacheTypespaceFetchPanic(suite.common.mockSess)

	// force the typespace fetch to be invoked
	suite.cache.enableCqlRefresh(unitTestEtherCtx)

	// Don't use Require().Panics(f) test so that reason for
	// panic can be verified.
	defer func() {
		ex := recover()
		suite.Require().NotNil(ex, "expected panic, didn't occur")
		suite.Require().Equal("PanicOnFetch", ex,
			"actual panic: %v", ex)
	}()
	suite.common.wsdb.NumTypespaces(unitTestEtherCtx)
}

// TestRefreshFailure tests if the error in DB refresh
// is properly passed on to the WSDB API
func (suite *wsdbCacheTestSuite) TestRefreshFailure() {
	expectedErr := errors.New("DB error")

	suite.cache.enableCqlRefresh(unitTestEtherCtx)
	mockWsdbCacheTypespaceFetchErr(suite.common.mockSess, expectedErr)
	mockWsdbCacheNamespaceFetchErr(suite.common.mockSess, expectedErr)
	mockWsdbCacheWorkspaceFetchErr(suite.common.mockSess, expectedErr)

	_, err := suite.common.wsdb.NumTypespaces(unitTestEtherCtx)
	suite.Require().Error(err)

	_, err = suite.common.wsdb.TypespaceList(unitTestEtherCtx)
	suite.Require().Error(err)

	_, err = suite.common.wsdb.NumNamespaces(unitTestEtherCtx, "t1")
	suite.Require().Error(err)

	_, err = suite.common.wsdb.NamespaceList(unitTestEtherCtx, "t1")
	suite.Require().Error(err)

	_, err = suite.common.wsdb.NumWorkspaces(unitTestEtherCtx, "t1", "n1")
	suite.Require().Error(err)

	_, err = suite.common.wsdb.WorkspaceList(unitTestEtherCtx, "t1", "n1")
	suite.Require().Error(err)
}

// TestCacheDeleteWorkspaceNumOK tests if the cache is updated
// properly when a workspace is deleted
func (suite *wsdbCacheTestSuite) TestCacheDeleteWorkspaceNumOK() {
	mockBranchWorkspace(suite.common.mockSess,
		wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", "ns1", "ws1", []byte(nil),
		gocql.ErrNotFound)
	err := suite.common.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ts1", "ns1", "ws1")
	suite.Require().NoError(err,
		"Error branching "+wsdb.NullSpaceName+" workspace: %v", err)

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, wsdb.NullSpaceName)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, "ts1")
	suite.cache.disableCqlRefresh(unitTestEtherCtx, 1*time.Hour, "ts1", "ns1")

	// cached APIs
	tsCount, err1 := suite.common.wsdb.NumTypespaces(unitTestEtherCtx)
	suite.Require().NoError(err1, "NumTypespaces failed: %s", err1)
	suite.Require().Equal(2, tsCount,
		"Incorrect number of Typespaces. Exp: 2, Actual: %d", tsCount)

	mockWsdbKeyDel(suite.common.mockSess, "ts1", "ns1", "ws1", nil)
	err = suite.common.wsdb.DeleteWorkspace(unitTestEtherCtx, "ts1", "ns1", "ws1")
	suite.Require().NoError(err, "Error in DeleteWorkspace: %v", err)

	tsCount, err1 = suite.common.wsdb.NumTypespaces(unitTestEtherCtx)
	suite.Require().NoError(err1, "NumTypespaces failed: %s", err1)
	// locally branched workspace should get deleted from cache
	suite.Require().Equal(1, tsCount,
		"Incorrect number of Typespaces. Exp: 1, Actual: %d", tsCount)
}

// TODO: once the APIs return errors, add appropriate test cases

func (suite *wsdbCacheTestSuite) TestCacheCreateWorkspaceNoKey() {
	suite.common.TestCreateWorkspaceNoKey()
}

func (suite *wsdbCacheTestSuite) TestCacheCreateWorkspaceDiffKey() {
	suite.common.TestCreateWorkspaceDiffKey()
}

func (suite *wsdbCacheTestSuite) TestCacheCreateWorkspaceSameKey() {
	suite.common.TestCreateWorkspaceSameKey()
}

func (suite *wsdbCacheTestSuite) TearDownTest() {
	resetCqlStore()
}

func (suite *wsdbCacheTestSuite) TearDownSuite() {
}

func TestWsdbCacheUnitTests(t *testing.T) {
	suite.Run(t, new(wsdbCacheTestSuite))
}
