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

	"github.com/gocql/gocql"
	mock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type wsdbCacheTestSuite struct {
	suite.Suite
	common *wsdbCommonUnitTest
	cache  *entityCache
	cfg    *Config
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

	mockSchemaOk(mockSession, "etherwsdb", "workspacedb", nil)

	mockCfg := setupMockConfig()
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
		cfg:      mockCfg,
	}
}

func (suite *wsdbCacheTestSuite) TestWsdbConfigDefault() {
	var config2 *Config

	config := *suite.common.cfg
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

	mockCfg := *suite.common.cfg
	mockCfg.WsDB.CacheTimeoutSecs = timeout

	mockCluster := new(MockCluster)
	noCacheWsdb, err := newNoCacheWsdb(mockCluster, &mockCfg)
	suite.Require().NoError(err, "Failed %q newNoCacheWsdb", err)

	suite.Require().Panics(func() {
		newCacheWsdb(noCacheWsdb, mockCfg.WsDB)
	})
}

func (suite *wsdbCacheTestSuite) TestCacheWithoutExpiry() {
	timeout := DontExpireWsdbCache

	mockCluster := new(MockCluster)
	mockCluster.On("CreateSession").Return(suite.common.mockSess, nil)

	mockCfg := *suite.common.cfg
	mockCfg.WsDB.CacheTimeoutSecs = timeout

	noCacheWsdb, err := newNoCacheWsdb(mockCluster, &mockCfg)
	suite.Require().NoError(err, "Failed %q newNoCacheWsdb", err)

	wdb := newCacheWsdb(noCacheWsdb, mockCfg.WsDB)
	cwsdb, ok := wdb.(*cacheWsdb)
	suite.Require().True(ok, "Incorrect type from newCacheWsdb")

	// check that cache refresh is disabled
	suite.Require().True(cwsdb.cache.neverExpires,
		"Cache expiry enabled even when explicity disabled")
	cwsdb.cache.InsertEntities(unitTestCqlCtx, "ts1")
	group := cwsdb.cache.getLastEntityGroup(unitTestCqlCtx, cwsdb.cache.root)
	suite.Require().True(group != nil, "group not found after insert")

	// check that refresh is not needed for this group
	action := group.refreshNeeded(unitTestCqlCtx)
	suite.Require().Equal(int(refreshIgnore), int(action),
		"incorrect refresh action %d when cache expiry disabled",
		action)

	time1 := group.expiresAt

	// in this test we check refresh of typespaces only
	// so disable refresh of workspace and namespace
	// namespaces and workspaces are also entityGroups and hence
	// separate test isn't needed to test for their expiry
	cwsdb.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, NullSpaceName,
		NullSpaceName)

	cwsdb.cache.CountEntities(unitTestCqlCtx)
	group = cwsdb.cache.getLastEntityGroup(unitTestCqlCtx, cwsdb.cache.root)
	suite.Require().True(group != nil, "group not found after Count")

	// check that refresh is not needed for this group
	action = group.refreshNeeded(unitTestCqlCtx)
	suite.Require().Equal(int(refreshIgnore), int(action),
		"incorrect refresh action %d when cache expiry disabled",
		action)

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
	tsRows := mockDbRows{[]interface{}{"ts1"}, []interface{}{NullSpaceName}}
	tsIter := new(MockIter)
	tsVals := []interface{}(nil)
	mockWsdbCacheTypespaceFetch(suite.common.mockSess, tsRows, tsVals,
		tsIter, nil)

	mockCfg := *suite.common.cfg
	mockCfg.WsDB.CacheTimeoutSecs = timeout

	noCacheWsdb, err := newNoCacheWsdb(mockCluster, &mockCfg)
	suite.Require().NoError(err, "Failed %q newNoCacheWsdb", err)

	wdb := newCacheWsdb(noCacheWsdb, mockCfg.WsDB)
	cwsdb, ok := wdb.(*cacheWsdb)
	suite.Require().True(ok, "Incorrect type from newCacheWsdb")

	// check that cache is setup with expiry as per configuration
	suite.Require().True(cwsdb.cache.expiryDuration == timeoutSecs,
		"Configured timeout %s, actual %s", timeoutSecs,
		cwsdb.cache.expiryDuration)

	var time1, time2 time.Time
	// create an entity group with expired entity
	cwsdb.cache.InsertEntities(unitTestCqlCtx, "ts1")
	group := cwsdb.cache.getLastEntityGroup(unitTestCqlCtx, cwsdb.cache.root)
	suite.Require().True(group != nil, "group not found after insert")
	time1 = group.expiresAt

	// in this test we check refresh of typespaces only
	// so disable refresh of workspace and namespace
	// namespaces and workspaces are also entityGroups and hence
	// separate test isn't needed to test for their expiry
	cwsdb.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, NullSpaceName,
		NullSpaceName)

	// causes refresh and next expiry to be setup based on cache timeout
	cwsdb.cache.CountEntities(unitTestCqlCtx)
	group = cwsdb.cache.getLastEntityGroup(unitTestCqlCtx, cwsdb.cache.root)
	suite.Require().True(group != nil, "group not found after Count")
	time2 = group.expiresAt

	// check that the expiry setup during refresh is >= timeout
	expiry := time2.Sub(time1)
	suite.Require().True(expiry >= timeoutSecs, "unexpected expiry %s found",
		expiry)
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

func (suite *wsdbCacheTestSuite) TestCacheAdvanceOutOfDateKey() {
	suite.common.TestAdvanceOutOfDateKey()
}

func (suite *wsdbCacheTestSuite) TestCacheAdvanceOutOfDateNonce() {
	suite.common.TestAdvanceOutOfDateNonce()
}

func (suite *wsdbCacheTestSuite) TestCacheAdvanceNotExist() {
	suite.common.TestAdvanceNotExist()
}

func (suite *wsdbCacheTestSuite) TestCacheSetWorkspaceImmutable() {
	suite.common.TestSetWorkspaceImmutable()
}

func (suite *wsdbCacheTestSuite) TestCacheSetWorkspaceImmutableError() {
	suite.common.TestSetWorkspaceImmutableError()
}

func (suite *wsdbCacheTestSuite) TestCacheWorkspaceIsImmutable() {
	suite.common.TestWorkspaceIsImmutable()
}

func (suite *wsdbCacheTestSuite) TestCacheWorkspaceIsImmutableError() {
	suite.common.TestWorkspaceIsImmutableError()
}

func (suite *wsdbCacheTestSuite) TestCacheDeleteImmutableSet() {
	suite.common.TestDeleteImmutableSet()
}

func (suite *wsdbCacheTestSuite) TestCacheAfterEmptyDB() {

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, NullSpaceName)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, NullSpaceName,
		NullSpaceName)

	nsCount, err1 := suite.common.wsdb.NumNamespaces(unitTestCqlCtx,
		NullSpaceName)
	suite.Require().NoError(err1,
		"NumNamespaces failed: %s", err1)
	suite.Require().Equal(1, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	nsList, err2 := suite.common.wsdb.NamespaceList(unitTestCqlCtx,
		NullSpaceName)
	suite.Require().NoError(err2,
		"NamespaceList failed: %s", err2)
	suite.Require().Contains(nsList, NullSpaceName,
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces(unitTestCqlCtx,
		NullSpaceName,
		NullSpaceName)
	suite.Require().NoError(err3,
		"NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	wsRows := mockDbRows{{"_", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"_", "_"}
	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, nil)
	mockWsdbKeyGet(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, []byte(nil),
		WorkspaceNonceInvalid, nil)

	wsMap, err4 := suite.common.wsdb.WorkspaceList(unitTestCqlCtx,
		NullSpaceName,
		NullSpaceName)
	suite.Require().NoError(err4,
		"WorkspaceList failed: %s", err4)
	suite.Require().Equal(map[string]WorkspaceNonce{
		NullSpaceName: WorkspaceNonceInvalid}, wsMap,
		"Incorrect workspaces in cache")

}

func (suite *wsdbCacheTestSuite) TestCacheWithRemoteInserts() {
	// test if remotely inserted namespace and workspaces
	// are seen in the cache

	tsRows := mockDbRows{[]interface{}{"remoteTS"}}
	tsIter := new(MockIter)
	tsVals := []interface{}(nil)

	nsRows := mockDbRows{[]interface{}{NullSpaceName},
		[]interface{}{"remoteNS"}}
	nsIter := new(MockIter)
	nsVals := []interface{}{"remoteTS"}

	wsRows := mockDbRows{{"remoteWS", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"remoteTS", "remoteNS"}

	mockWsdbCacheTypespaceFetch(suite.common.mockSess, tsRows, tsVals,
		tsIter, nil)
	mockWsdbCacheNamespaceFetch(suite.common.mockSess, nsRows, nsVals,
		nsIter, nil)
	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, nil)
	nonce := WorkspaceNonce{Id: 5, PublishTime: 0xbeef}
	mockWsdbKeyGet(suite.common.mockSess, "remoteTS", "remoteNS", "remoteWS",
		[]byte(nil), nonce, nil)

	suite.cache.enableCqlRefresh(unitTestCqlCtx)

	nsCount, err1 := suite.common.wsdb.NumNamespaces(unitTestCqlCtx,
		"remoteTS")
	suite.Require().NoError(err1, "NumNamespace failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	// needs to be done after DB fetch of
	// namespace since the fetch inserts remote
	// inserted namespaces into cache with default age
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "remoteTS")

	nsList, err2 := suite.common.wsdb.NamespaceList(unitTestCqlCtx, "remoteTS")
	suite.Require().NoError(err2, "NamespaceList failed: %s", err2)
	suite.Require().Contains(nsList, NullSpaceName,
		"Incorrect namespaces in cache")
	suite.Require().Contains(nsList, "remoteNS",
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces(unitTestCqlCtx,
		"remoteTS", "remoteNS")
	suite.Require().NoError(err3, "NumWorkspaces failed: %s", err3)
	suite.Require().Equal(1, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	wsMap, err4 := suite.common.wsdb.WorkspaceList(unitTestCqlCtx,
		"remoteTS", "remoteNS")
	suite.Require().NoError(err4, "WorkspaceList failed: %s", err4)
	suite.Require().Equal(1, len(wsMap),
		"Incorrect number of workspaces in cache")
	suite.Require().Contains(wsMap, "remoteWS",
		"Incorrect workspaces in cache")
	suite.Require().Equal(nonce, wsMap["remoteWS"],
		"Incorrect nonce value for remoteWS")
}

func (suite *wsdbCacheTestSuite) TestCacheWithRemoteDeletes() {
	// test if remotely deleted namespace and workspaces
	// drain out from the cache

	tsRows := mockDbRows{[]interface{}{"remoteTS"}}
	tsIter := new(MockIter)
	tsVals := []interface{}(nil)

	// first ensure that remote entries are injected into cache
	nsRows := mockDbRows{[]interface{}{NullSpaceName},
		[]interface{}{"remoteNS"}}
	nsIter := new(MockIter)
	nsVals := []interface{}{"remoteTS"}

	wsRows := mockDbRows{{"remoteWS1", int64(0), int64(0)}, {"remoteWS2",
		int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"remoteTS", "remoteNS"}

	mockWsdbCacheTypespaceFetch(suite.common.mockSess, tsRows, tsVals,
		tsIter, nil)
	mockWsdbCacheNamespaceFetch(suite.common.mockSess, nsRows, nsVals,
		nsIter, nil)
	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, nil)
	nonce := WorkspaceNonce{Id: 5, PublishTime: 0xbeef}
	mockWsdbKeyGet(suite.common.mockSess, "remoteTS", "remoteNS", "remoteWS2",
		[]byte(nil), nonce, nil)

	suite.cache.enableCqlRefresh(unitTestCqlCtx)

	nsCount, err1 := suite.common.wsdb.NumNamespaces(unitTestCqlCtx,
		"remoteTS")
	suite.Require().NoError(err1, "NumNamespaces failed: %s", err1)
	suite.Require().Equal(2, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)

	// needs to be done after DB fetch of
	// namespace since the fetch inserts remote
	// inserted namespaces into cache with default age
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "remoteTS")
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "remoteTS", "remoteNS")

	wsCount, err2 := suite.common.wsdb.NumWorkspaces(unitTestCqlCtx,
		"remoteTS", "remoteNS")
	suite.Require().NoError(err2, "NumWorkspaces failed: %s", err2)
	suite.Require().Equal(2, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	// cache must now contain
	// typespaces: remoteTS
	// namespaces: null, remoteNS
	// workspaces for remoteNS: remoteWS

	// mock workspace DB fetch returning no workspaces for "remoteNS" namespace
	wsRows = mockDbRows{[]interface{}{"remoteWS2", int64(0), int64(0)}}
	wsVals = []interface{}{"remoteTS", "remoteNS"}
	wsIter.SetRows(wsRows)

	suite.cache.enableCqlRefresh(unitTestCqlCtx, "remoteTS")
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "remoteTS", "remoteNS")

	wsMap, err3 := suite.common.wsdb.WorkspaceList(unitTestCqlCtx, "remoteTS",
		"remoteNS")
	suite.Require().NoError(err3, "WorkspaceList failed: %s", err3)
	suite.Require().Equal(1, len(wsMap),
		"Incorrect number of workspaces in cache")
	suite.Require().Contains(wsMap, "remoteWS2",
		"Incorrect workspaces in cache")
	suite.Require().Equal(nonce, wsMap["remoteWS2"],
		"Incorrect nonce value for remoteWS2")

}

func (suite *wsdbCacheTestSuite) TestCacheAfterBranching() {

	// newly branched workspace must be in cache
	mockBranchWorkspace(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, "ts1", "ns1", "ws1",
		[]byte(nil), WorkspaceNonceInvalid, gocql.ErrNotFound)
	_, n1, err := suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts1", "ns1", "ws1")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	mockBranchWorkspace(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, "ts1", "ns1", "ws2",
		[]byte(nil), WorkspaceNonceInvalid, gocql.ErrNotFound)
	_, n2, err := suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts1", "ns1", "ws2")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "ts1")
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "ts1", "ns1")

	nsCount, err1 := suite.common.wsdb.NumNamespaces(unitTestCqlCtx, "ts1")
	suite.Require().NoError(err1, "NumNamespaces failed: %s", err1)
	suite.Require().Equal(1, nsCount,
		"Incorrect number of namespaces in cache: %d", nsCount)
	nsList, err2 := suite.common.wsdb.NamespaceList(unitTestCqlCtx, "ts1")
	suite.Require().NoError(err2, "NamespaceList failed: %s", err2)
	// use Contains since order of elements within a list can change
	// as map traversal doesn't ensure order
	suite.Require().Contains(nsList, "ns1",
		"Incorrect namespaces in cache")

	wsCount, err3 := suite.common.wsdb.NumWorkspaces(unitTestCqlCtx,
		"ts1", "ns1")
	suite.Require().NoError(err3, "NumWorkspaces failed: %s", err3)
	suite.Require().Equal(2, wsCount,
		"Incorrect number of workspaces in cache: %d", wsCount)

	// Two disableCqlRefresh(s) are added below, so that the cache doesn't go to
	// db for uninitialized nonce values.
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour,
		"ts1", "ns1", "ws1")
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour,
		"ts1", "ns1", "ws2")
	wsMap, err4 := suite.common.wsdb.WorkspaceList(unitTestCqlCtx,
		"ts1", "ns1")
	suite.Require().NoError(err4, "WorkspaceList failed: %s", err4)
	suite.Require().Contains(wsMap, "ws1",
		"Incorrect workspace in cache")
	suite.Require().Equal(n1, wsMap["ws1"],
		"Incorrect nonce value for ws1")
	suite.Require().Contains(wsMap, "ws2",
		"Incorrect workspace in cache")
	suite.Require().Equal(n2, wsMap["ws2"],
		"Incorrect nonce value for ws2")
}

func (suite *wsdbCacheTestSuite) TestCacheConcInsertsRefresh() {

	// 1. Trigger a workspace refresh i.e. cql query
	//    and pause it using the chan 'wsFetchPause'
	//    in the mock iter.
	// 2. Once it is paused, perform local insert.
	// 3. Upause the fetch.
	// 4. Check that cache has remote as well as new local insert
	//    even though fetched data didn't include the local insert.

	suite.cache.InsertEntities(unitTestCqlCtx, "ts1", "ns1", "specialWS", "0")

	wsRows := mockDbRows{[]interface{}{"specialWS", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts1", "ns1"}

	var wsMap map[string]WorkspaceNonce
	var wsWg sync.WaitGroup
	// buffered chan since db will be queried twice and we want it to block
	// on the second query.
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)
	nonce := WorkspaceNonce{Id: 7, PublishTime: 0xbeef}
	mockWsdbKeyGet(suite.common.mockSess, "ts1", "ns1", "specialWS",
		[]byte(nil), nonce, nil)

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "ts1")
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "ts1", "ns1")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		var err error
		wsMap, err = suite.common.wsdb.WorkspaceList(unitTestCqlCtx,
			"ts1", "ns1")
		suite.Require().NoError(err,
			"WorkspaceList returned error for ts1/ns1: %v", err)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// data fetched from db contains null and specialWS workspaces for the null
	// namespace
	mockBranchWorkspace(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, "ts1", "ns1", "ws1",
		[]byte(nil), WorkspaceNonceInvalid, gocql.ErrNotFound)

	// causes a local insert of ws1 workspace for the null namespace
	_, _, err := suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts1", "ns1", "ws1")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	// Since we want the nonce refresh to hit the cache.
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour,
		"ts1", "ns1", "ws1")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for workspace API to complete
	wsWg.Wait()

	suite.Require().Contains(wsMap, "ws1",
		"Expected workspace ws1 not in cache")
	suite.Require().Contains(wsMap, "specialWS",
		"Expected workspace specialWS not in cache")
	suite.Require().Equal(nonce, wsMap["specialWS"],
		"Incorrect nonce value for specialWS")
}

func (suite *wsdbCacheTestSuite) TestCacheConcDeletesRefresh() {

	// 1. Trigger a workspace refresh i.e. cql query
	//    and pause it using the chan 'wsFetchPause'
	//    in the mock iter.
	// 2. Once it is paused, perform local delete.
	// 3. Upause the fetch.
	// 4. Check that cache does not have the locally deleted WS,
	//    even though it was a part of the return from DB query.

	suite.cache.InsertEntities(unitTestCqlCtx, NullSpaceName,
		NullSpaceName, "specialWS", "7")

	wsRows := mockDbRows{{NullSpaceName}, {"specialWS", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{NullSpaceName, NullSpaceName}

	var wsMap map[string]WorkspaceNonce
	var wsWg sync.WaitGroup
	// buffered chan since db will be queried twice and we want it to block
	// on the second query.
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)
	mockWsdbKeyGet(suite.common.mockSess, NullSpaceName, NullSpaceName,
		NullSpaceName, []byte(nil), WorkspaceNonceInvalid, nil)

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, NullSpaceName)
	suite.cache.enableCqlRefresh(unitTestCqlCtx, NullSpaceName,
		NullSpaceName)

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		var err error
		wsMap, err = suite.common.wsdb.WorkspaceList(unitTestCqlCtx,
			NullSpaceName, NullSpaceName)
		suite.Require().NoError(err,
			"WorkspaceList returned error for _/_/: %v", err)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for the null
	// namespace

	// TODO: currently workspaceDB API doesn't contain deletes
	//       so do a cache delete here
	suite.cache.DeleteEntities(unitTestCqlCtx, NullSpaceName, NullSpaceName,
		"specialWS")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Contains(wsMap, NullSpaceName,
		"Expected null workspace not in cache")
	suite.Require().NotContains(wsMap, "specialWS",
		"Unexpected workspace specialWS in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheSameInsDelDuringRefresh() {
	suite.cache.InsertEntities(unitTestCqlCtx, NullSpaceName,
		NullSpaceName, "specialWS")

	// 1. Trigger a workspace refresh i.e. cql query
	//    and pause it using the chan 'wsFetchPause'
	//    in the mock iter.
	// 2. Once it is paused, perform local insert and local delete of same ws.
	// 3. Upause the fetch.
	// 4. Check that cache does not have the locally inserted and deleted WS,
	//    but should have the new ws received from the db.

	wsRows := mockDbRows{{NullSpaceName}, {"specialWS", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{NullSpaceName, NullSpaceName}
	var wsMap map[string]WorkspaceNonce
	var wsWg sync.WaitGroup
	// buffered chan since db will be queried twice and we want it to block
	// on the second query.
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)
	mockWsdbKeyGet(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, []byte(nil),
		WorkspaceNonceInvalid, nil)
	nonce := WorkspaceNonce{Id: 9, PublishTime: 0xbeef}
	mockWsdbKeyGet(suite.common.mockSess, NullSpaceName,
		NullSpaceName, "specialWS", []byte(nil), nonce, nil)

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, NullSpaceName)
	suite.cache.enableCqlRefresh(unitTestCqlCtx, NullSpaceName,
		NullSpaceName)

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		var err error
		wsMap, err = suite.common.wsdb.WorkspaceList(unitTestCqlCtx,
			NullSpaceName, NullSpaceName)
		suite.Require().NoError(err,
			"WorkspaceList returned error for _/_/: %v", err)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains null and specialWS workspaces for null
	// namespace
	suite.cache.InsertEntities(unitTestCqlCtx, NullSpaceName,
		NullSpaceName, "newWS")
	suite.cache.DeleteEntities(unitTestCqlCtx, NullSpaceName,
		NullSpaceName, "newWS")

	suite.cache.DeleteEntities(unitTestCqlCtx, NullSpaceName,
		NullSpaceName, "specialWS")
	suite.cache.InsertEntities(unitTestCqlCtx, NullSpaceName,
		NullSpaceName, "specialWS")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Contains(wsMap, NullSpaceName,
		"Expected null workspace not in cache")
	suite.Require().Contains(wsMap, "specialWS",
		"Unexpected workspace specialWS in cache")
	suite.Require().Equal(nonce, wsMap["specialWS"],
		"Incorrect nonce value for specialWS")
	suite.Require().NotContains(wsMap, "newWS",
		"Unexpected workspace newWS in cache")
}

func (suite *wsdbCacheTestSuite) TestCacheGroupDeleteDuringRefresh() {

	// 1. Trigger a workspace refresh i.e. cql query
	//    and pause it using the chan 'wsFetchPause'
	//    in the mock iter.
	// 2. Once it is paused, perform local delete of all WS inside a NS.
	// 3. Upause the fetch.
	// 4. Check that cache does not have the locally deleted WS,
	//    even though it was a part of the return from DB query.

	nonceA := GetUniqueNonce()
	mockBranchWorkspace(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, "ts2", "ns2", "a",
		[]byte(nil), nonceA, gocql.ErrNotFound)

	nonceB := GetUniqueNonce()
	mockBranchWorkspace(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, "ts2", "ns2", "b",
		[]byte(nil), nonceB, gocql.ErrNotFound)

	_, _, err := suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts2", "ns2", "a")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	_, _, err = suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts2", "ns2", "b")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	wsRows := mockDbRows{{"a", int64(0), int64(0)}, {"b", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts2", "ns2"}
	var wsMap map[string]WorkspaceNonce
	var wsWg sync.WaitGroup
	// buffered chan since db will be queried twice and we want it to block
	// on the second query.
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "ts2")
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "ts2", "ns2")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		var err error
		wsMap, err = suite.common.wsdb.WorkspaceList(unitTestCqlCtx,
			"ts2", "ns2")
		suite.Require().NoError(err,
			"WorkspaceList returned error for ts2/ns2: %v", err)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete all workspace within test namespace which
	// also deletes the test namespace itself
	suite.cache.DeleteEntities(unitTestCqlCtx, "ts2", "ns2", "a")
	suite.cache.DeleteEntities(unitTestCqlCtx, "ts2", "ns2", "b")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Equal(0, len(wsMap),
		"workspace list in cache is not empty")
}

// TestCacheParentDeleteDuringRefresh tests scenario of
// delete "parentNS" namespace while DB refresh is underway
// as part of getting list of workspaces for "parentNS" namespace
// to ensure that list of workspaces is empty
func (suite *wsdbCacheTestSuite) TestCacheParentDeleteDuringRefresh() {
	nonceChildWS := GetUniqueNonce()
	mockBranchWorkspace(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, "ts", "parentNS",
		"childWS", []byte(nil), nonceChildWS, gocql.ErrNotFound)

	_, _, err := suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts", "parentNS",
		"childWS")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	wsRows := mockDbRows{[]interface{}{"childWS", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts", "parentNS"}
	var wsMap map[string]WorkspaceNonce
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "ts")
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "ts", "parentNS")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		var err error
		wsMap, err = suite.common.wsdb.WorkspaceList(unitTestCqlCtx, "ts",
			"parentNS")
		suite.Require().NoError(err,
			"WorkspaceList returned error for ts/parentNS: %v", err)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete null namespace
	suite.cache.DeleteEntities(unitTestCqlCtx, "ts", "parentNS")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Equal(0, len(wsMap),
		"workspace list in cache is not empty")
}

// TestCacheAncestorDeleteDuringRefresh tests the scenario of
// delete "null" typespace while DB refresh is underway as part of
// getting list of workspaces for "parentNS" namespace to
// ensure list of workspaces is empty
func (suite *wsdbCacheTestSuite) TestCacheAncestorDeleteDuringRefresh() {
	nonceChildWS := GetUniqueNonce()
	mockBranchWorkspace(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, "ts", "parentNS",
		"childWS", []byte(nil), nonceChildWS, gocql.ErrNotFound)

	_, _, err := suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts", "parentNS",
		"childWS")
	suite.Require().NoError(err, "Error rebranching workspace: %v", err)

	wsRows := mockDbRows{[]interface{}{"childWS", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"ts", "parentNS"}
	var wsMap map[string]WorkspaceNonce
	var wsWg sync.WaitGroup
	wsFetchPause := make(chan bool)

	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "ts")
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "ts", "parentNS")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		var err error
		wsMap, err = suite.common.wsdb.WorkspaceList(unitTestCqlCtx, "ts",
			"parentNS")
		suite.Require().NoError(err,
			"WorkspaceList returned error for ts/parentNS: %v", err)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// delete null typespace (and all children)
	suite.cache.DeleteEntities(unitTestCqlCtx, "ts")

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for NamespaceList API to complete
	wsWg.Wait()

	suite.Require().Equal(0, len(wsMap),
		"workspace list in cache is not empty")
}

// TestCacheChildDeleteDuringRefresh tests the scenario of
// delete "childWS" workspace when DB refresh is
// underway as part of getting list of namespaces for "ts" typespace
// to ensure the list of namespaces is eempty
func (suite *wsdbCacheTestSuite) TestCacheChildDeleteDuringRefresh() {
	nonceChildWS := GetUniqueNonce()
	mockBranchWorkspace(suite.common.mockSess, NullSpaceName,
		NullSpaceName, NullSpaceName, "ts", "parentNS",
		"childWS", []byte(nil), nonceChildWS, gocql.ErrNotFound)

	_, _, err := suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts", "parentNS",
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

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "ts")

	nsWg.Add(1)
	go func() {
		defer nsWg.Done()
		nsList, _ = suite.common.wsdb.NamespaceList(unitTestCqlCtx, "ts")
	}()

	// wait for fetch to stall
	<-nsFetchPause

	// delete childWS which causes parentNS to also get
	// deleted
	suite.cache.DeleteEntities(unitTestCqlCtx, "ts", "parentNS", "childWS")

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
	suite.cache.enableCqlRefresh(unitTestCqlCtx)

	_, err := suite.common.wsdb.NumTypespaces(unitTestCqlCtx)

	suite.Require().Error(err, "NumTypespaces did not get error")
	suite.Require().Equal("PanicOnFetch", err.Error(),
		"Unexpected error %s in NumTypespaces", err.Error())
}

// TestRefreshFailure tests if the error in DB refresh
// is properly passed on to the WSDB API
func (suite *wsdbCacheTestSuite) TestRefreshFailure() {
	expectedErr := errors.New("DB error")

	suite.cache.enableCqlRefresh(unitTestCqlCtx)
	mockWsdbCacheTypespaceFetchErr(suite.common.mockSess, expectedErr)
	mockWsdbCacheNamespaceFetchErr(suite.common.mockSess, expectedErr)
	mockWsdbCacheWorkspaceFetchErr(suite.common.mockSess, expectedErr)

	_, err := suite.common.wsdb.NumTypespaces(unitTestCqlCtx)
	suite.Require().Error(err)

	_, err = suite.common.wsdb.TypespaceList(unitTestCqlCtx)
	suite.Require().Error(err)

	_, err = suite.common.wsdb.NumNamespaces(unitTestCqlCtx, "t1")
	suite.Require().Error(err)

	_, err = suite.common.wsdb.NamespaceList(unitTestCqlCtx, "t1")
	suite.Require().Error(err)

	_, err = suite.common.wsdb.NumWorkspaces(unitTestCqlCtx, "t1", "n1")
	suite.Require().Error(err)

	_, err = suite.common.wsdb.WorkspaceList(unitTestCqlCtx, "t1", "n1")
	suite.Require().Error(err)
}

// TestCacheDeleteWorkspaceNumOK tests if the cache is updated
// properly when a workspace is deleted
func (suite *wsdbCacheTestSuite) TestCacheDeleteWorkspaceNumOK() {
	mockBranchWorkspace(suite.common.mockSess,
		NullSpaceName, NullSpaceName,
		NullSpaceName, "ts1", "ns1", "ws1", []byte(nil),
		WorkspaceNonceInvalid, gocql.ErrNotFound)
	_, _, err := suite.common.wsdb.BranchWorkspace(unitTestCqlCtx,
		NullSpaceName, NullSpaceName, NullSpaceName, "ts1", "ns1", "ws1")
	suite.Require().NoError(err,
		"Error branching "+NullSpaceName+" workspace: %v", err)

	// disable fetches from DB so that cache state is unchanged
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, NullSpaceName)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, NullSpaceName,
		NullSpaceName)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "ts1")
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "ts1", "ns1")

	// cached APIs
	tsCount, err1 := suite.common.wsdb.NumTypespaces(unitTestCqlCtx)
	suite.Require().NoError(err1, "NumTypespaces failed: %s", err1)
	suite.Require().Equal(2, tsCount,
		"Incorrect number of Typespaces. Exp: 2, Actual: %d", tsCount)

	mockWsdbKeyDel(suite.common.mockSess, "ts1", "ns1", "ws1", nil)
	err = suite.common.wsdb.DeleteWorkspace(unitTestCqlCtx,
		"ts1", "ns1", "ws1")
	suite.Require().NoError(err, "Error in DeleteWorkspace: %v", err)

	tsCount, err1 = suite.common.wsdb.NumTypespaces(unitTestCqlCtx)
	suite.Require().NoError(err1, "NumTypespaces failed: %s", err1)
	// locally branched workspace should get deleted from cache
	suite.Require().Equal(1, tsCount,
		"Incorrect number of Typespaces. Exp: 1, Actual: %d", tsCount)
}

// a dummy test to verify that mock.Anything works
// fine for session.Query mocking
func (suite *wsdbCacheTestSuite) TestCacheIgnoreField() {

	// setup mock
	func() {
		mq := new(MockQuery)
		stmt := `
INSERT INTO ether.workspacedb
(typespace, namespace, workspace, key, ignore)
VALUES (?,?,?,?,?,?)`

		suite.common.mockSess.On("Query", stmt,
			"ts", "ns", "ws", []byte(nil),
			mock.AnythingOfType("int64"),
			mock.AnythingOfType("int64")).Return(mq)
		mq.On("Exec").Return(nil)
	}()

	// trigger mock
	nonce := GetUniqueNonce()
	query := suite.common.mockSess.Query(`
INSERT INTO ether.workspacedb
(typespace, namespace, workspace, key, ignore)
VALUES (?,?,?,?,?,?)`, "ts", "ns", "ws", []byte(nil), nonce.Id, nonce.PublishTime)

	err := query.Exec()
	suite.Require().NoError(err, "Insert failed with %s", err)
}

// caller1 and caller2 are two concurrent List requests
// for the same cache path. They should never see nil
// data if the cache entry is not setup. The cache entry
// may not be setup if the cache is just started up or if
// that cache entity was recently deleted from cache.
func (suite *wsdbCacheTestSuite) TestCacheConcTsListWhenEmpty() {

	// make sure the cache is empty, not even null workspace
	// since the test requires empty typespace list in cache
	suite.cache.DeleteEntities(unitTestCqlCtx, NullSpaceName)

	tsRows := mockDbRows{[]interface{}{"t1"}}
	tsIter := new(MockIter)
	tsVals := []interface{}(nil)

	var tsWg sync.WaitGroup
	var tsList1, tsList2 []string
	// buffered chan since db will be queried twice and we want it to block
	// on the second query.
	tsFetchPause := make(chan bool)

	mockWsdbCacheTypespaceFetch(suite.common.mockSess, tsRows, tsVals,
		tsIter, tsFetchPause)

	suite.cache.enableCqlRefresh(unitTestCqlCtx)

	tsWg.Add(1)
	go func() {
		defer tsWg.Done()
		var err error
		var call1Ctx = &StdCtx{RequestID: 1}
		tsList1, err = suite.common.wsdb.TypespaceList(call1Ctx)
		suite.Require().NoError(err,
			"TypespaceList call1 returned error : %v", err)
	}()

	// wait for fetch to stall
	<-tsFetchPause

	// fetched data contains t1 typespace

	// now start another TypespaceList API call which should see that a
	// refresh is in progress, block and both TypespaceList API calls
	// should complete with the t1 as the typespace

	tsWg.Add(1)
	go func() {
		defer tsWg.Done()
		var err error
		var call2Ctx = &StdCtx{RequestID: 2}
		tsList2, err = suite.common.wsdb.TypespaceList(call2Ctx)
		suite.Require().NoError(err,
			"TypespaceList call2 returned error : %v", err)
	}()

	// unpause the DB fetch
	tsFetchPause <- true

	// wait for both TypespaceList API calls to complete
	tsWg.Wait()

	suite.Require().Contains(tsList1, "t1",
		"Expected t1 typespace not seen in call1")
	suite.Require().Contains(tsList2, "t1",
		"Expected t1 typespace not seen in call2")

}

// all concurrent callers must terminate with the refresh error
func (suite *wsdbCacheTestSuite) TestCacheConcTsListWhenEmptyErr() {

	// make sure the cache is empty, not even null workspace
	// since the test requires empty typespace list in cache
	suite.cache.DeleteEntities(unitTestCqlCtx, NullSpaceName)

	mockWsdbCacheTypespaceFetchPanic(suite.common.mockSess)

	var tsWg sync.WaitGroup
	// channel used to create concurrent TS list requests
	tsListPause := make(chan bool)

	suite.cache.enableCqlRefresh(unitTestCqlCtx)

	tsWg.Add(1)
	go func() {
		defer tsWg.Done()
		var err error
		var call1Ctx = &StdCtx{RequestID: 1}
		<-tsListPause

		_, err = suite.common.wsdb.TypespaceList(call1Ctx)
		suite.Require().Error(err, "call1 expected did not get error")
		suite.Require().Equal("PanicOnFetch", err.Error(),
			"Unexpected error %s in call1", err.Error())
	}()

	tsWg.Add(1)
	go func() {
		defer tsWg.Done()
		var err error
		var call2Ctx = &StdCtx{RequestID: 2}
		<-tsListPause

		_, err = suite.common.wsdb.TypespaceList(call2Ctx)
		suite.Require().Error(err, "call2 expected did not get error")
		suite.Require().Equal("PanicOnFetch", err.Error(),
			"Unexpected error %s in call2", err.Error())
	}()

	// let go the TS list APIs
	close(tsListPause)

	// wait for both TypespaceList API calls to complete
	tsWg.Wait()

}

// caller1 and caller2 are two concurrent List requests
// for the same cache path. They should never see nil
// data if the cache entry is not setup. The cache entry
// may not be setup if the cache is just started up or if
// that cache entity was recently deleted from cache.
func (suite *wsdbCacheTestSuite) TestCacheConcNsListWhenEmpty() {
	suite.cache.InsertEntities(unitTestCqlCtx, "t1")

	nsRows := mockDbRows{[]interface{}{"n1"}}
	nsIter := new(MockIter)
	nsVals := []interface{}{"t1"}

	var nsWg sync.WaitGroup
	var nsList1, nsList2 []string
	// buffered chan since db will be queried twice and we want it to block
	// on the second query.
	nsFetchPause := make(chan bool)

	mockWsdbCacheNamespaceFetch(suite.common.mockSess, nsRows, nsVals,
		nsIter, nsFetchPause)

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "t1")

	nsWg.Add(1)
	go func() {
		defer nsWg.Done()
		var err error
		var call1Ctx = &StdCtx{RequestID: 1}
		nsList1, err = suite.common.wsdb.NamespaceList(call1Ctx, "t1")
		suite.Require().NoError(err,
			"NamespaceList call1 returned error : %v", err)
	}()

	// wait for fetch to stall
	<-nsFetchPause

	// fetched data contains t1 typespace

	// now start another TypespaceList API call which should see that a
	// refresh is in progress, block and both TypespaceList API calls
	// should complete with the t1 as the typespace

	nsWg.Add(1)
	go func() {
		defer nsWg.Done()
		var err error
		var call2Ctx = &StdCtx{RequestID: 2}
		nsList2, err = suite.common.wsdb.NamespaceList(call2Ctx, "t1")
		suite.Require().NoError(err,
			"NamespaceList call2 returned error : %v", err)
	}()

	// unpause the DB fetch
	nsFetchPause <- true

	// wait for both TypespaceList API calls to complete
	nsWg.Wait()

	suite.Require().Contains(nsList1, "n1",
		"Expected n1 namespace not seen in call1")
	suite.Require().Contains(nsList2, "n1",
		"Expected n1 namespace not seen in call2")
}

// caller1 and caller2 are two concurrent List requests
// for the same cache path. They should never see nil
// data if the cache entry is not setup. The cache entry
// may not be setup if the cache is just started up or if
// that cache entity was recently deleted from cache.
func (suite *wsdbCacheTestSuite) TestCacheConcWsListWhenEmpty() {
	suite.cache.InsertEntities(unitTestCqlCtx, "t1", "n1")

	var wsWg sync.WaitGroup
	var wsMap1, wsMap2 map[string]WorkspaceNonce
	wsFetchPause := make(chan bool)

	wsRows := mockDbRows{{"w1", int64(0), int64(0)}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"t1", "n1"}
	mockWsdbCacheWorkspaceFetch(suite.common.mockSess, wsRows, wsVals,
		wsIter, wsFetchPause)
	nonce := WorkspaceNonce{Id: 7, PublishTime: 0xbeef}
	mockWsdbKeyGet(suite.common.mockSess, "t1", "n1", "w1",
		[]byte(nil), nonce, nil)

	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour)
	suite.cache.disableCqlRefresh(unitTestCqlCtx, 1*time.Hour, "t1")
	suite.cache.enableCqlRefresh(unitTestCqlCtx, "t1", "n1")

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		var err error
		var call1Ctx = &StdCtx{RequestID: 1}
		wsMap1, err = suite.common.wsdb.WorkspaceList(call1Ctx, "t1", "n1")
		suite.Require().NoError(err,
			"WorkspaceList call1 returned error : %v", err)
	}()

	// wait for fetch to stall
	<-wsFetchPause

	// fetched data contains t1 typespace

	// now start another WorkspaceList API call which should see that a
	// refresh is in progress, block and both TypespaceList API calls
	// should complete with the t1 as the typespace

	wsWg.Add(1)
	go func() {
		defer wsWg.Done()
		var err error
		var call2Ctx = &StdCtx{RequestID: 2}
		wsMap2, err = suite.common.wsdb.WorkspaceList(call2Ctx, "t1", "n1")
		suite.Require().NoError(err,
			"WorkspaceList call2 returned error : %v", err)
	}()

	// unpause the DB fetch
	wsFetchPause <- true

	// wait for both WorkspaceList API calls to complete
	wsWg.Wait()

	suite.Require().Contains(wsMap1, "w1",
		"Expected w1 workspace not seen in call1")
	suite.Require().Contains(wsMap2, "w1",
		"Expected w1 workspace not seen in call2")

	suite.Require().Equal(wsMap1["w1"], nonce,
		"Expected nonce 7 not see in wsMap1, %d", wsMap1["w1"].Id)
	suite.Require().Equal(wsMap2["w1"], nonce,
		"Expected nonce 7 not see in wsMap2, %d", wsMap2["w1"].Id)
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
