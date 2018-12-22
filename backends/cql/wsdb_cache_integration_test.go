// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// +build integration

package cql

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/stretchr/testify/suite"
)

type wsdbCacheIntegTestSuite struct {
	suite.Suite
	common *wsdbCommonIntegTest
	cache  *entityCache
}

func (suite *wsdbCacheIntegTestSuite) SetupTest() {
	confFile, err := EtherConfFile()
	suite.Require().NoError(err, "error in getting ether configuration file")
	err = SetupIntegTestKeyspace(confFile)
	suite.Require().NoError(err, "SetupIntegTestKeyspace returned an error")
	err = DoTestSchemaOp(confFile, SchemaCreate)
	suite.Require().NoError(err, "DoTestSchemaOp SchemaCreate returned an error")

	nwsdb := NewWorkspaceDB(confFile)
	cwsdb, ok := nwsdb.(*cacheWsdb)
	suite.Require().True(ok, "Incorrect type from newCacheWsdb")

	suite.cache = cwsdb.cache

	err = nwsdb.CreateWorkspace(integTestEtherCtx, NullSpaceName, NullSpaceName, NullSpaceName,
		wsdb.WorkspaceNonceInvalid, []byte(nil))
	suite.Require().NoError(err, "Error during CreateWorkspace")

	suite.common = &wsdbCommonIntegTest{
		req: suite.Require(),
		db:  nwsdb,
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

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegWorkspaceNonce() {
	suite.common.TestIntegWorkspaceNonce()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegSetWorkspaceImmutable() {
	suite.common.TestIntegSetWorkspaceImmutable()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegSetWorkspaceImmutableError() {
	suite.common.TestIntegSetWorkspaceImmutableError()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegWorkspaceIsImmutable() {
	suite.common.TestIntegWorkspaceIsImmutable()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegWorkspaceIsImmutableError() {
	suite.common.TestIntegWorkspaceIsImmutableError()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegDeleteImmutableSet() {
	suite.common.TestIntegDeleteImmutableSet()
}

func (suite *wsdbCacheIntegTestSuite) TestCacheIntegDeleteWorkspaceNumOK() {
	_, _, err := suite.common.db.BranchWorkspace(integTestEtherCtx, NullSpaceName,
		qwsdb.NullSpaceName, NullSpaceName,
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

type testCtx struct {
	context.Context
	ectx *StdCtx
}

// TestCacheListCountMixer uses multiple goroutines to randomly issue
// WSDB List and Count APIs while a goroutine expires entries in the cache.
// The API to be used, the workspace components used are all chosen randomly
// from an available set. The List and Count APIs should never see an empty
// list or zero count during this test.
func (suite *wsdbCacheIntegTestSuite) TestCacheListCountMixer() {
	baseReqId := uint64(10)
	testData := getWorkspaceTestData(5, 5, 5)

	testDuration := 2 * time.Minute
	err := loadWorkspaceData(integTestEtherCtx, suite.common.db, testData)
	suite.Require().NoError(err, "Error %s during loadWorkspaceData", err)

	emptyCache(integTestEtherCtx, suite.cache, testData)

	testTimeCtx, cancel := context.WithTimeout(context.Background(),
		testDuration)
	defer cancel()

	testGroup, testMainCtx := errgroup.WithContext(testTimeCtx)
	tctx := &testCtx{
		Context: testMainCtx,
		ectx:    &StdCtx{RequestID: baseReqId},
	}

	fmt.Printf("Starting CacheListCountMixer test for %v\n", testDuration)

	for i := 1; i <= 10; i++ {
		newTctx := *tctx
		newTctx.ectx = &StdCtx{RequestID: baseReqId + uint64(i)}
		testGroup.Go(func() error {
			return countListChecker(&newTctx, suite.common.db, testData)
		})
	}

	testGroup.Go(func() error {
		expCtx := *tctx
		expCtx.ectx = &StdCtx{RequestID: baseReqId - uint64(1)}
		return expirator(&expCtx, suite.cache, testData)
	})

	err = testGroup.Wait()
	suite.Require().NoError(err, "Error %s", err)
}

type wsdata struct {
	// m holds list of all typespaces under TSLIST,
	// namespaces for a ts and workspaces for a ts/ns
	m map[string][]string
	// list of all workspaces in ts/ns/ws format
	l []string
}

const allTypespaces = "TSLIST"

func getWorkspaceTestData(numTs, numNsPerTs, numWsPerNs int) *wsdata {
	w := &wsdata{
		m: make(map[string][]string),
		l: make([]string, 0),
	}

	for ts := 1; ts < numTs; ts++ {
		tsStr := fmt.Sprintf("ts%d", ts)
		w.m[allTypespaces] = append(w.m[allTypespaces], tsStr)
		for ns := 1; ns < numNsPerTs; ns++ {
			nsStr := fmt.Sprintf("ns%d", ns)
			w.m[tsStr] = append(w.m[tsStr], nsStr)
			for ws := 1; ws < numWsPerNs; ws++ {
				wsStr := fmt.Sprintf("ws%d", ws)
				key := fmt.Sprintf("%s/%s", tsStr, nsStr)
				w.m[key] = append(w.m[key], wsStr)
				// uses non-zero nonce in this test
				w.l = append(w.l,
					fmt.Sprintf("%s/%s/%s/%d", tsStr, nsStr,
						wsStr, rand.Int63()+1))

			}
		}
	}
	// add null typespace into the list of typespaces
	// we don't use the null workspace in the test though
	w.m[allTypespaces] = append(w.m[allTypespaces], "_")

	// sort each []string in the map
	for _, v := range w.m {
		sort.Strings(v)
	}

	return w
}

func loadWorkspaceData(c Ctx, db WorkspaceDB, w *wsdata) error {
	for _, ws := range w.l {
		parts := strings.Split(ws, "/")
		nonceStr := fmt.Sprintf("%s %d", parts[3], 0)
		nonce, err := StringToNonce(nonceStr)
		if err != nil {
			return err
		}
		err = db.CreateWorkspace(c, parts[0], parts[1], parts[2],
			nonce, ObjectKey{})
		if err != nil {
			return err
		}
	}

	return nil
}

func emptyCache(c Ctx, ec *entityCache, w *wsdata) {
	for _, ws := range w.l {
		parts := strings.Split(ws, "/")
		// deleting the typespace removes all cache entities
		// underneath it
		ec.DeleteEntities(c, parts[0])
	}
	// currently the cache has null workspace at the start
	// so we explicitly clean it up
	ec.DeleteEntities(c, "_")
}

func listEqual(l1 []string, l2 []string) bool {
	sort.Strings(l2)
	return reflect.DeepEqual(l1, l2)
}

func countListChecker(c *testCtx, db WorkspaceDB, w *wsdata) error {
	var err error
	var l []string
	var m map[string]wsdb.WorkspaceNonce
	var count int

	apiCount := 6

	for {
		api := rand.Intn(apiCount)
		wsidx := rand.Intn(len(w.l))

		wsparts := strings.Split(w.l[wsidx], "/")
		ts, ns := wsparts[0], wsparts[1]
		switch api {
		case 0:
			l, err = db.TypespaceList(c.ectx)
			if err != nil {
				return err
			}
			if len(l) == 0 {
				return fmt.Errorf("Found empty list for TypespaceList")
			}
			if !listEqual(w.m[allTypespaces], l) {
				return fmt.Errorf("Mismatched list of Typespaces: "+
					"expected: %v actual: %v", w.m[allTypespaces], l)
			}
			testLog(c, "TypespaceList %s", l)
		case 1:
			l, err = db.NamespaceList(c.ectx, ts)
			if err != nil {
				return err
			}
			if len(l) == 0 {
				return fmt.Errorf("Found empty list for NamespaceList(%s)", ts)
			}
			if !listEqual(w.m[ts], l) {
				return fmt.Errorf("Mistmatch list of Namespaces(%s): "+
					"expected: %v actual: %v", ts, w.m[ts], l)
			}
			testLog(c, "NamespaceList(%s) %s", ts, l)
		case 2:
			m, err = db.WorkspaceList(c.ectx, ts, ns)
			if err != nil {
				return err
			}
			if len(m) == 0 {
				return fmt.Errorf("Found empty map for WorkspaceList(%s/%s)",
					ts, ns)
			}
			wslist := make([]string, 0)
			for w, n := range m {
				if n == WorkspaceNonceInvalid {
					return fmt.Errorf("Found 0 nonce for Workspace(%s/%s/%s)",
						ts, ns, w)
				}
				wslist = append(wslist, w)
			}
			key := ts + "/" + ns
			if !listEqual(w.m[key], wslist) {
				return fmt.Errorf("Mismatch list of Workspaces(%s): "+
					"expected: %v actual: %v", key, w.m[key], wslist)
			}
			testLog(c, "WorkspaceList(%s/%s) %v", ts, ns, m)
		case 3:
			count, err = db.NumTypespaces(c.ectx)
			if err != nil {
				return err
			}
			if count == 0 {
				return fmt.Errorf("Found zero count for NumTypespaces")
			}
			testLog(c, "NumTypespaces %d", count)
		case 4:
			count, err = db.NumNamespaces(c.ectx, ts)
			if err != nil {
				return err
			}
			if count == 0 {
				return fmt.Errorf("Found zero count for NumNamespaces(%s)", ts)
			}
			testLog(c, "NumNamespaces(%s) %d", ts, count)
		case 5:
			count, err = db.NumWorkspaces(c.ectx, ts, ns)
			if err != nil {
				return err
			}
			if count == 0 {
				return fmt.Errorf("Found zero count for NumWorkspaces(%s/%s)",
					ts, ns)
			}
			testLog(c, "NumWorkspaces(%s/%s) %d", ts, ns, count)
		}

		select {
		case <-c.Done():
			return ignoreDeadlineExceeded(c)
		default:
		}
		time.Sleep(time.Duration(rand.Intn(100)) * time.Nanosecond)
		testLog(c, "Next iteration")
	}

}

func expirator(c *testCtx, ec *entityCache, w *wsdata) error {

	for {
		wsidx := rand.Intn(len(w.l))
		wsparts := strings.Split(w.l[wsidx], "/")
		wsprefix := wsparts[0:rand.Intn(len(wsparts))]
		ec.enableCqlRefresh(c.ectx, wsprefix...)
		select {
		case <-c.Done():
			return ignoreDeadlineExceeded(c)
		default:
		}
		time.Sleep(time.Duration(rand.Intn(100)))
	}

}

func testLog(c *testCtx, fmt string, args ...interface{}) {
	c.ectx.Vlog(fmt, args...)
}

func ignoreDeadlineExceeded(c *testCtx) error {
	if c.Err() == context.DeadlineExceeded {
		return nil
	}
	return c.Err()
}

func (suite *wsdbCacheIntegTestSuite) TearDownTest() {
	confFile, err := EtherConfFile()
	suite.Require().NoError(err, "error in getting ether configuration file")
	_ = DoTestSchemaOp(confFile, SchemaDelete)
	resetCqlStore()
}

func (suite *wsdbCacheIntegTestSuite) TearDownSuite() {
}

func TestWSDBCacheIntegTests(t *testing.T) {
	suite.Run(t, new(wsdbCacheIntegTestSuite))
}
