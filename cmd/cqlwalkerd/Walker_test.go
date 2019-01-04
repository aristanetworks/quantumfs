// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/cmd/cqlwalker/utils"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/walker"
)

func (t *testHelper) testCtx() *Ctx {
	datastore := t.GetDataStore()
	translator := datastore.(*cql.CqlBlobStoreTranslator)

	return &Ctx{
		qctx:  t.QfsCtx(),
		wsdb:  t.GetWorkspaceDB(),
		ds:    datastore,
		cqlds: translator.Blobstore,
		ttlCfg: &utils.TTLConfig{
			SkipMapResetAfter_ms: 500,
			TTLNew:               600,
		},
		numWalkers:    1,
		wsNameMatcher: func(s string) bool { return true },
	}
}

func (test *testHelper) setTTL(c *Ctx, filepath string, ttl int64) {
	c.vlog("setTTL %s %d", filepath, ttl)

	record := test.GetRecord(filepath)
	fileId := record.ID().Value()
	walkerCtx := walker.Ctx{
		Qctx: c.qctx,
	}

	buf, _, err := c.cqlds.Get(utils.ToECtx(&walkerCtx), fileId)
	test.AssertNoErr(err)

	newmetadata := make(map[string]string)
	newmetadata[cql.TimeToLive] = fmt.Sprintf("%d", ttl)
	test.AssertNoErr(c.cqlds.Insert(utils.ToECtx(&walkerCtx), fileId, buf,
		newmetadata))
}

func (test *testHelper) getTTL(c *Ctx, filepath string) int64 {
	c.vlog("getTTL %s", filepath)

	record := test.GetRecord(filepath)
	fileId := record.ID().Value()
	walkerCtx := walker.Ctx{
		Qctx: c.qctx,
	}
	metadata, err := c.cqlds.Metadata(utils.ToECtx(&walkerCtx), fileId)

	test.AssertNoErr(err)
	test.Assert(metadata != nil, "Metadata missing for file")

	ttl, ok := metadata[cql.TimeToLive]
	test.Assert(ok, "Metadata missing cql TimeToLive")

	ttlVal, err := strconv.ParseInt(ttl, 10, 64)
	test.AssertNoErr(err)

	return ttlVal
}

// Test to ensure that walker skip map prevents redundant node walking
// when optimal walks are enabled
func TestRefreshTTLCache_WalkOptimal(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()
		directory := workspace + "/dirA/dirB/dirC"
		file := directory + "/file"

		test.AssertNoErr(os.MkdirAll(directory, 0777))
		test.AssertNoErr(testutils.PrintToFile(file, "file data"))

		test.SyncAllWorkspaces()

		c := test.testCtx()
		// optimal walks
		go walkFullWSDBLoop(c, false, true)

		test.WaitFor("First walker pass", func() bool {
			ttl := test.getTTL(c, file)
			return ttl == c.ttlCfg.TTLNew
		})

		// Clear the TTL so we can tell when walker bypasses skipMap again
		test.setTTL(c, file, 1)

		walks := test.CountLogStrings("TTL refresh for")

		test.WaitFor("Walker to walk again", func() bool {
			walksNow := test.CountLogStrings("TTL refresh for")
			return walksNow > walks
		})

		// Even with walker busy looping, the skipMap should have stopped
		// TTLs from being reset
		unrefreshedTTL := test.getTTL(c, file)
		test.Assert(unrefreshedTTL == 1,
			"TTL refreshed, not skipped: %d", unrefreshedTTL)

		test.WaitForLogString(SkipMapClearLog, "SkipMap never clears")
		test.WaitFor("TTL to be refreshed again", func() bool {
			ttl := test.getTTL(c, file)
			return ttl == c.ttlCfg.TTLNew
		})
	})
}

// Test to check that keys are repeatedly refreshed when optimal walk is disabled
func TestRefreshTTLCache_WalkNonOptimal(t *testing.T) {
	runTest(t, func(test *testHelper) {

		wsFullPath := test.NewWorkspace()
		directory := wsFullPath + "/dirA/dirB/dirC"
		file := directory + "/file"

		wsParts := strings.Split(wsFullPath, "/")
		workspace := strings.Join(wsParts[len(wsParts)-3:len(wsParts)], "/")

		test.AssertNoErr(os.MkdirAll(directory, 0777))
		test.AssertNoErr(testutils.PrintToFile(file, "file data"))

		test.SyncAllWorkspaces()

		record := test.GetRecord(file)
		fileId := record.ID().Value()

		c := test.testCtx()
		// In legacy mode, TTLRefreshTime is saved as ms into
		// SkipMapResetAfter_ms. This test sets TTL to 1s so
		// to force refresh after it, the threshold should be
		// greater than 1s
		c.ttlCfg.SkipMapResetAfter_ms = 2000

		// non-optimal walks
		go walkFullWSDBLoop(c, false, false)

		test.WaitForLogString(
			fmt.Sprintf("Success: TTL refresh for %s", workspace),
			"Walker never refreshes the workspace")

		// ttl should be refreshed
		ttl := test.getTTL(c, file)
		test.Assert(ttl == c.ttlCfg.TTLNew,
			"TTL not refreshed, ttl: %d", ttl)
		// TTL is refreshed using Inserts, so count refreshes
		refreshes := test.CountLogStrings(
			fmt.Sprintf("Insert key: %s", hex.EncodeToString(fileId)))

		// count walks of the test workspace
		walks := test.CountLogStrings(
			fmt.Sprintf("Success: TTL refresh for %s", workspace))

		test.WaitFor("Walker to refresh workspace again", func() bool {
			walksNow := test.CountLogStrings(
				fmt.Sprintf("Success: TTL refresh for %s",
					workspace))
			return walksNow > walks
		})

		refreshesNow := test.CountLogStrings(
			fmt.Sprintf("Insert key: %s", hex.EncodeToString(fileId)))

		// due to threshold TTL check, walker will not refresh key
		test.Assert(refreshesNow == refreshes,
			"TTL not refreshed, refreshes: %d, refreshesNow: %d",
			refreshes, refreshesNow)

		// Clear the TTL so that walker can refresh the key again
		test.setTTL(c, file, 1)

		// count walks of the test workspace
		walks = test.CountLogStrings(
			fmt.Sprintf("Success: TTL refresh for %s", workspace))

		test.WaitFor("Walker to refresh workspace after setTTL",
			func() bool {
				walksNow := test.CountLogStrings(
					fmt.Sprintf("Success: TTL refresh for %s",
						workspace))
				return walksNow > walks
			})

		ttl = test.getTTL(c, file)
		test.Assert(ttl == c.ttlCfg.TTLNew,
			"TTL not refreshed again, ttl: %d", ttl)
	})
}

// Test that the skip map LRU maintains a consistent cache length within a max length
func TestMapMaxLen(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		c := test.testCtx()
		c.ttlCfg.SkipMapMaxLen = 5

		// Make a bunch more files than the cache capacity
		for i := 0; i < 10; i++ {
			filename := fmt.Sprintf("%s/file%d", workspace, i)
			test.AssertNoErr(testutils.PrintToFile(filename,
				fmt.Sprintf("msc unique data %d", i)))
		}

		test.SyncAllWorkspaces()

		c.skipMap = utils.NewSkipMap(c.ttlCfg.SkipMapMaxLen)
		walkFullWSDBSetup(c)

		cacheLen, mapLen := c.skipMap.Len()
		test.Assert(cacheLen == mapLen, "Lru and map mismatch")
		// The cache should be full, given the number of files we made
		test.Assert(cacheLen == c.ttlCfg.SkipMapMaxLen,
			"Max length not obeyed: %d", cacheLen)

		// No matter how many times we walk, the map length should be obeyed
		walkFullWSDBSetup(c)

		cacheLen, mapLen = c.skipMap.Len()
		test.Assert(cacheLen == mapLen, "Lru and map mismatch")
		test.Assert(cacheLen == c.ttlCfg.SkipMapMaxLen,
			"Max length not obeyed: %d", cacheLen)
	})
}

// TestWorkspacePrunedAfterListing asserts that walk of
// a workspace which has been pruned after walker daemon
// sees the workspace in listing, doesn't result in error.
func TestWorkspacePrunedAfterListing(t *testing.T) {
	runTest(t, func(test *testHelper) {
		// workspace t1/nw/w1 wasn't created
		c := test.testCtx()
		err := runWalker(c, "t1", "n1", "w1")
		test.AssertNoErr(err)
	})
}

// TestWsNameMatcher checks if the workspaces are
// included in the walks based on the prefix.
func TestWsNameMatcher(t *testing.T) {
	runTest(t, func(test *testHelper) {

		c := test.testCtx()
		workspaces, files, oldTTLs := test.setupWsFilesTTLs(c, 2)
		c.wsNameMatcher = getPrefixMatcher(workspaces[0], false)

		walkFullWSDBSetup(c)

		newTTL0 := test.getTTL(c, files[0])
		newTTL1 := test.getTTL(c, files[1])

		// check that refresh happened only in first workspace
		test.Assert(newTTL0 > oldTTLs[0],
			"TTL for file-0 not refreshed, old: %d new: %d",
			oldTTLs[0], newTTL0)
		test.Assert(newTTL1 <= oldTTLs[1],
			"TTL for file-1 got refreshed, old: %d new:%d",
			oldTTLs[1], newTTL1)

	})
}

// TestWsNameNegMatcher checks if the workspaces are
// included in the walks based on the prefix.
func TestWsNameNegMatcher(t *testing.T) {
	runTest(t, func(test *testHelper) {

		c := test.testCtx()
		workspaces, files, oldTTLs := test.setupWsFilesTTLs(c, 2)
		c.wsNameMatcher = getPrefixMatcher(workspaces[0], true)

		walkFullWSDBSetup(c)

		newTTL0 := test.getTTL(c, files[0])
		newTTL1 := test.getTTL(c, files[1])

		// check that refresh happened only in second workspace
		test.Assert(newTTL0 <= oldTTLs[0],
			"TTL for file-0 got refreshed, old: %d new:%d",
			oldTTLs[0], newTTL0)
		test.Assert(newTTL1 > oldTTLs[1],
			"TTL for file-1 not refreshed, old: %d new: %d",
			oldTTLs[1], newTTL1)

	})
}

// TestWriteStatPoints_NonOptimal checks if the information in
// context can be converted to stat information
func TestWriteStatPoints_NonOptimal(t *testing.T) {
	runTest(t, func(test *testHelper) {

		c := test.testCtx()
		w := wsDetails{
			ts:     "t",
			ns:     "n",
			ws:     "w",
			rootID: "xx",
		}
		AddPointWalkerHeartBeat(c)
		AddPointWalkerWorkspace(c, w, true, 1*time.Second, "no errors")
		AddPointWalkerIteration(c, 1*time.Second)
	})
}

// TestWriteStatPoints_Optimal checks if the information in
// context can be converted to stat information
func TestWriteStatPoints_Optimal(t *testing.T) {
	runTest(t, func(test *testHelper) {

		c := test.testCtx()
		c.skipMap = utils.NewSkipMap(5)
		w := wsDetails{
			ts:     "t",
			ns:     "n",
			ws:     "w",
			rootID: "xx",
		}
		AddPointWalkerHeartBeat(c)
		AddPointWalkerWorkspace(c, w, true, 1*time.Second, "no errors")
		AddPointWalkerIteration(c, 1*time.Second)
	})
}

// TestSkipWsWrittenSince checks if workspaces are skipped
// from walk based on provided duration.
func TestSkipWsWrittenSince(t *testing.T) {
	runTest(t, func(test *testHelper) {

		c := test.testCtx()
		workspaces, files, oldTTLs := test.setupWsFilesTTLs(c, 3)
		dummyErrFmt := "dummy error: %s"
		c.wsLastWriteTime = func(c *Ctx, ts, ns,
			ws string) (time.Time, error) {
			wsFullPath := fmt.Sprintf("%s/%s/%s", ts, ns, ws)
			switch wsFullPath {
			case workspaces[1]:
				return time.Now(), nil
			case workspaces[2]:
				return time.Now(), fmt.Errorf(dummyErrFmt,
					"error in getting last write time")
			default:
				return time.Unix(0, 0), nil
			}
		}
		c.lwDuration = time.Hour

		walkFullWSDBSetup(c)

		newTTL0 := test.getTTL(c, files[0])
		newTTL1 := test.getTTL(c, files[1])
		newTTL2 := test.getTTL(c, files[2])

		// refresh should happen for workspaces[0]
		// refresh should not happen for workspaces[1]
		//   due to the last write time being very recent
		// refresh should happen for workspaces[2]
		//   due to error in finding last write time
		test.Assert(newTTL0 > oldTTLs[0],
			"TTL for file-0 did not refresh, old: %d new: %d",
			oldTTLs[0], newTTL0)
		test.Assert(newTTL1 <= oldTTLs[1],
			"TTL for file-1 refreshed, old: %d new: %d",
			oldTTLs[1], newTTL1)
		test.Assert(newTTL2 > oldTTLs[2],
			"TTL for file-2 did not refresh, old: %d new: %d",
			oldTTLs[2], newTTL2)

		test.ShouldFailLogscan = true
	})

}
