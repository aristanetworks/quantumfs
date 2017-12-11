// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/aristanetworks/ether/cql"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	"github.com/aristanetworks/quantumfs/walker"
	"github.com/aristanetworks/qubit/tools/qwalker/utils"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

func (t *testHelper) testCtx() *Ctx {
	datastore := t.GetDataStore()
	translator := datastore.(*thirdparty_backends.EtherBlobStoreTranslator)

	return &Ctx{
		qctx:  t.QfsCtx(),
		wsdb:  t.GetWorkspaceDB(),
		ds:    datastore,
		cqlds: translator.Blobstore,
		ttlCfg: &qubitutils.TTLConfig{
			SkipMapResetAfter_ms: 500,
			TTLNew:               600,
		},
		numWalkers: 1,
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
func TestRefreshTTLCache(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()
		directory := workspace + "/dirA/dirB/dirC"
		file := directory + "/file"

		test.AssertNoErr(os.MkdirAll(directory, 0777))
		test.AssertNoErr(testutils.PrintToFile(file, "file data"))

		test.SyncAllWorkspaces()

		c := test.testCtx()
		go walkFullWSDBLoop(c, false)

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

		skipMap := utils.NewSkipMap(c.ttlCfg.SkipMapMaxLen)
		walkFullWSDBSetup(c, skipMap)

		cacheLen, mapLen := skipMap.Len()
		test.Assert(cacheLen == mapLen, "Lru and map mismatch")
		// The cache should be full, given the number of files we made
		test.Assert(cacheLen == c.ttlCfg.SkipMapMaxLen,
			"Max length not obeyed: %d", cacheLen)

		// No matter how many times we walk, the map length should be obeyed
		walkFullWSDBSetup(c, skipMap)

		cacheLen, mapLen = skipMap.Len()
		test.Assert(cacheLen == mapLen, "Lru and map mismatch")
		test.Assert(cacheLen == c.ttlCfg.SkipMapMaxLen,
			"Max length not obeyed: %d", cacheLen)
	})
}
