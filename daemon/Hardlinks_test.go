// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that different parts of Hardlink support are working

import "testing"
import "github.com/aristanetworks/quantumfs"

func TestHardlinkReload(t *testing.T) {
	runTest(t, func(test *testHelper) {
if false {
		workspace := test.newWorkspace()

		// Create a couple files so we can copy its directory record
		data := genData(2000)
		testFileA := workspace + "/testFile"
		err := printToFile(testFileA, string(data[:1000]))
		test.assertNoErr(err)

		testFileB := workspace +"/testFileB"
		err = printToFile(testFileB, string(data))
		test.assertNoErr(err)

		// artificially insert some hardlinks into the map
		wsr := test.getWorkspaceRoot(workspace)

		wsr.lock.Lock()
		files := wsr.children.records()
		for i := uint64(0); i < uint64(len(files)); i++ {
			record := files[i].(*quantumfs.DirectoryRecord)
			wsr.hardlinks[i] = record
			wsr.dirtyLinks[InodeId(i)] = InodeId(i)
		}
		wsr.lock.Unlock()

		// Write another file to ensure the wsr is dirty
		testFileC := workspace + "/testFileC"
		err = printToFile(testFileC, string(data[:1000]))
		test.assertNoErr(err)

		// trigger a sync so the workspace is published
		test.syncAllWorkspaces()

		workspaceB := "copyWorkspace/test"
		api := test.getApi()
		err = api.Branch(test.relPath(workspace), workspaceB)
		test.assert(err == nil, "Unable to branch")

		wsrB := test.getWorkspaceRoot(workspaceB)
		wsr.lock.RLock()
		defer wsr.lock.RUnlock()
		wsrB.lock.RLock()
		defer wsrB.lock.RUnlock()

		test.assert(len(wsr.hardlinks) == len(wsrB.hardlinks),
			"Hardlink map length not preserved: %v %v", wsr.hardlinks,
			wsrB.hardlinks)
		test.assert(len(wsrB.dirtyLinks) == 0,
			"Dirty state not clean after branch: %d", wsrB.dirtyLinks)

		for i := uint64(0); i < uint64(len(wsr.hardlinks)); i++ {
			test.assert(*(wsr.hardlinks[i]) == *(wsrB.hardlinks[i]),
				"Hardlink not reloaded right.")
		}
}
	})
}
