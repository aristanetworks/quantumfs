// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that different parts of Hardlink support are working

import "testing"
import "github.com/aristanetworks/quantumfs"

func TestHardlinkReload(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		// Create a couple files so we can copy its directory record
		data := genData(2000)
		testFileA := workspace + "/testFile"
		err := printToFile(testFileA, string(data[:1000]))
		test.assertNoErr(err)

		testFileB := workspace + "/testFileB"
		err = printToFile(testFileB, string(data))
		test.assertNoErr(err)

		// artificially insert some hardlinks into the map
		wsr := test.getWorkspaceRoot(workspace)

		files := wsr.children.records()
		for i := uint64(0); i < uint64(len(files)); i++ {
			record := files[i].(*quantumfs.DirectoryRecord)
			wsr.hardlinks[i] = record
			wsr.dirtyLinks[InodeId(i)] = i
		}

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

		test.assert(len(wsr.hardlinks) == len(wsrB.hardlinks),
			"Hardlink map length not preserved: %v %v", wsr.hardlinks,
			wsrB.hardlinks)
		test.assert(len(wsrB.dirtyLinks) == 0,
			"Dirty state not clean after branch: %d", wsrB.dirtyLinks)

		for k, v := range wsr.hardlinks {
			linkBPtr, exists := wsrB.hardlinks[k]
			linkB := *linkBPtr
			test.assert(exists, "link not reloaded in new wsr")
			test.assert(v.Filename() == linkB.Filename(),
				"Filename not preserved")
			test.assert(v.Type() == linkB.Type(), "Type not preserved")
			test.assert(v.ID().String() == linkB.ID().String(),
				"ID not preserved")
			test.assert(v.Size() == linkB.Size(), "Size not preserved")
			test.assert(v.ModificationTime() == linkB.ModificationTime(),
				"Modtime not preserved")
			test.assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
			test.assert(v.Permissions() == linkB.Permissions(),
				"Permissions not preserved")
			test.assert(v.Owner() == linkB.Owner(),
				"OwnerID not preserved")
			test.assert(v.Group() == linkB.Group(),
				"GroupID not preserved")
			test.assert(v.ExtendedAttributes().String() ==
				linkB.ExtendedAttributes().String(),
				"ExtendedAttributes not preserved")
			test.assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
			test.assert(v.ContentTime() == linkB.ContentTime(),
				"ContentTime not preserved")
		}
	})
}
