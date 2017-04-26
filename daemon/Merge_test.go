// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test workspace merging

import "os"
import "syscall"
import "testing"


func TestBasicMerge(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspaceA := test.NewWorkspace()
		workspaceB := test.NewWorkspace()

		fileA := workspaceA + "/fileA"
		fileB := workspaceB + "/fileB"
		fileC := workspaceA + "/subdir/fileC"
		fileC2 := workspaceB + "/subdir/fileC"
		fileD := workspaceA + "/subdir/fileD"
		fileD2 := workspaceB + "/subdir/fileD"

		err := os.MkdirAll(workspaceA + "/subdir", 0777)
		test.AssertNoErr(err)
		err = os.MkdirAll(workspaceB + "/subdir", 0777)
		test.AssertNoErr(err)

		dataA := test.MakeFile(fileA)
		dataB := test.MakeFile(fileB)
		test.MakeFile(fileC)
		dataC2 := test.MakeFile(fileC2)
		// reverse the order for the D files
		test.MakeFile(fileD2)
		dataD := test.MakeFile(fileD)

		test.SyncAllWorkspaces()
		api := test.getApi()

		// Because of the buggy state of DeleteWorkspace and the fact that
		// we can't rely on workspace inodes to update when the rootId
		// changes, we have to Branch first into a workspace we never touch
		tempBranch := test.absPath("branch/basic/temp")
		err = api.Branch(test.RelPath(workspaceA), test.RelPath(tempBranch))
		test.AssertNoErr(err)

		err = api.Merge(test.RelPath(workspaceB), test.RelPath(tempBranch))
		test.AssertNoErr(err)

		// Now we have to branch again so that the rootId change is
		// actually reflected in our local workspace instance
		newBranch := test.absPath("branch/basic/test")
		err = api.Branch(test.RelPath(tempBranch), test.RelPath(newBranch))
		test.AssertNoErr(err)

		fileA = newBranch + "/fileA"
		fileB = newBranch + "/fileB"
		fileC = newBranch + "/subdir/fileC"
		fileD = newBranch + "/subdir/fileD"

		test.CheckData(fileA, dataA)
		test.CheckData(fileB, dataB)
		// ensure we took remote
		test.CheckData(fileC, dataC2)
		// ensure we took local
		test.CheckData(fileD, dataD)
	})
}

func TestSpecialsMerge(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspaceA := test.NewWorkspace()
		workspaceB := test.NewWorkspace()

		fileA := workspaceA + "/fileA"
		fileB := workspaceB + "/fileB"
		symlinkA := workspaceA + "/symlink"
		symlinkB := workspaceB + "/symlink"
		symlinkB2 := workspaceB + "/symlink2"
		symlinkA2 := workspaceA + "/symlink2"
		specialA := workspaceA + "/special"
		specialB := workspaceB + "/special"

		dataA := test.MakeFile(fileA)
		dataB := test.MakeFile(fileB)
		err := syscall.Symlink(fileA, symlinkA)
		test.AssertNoErr(err)
		err = syscall.Symlink(fileB, symlinkB)
		test.AssertNoErr(err)
		err = syscall.Symlink(fileB, symlinkB2)
		test.AssertNoErr(err)
		err = syscall.Symlink(fileA, symlinkA2)
		test.AssertNoErr(err)

		err = syscall.Mknod(specialA, syscall.S_IFCHR, 0x12345678)
		test.AssertNoErr(err)
		err = syscall.Mknod(specialB, syscall.S_IFBLK, 0x11111111)
		test.AssertNoErr(err)

		statA := test.SysStat(fileA)
		statB := test.SysStat(fileB)
		statLinkB := test.SysLstat(symlinkB)
		statLinkA2 := test.SysLstat(symlinkA2)
		statSpecB := test.SysStat(specialB)

		test.SyncAllWorkspaces()

		api := test.getApi()
		err = api.Merge(test.RelPath(workspaceB), test.RelPath(workspaceA))
		test.AssertNoErr(err)
if false {
		// to reload the workspaceA rootID, branch it
		newBranch := test.absPath("branch/specials/test")
		err = api.Branch(test.RelPath(workspaceA), test.RelPath(newBranch))
		test.AssertNoErr(err)

		fileA = newBranch + "/fileA"
		fileB = newBranch + "/fileB"
		symlinkA = newBranch + "/symlink"
		symlinkA2 = newBranch + "/symlink2"
		specialB = newBranch + "/special"
}
		test.remountFilesystem()

		test.CheckData(fileA, dataA)
		// symlink should be overwritten and pointing to fileB
		test.CheckData(symlinkA, dataB)
		test.CheckData(fileB, dataB)
		// remote should have been overwritten this time
		test.CheckData(symlinkA2, dataA)

		// Check the stats
		test.Assert(test.SysStat(fileA) == statA, "fileA changed")
		test.Assert(test.SysStat(fileB) == statB, "fileB changed")
		test.Assert(test.SysLstat(symlinkA) == statLinkB,
			"symlink not changed")
		test.Assert(test.SysLstat(symlinkA2) == statLinkA2,
			"symlink not preserved")
		test.Assert(test.SysStat(specialB) == statSpecB,
			"special not changed")

	})
}
