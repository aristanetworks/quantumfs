// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test workspace merging

import "os"
import "syscall"
import "testing"

type mergeTestCheck func(merged string)
type mergeTestSetup func(branchA string, branchB string) mergeTestCheck

func MergeTester(test *testHelper, setup mergeTestSetup) {
	workspaceA := test.NewWorkspace()
	workspaceB := test.NewWorkspace()

	check := setup(workspaceA, workspaceB)

	// merge and create a new branch
	test.SyncAllWorkspaces()
	api := test.getApi()

	// Because of the buggy state of DeleteWorkspace and the fact that
	// we can't rely on workspace inodes to update when the rootId
	// changes, we have to Branch first into a workspace we never touch
	tempBranch := test.AbsPath("branch/basic/temp")
	err := api.Branch(test.RelPath(workspaceA), test.RelPath(tempBranch))
	test.AssertNoErr(err)

	err = api.Merge(test.RelPath(workspaceB), test.RelPath(tempBranch))
	test.AssertNoErr(err)

	// Now we have to branch again so that the rootId change is
	// actually reflected in our local workspace instance
	newBranch := test.AbsPath("branch/basic/test")
	err = api.Branch(test.RelPath(tempBranch), test.RelPath(newBranch))
	test.AssertNoErr(err)

	check(newBranch)
}

func TestMergePlainFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(branchA string,
			branchB string) mergeTestCheck {

			test.MakeFile(branchA + "/fileA")
			dataB := test.MakeFile(branchB + "/fileA")
			test.MakeFile(branchB + "/fileB")
			dataA2 := test.MakeFile(branchA + "/fileB")

			return func(merged string) {
				test.CheckData(merged+"/fileA", dataB)
				test.CheckData(merged+"/fileB", dataA2)
			}
		})
	})
}

func TestMergePlainSubdir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(branchA string,
			branchB string) mergeTestCheck {

			test.MakeFile(branchA + "/subdir/fileA")
			dataB := test.MakeFile(branchB + "/subdir/fileA")
			test.MakeFile(branchB + "/subdir/fileB")
			dataA2 := test.MakeFile(branchA + "/subdir/fileB")

			return func(merged string) {
				test.CheckData(merged+"/subdir/fileA", dataB)
				test.CheckData(merged+"/subdir/fileB", dataA2)
			}
		})
	})
}

func TestMergeSymlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(branchA string,
			branchB string) mergeTestCheck {

			test.MakeFile(branchA + "/subdir/fileA")
			dataB := test.MakeFile(branchB + "/subdir/fileB")
			err := syscall.Symlink(branchA+"/subdir/fileA",
				branchA+"/symlink")
			test.AssertNoErr(err)
			err = syscall.Symlink(branchB+"/subdir/fileB",
				branchB+"/symlink")

			return func(merged string) {
				test.CheckData(merged+"/symlink", dataB)
			}
		})
	})
}

func TestMergeSpecial(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(branchA string,
			branchB string) mergeTestCheck {

			err := syscall.Mknod(branchA+"/special",
				syscall.S_IFCHR, 0x12345678)
			test.AssertNoErr(err)
			err = syscall.Mknod(branchB+"/special",
				syscall.S_IFBLK, 0x11111111)
			test.AssertNoErr(err)

			statSpecB := test.SysStat(branchB + "/special")

			return func(merged string) {
				specialStats := test.SysStat(merged + "/special")
				test.Assert(specialStats.Rdev == statSpecB.Rdev,
					"special Rdev changed %x vs %x",
					specialStats.Rdev, statSpecB.Rdev)
				test.Assert(specialStats.Mode == statSpecB.Mode,
					"special Mode changed %x vs %x",
					specialStats.Mode, statSpecB.Mode)
				test.Assert(specialStats.Mtim == statSpecB.Mtim,
					"special Modtime changed %v vs %v",
					specialStats.Mtim, statSpecB.Mtim)
			}
		})
	})
}

func TestMergeDifferentTypes(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(branchA string,
			branchB string) mergeTestCheck {

			err := os.MkdirAll(branchA+"/fileA/fileisadir", 0777)
			test.AssertNoErr(err)
			dataB := test.MakeFile(branchB + "/fileA")

			err = os.MkdirAll(branchB+"/fileB/fileisadir", 0777)
			test.AssertNoErr(err)
			dataA2 := test.MakeFile(branchA + "/fileB")

			return func(merged string) {
				test.CheckData(merged+"/fileA", dataB)
				test.CheckData(merged+"/fileB", dataA2)
			}
		})
	})
}
