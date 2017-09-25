// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test workspace merging

import (
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs/testutils"
)

type baseSetup func(base string)
type mergeTestCheck func(merged string)
type mergeTestSetup func(branchA string, branchB string) mergeTestCheck

func MergeTester(test *testHelper, base baseSetup, setup mergeTestSetup) {
	workspaceBase := test.NewWorkspace()

	if base != nil {
		base(workspaceBase)
	}

	api := test.getApi()
	workspaceA := test.AbsPath("test/workspaceA/test")
	workspaceB := test.AbsPath("test/workspaceB/test")

	test.AssertNoErr(api.Branch(test.RelPath(workspaceBase),
		test.RelPath(workspaceA)))
	test.AssertNoErr(api.Branch(test.RelPath(workspaceBase),
		test.RelPath(workspaceB)))
	test.AssertNoErr(api.EnableRootWrite(test.RelPath(workspaceA)))
	test.AssertNoErr(api.EnableRootWrite(test.RelPath(workspaceB)))

	check := setup(workspaceA, workspaceB)

	// merge and create a new branch
	test.SyncAllWorkspaces()

	// Because of the buggy state of DeleteWorkspace and the fact that
	// we can't rely on workspace inodes to update when the rootId
	// changes, we have to Branch first into a workspace we never touch
	tempBranch := test.AbsPath("branch/basic/temp")
	err := api.Branch(test.RelPath(workspaceA), test.RelPath(tempBranch))
	test.AssertNoErr(err)

	if base != nil {
		err = api.Merge3Way(test.RelPath(workspaceBase),
			test.RelPath(workspaceB), test.RelPath(tempBranch))
		test.AssertNoErr(err)
	} else {
		err = api.Merge(test.RelPath(workspaceB), test.RelPath(tempBranch))
		test.AssertNoErr(err)
	}

	// Now we have to branch again so that the rootId change is
	// actually reflected in our local workspace instance
	newBranch := test.AbsPath("branch/basic/test")
	err = api.Branch(test.RelPath(tempBranch), test.RelPath(newBranch))
	test.AssertNoErr(err)
	test.AssertNoErr(api.EnableRootWrite(test.RelPath(newBranch)))

	check(newBranch)
}

func TestMergePlainFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, nil, func(branchA string,
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
		MergeTester(test, nil, func(branchA string,
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
		MergeTester(test, nil, func(branchA string,
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
		MergeTester(test, nil, func(branchA string,
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
		MergeTester(test, nil, func(branchA string,
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

func TestMergeHardlinksOverlap(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, nil, func(branchA string,
			branchB string) mergeTestCheck {

			dataA := "A data contents"
			test.AssertNoErr(os.MkdirAll(branchA+"/dirA", 0777))
			test.AssertNoErr(testutils.PrintToFile(branchA+"/dirA/fileA",
				dataA))
			test.AssertNoErr(syscall.Link(branchA+"/dirA/fileA",
				branchA+"/fileB"))

			dataC := "C data contents"
			test.AssertNoErr(os.MkdirAll(branchB+"/dirA", 0777))
			test.AssertNoErr(testutils.PrintToFile(branchB+"/dirA/fileC",
				dataC))
			test.AssertNoErr(syscall.Link(branchB+"/dirA/fileC",
				branchB+"/fileB"))
			test.AssertNoErr(syscall.Link(branchB+"/dirA/fileC",
				branchB+"/fileD"))

			test.AssertNoErr(syscall.Link(branchA+"/dirA/fileA",
				branchA+"/fileD"))

			return func(merged string) {
				test.CheckLink(merged+"/fileB", []byte(dataC), 2)
				test.CheckLink(merged+"/fileD", []byte(dataA), 2)
			}
		})
	})
}

func TestMergeTraverse(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, nil, func(branchA string,
			branchB string) mergeTestCheck {

			dataA := "A data contents"
			dataB := "B data"
			dirA := "/dirA"
			dirD := dirA + "/dirB/dirC/dirD"

			test.AssertNoErr(os.MkdirAll(branchB+dirD, 0777))
			test.AssertNoErr(testutils.PrintToFile(branchB+dirD+"/fileA",
				dataA))
			test.AssertNoErr(syscall.Link(branchB+dirD+"/fileA",
				branchB+dirD+"/linkB"))

			test.AssertNoErr(os.MkdirAll(branchA+dirA, 0777))
			test.AssertNoErr(testutils.PrintToFile(branchA+dirA+"/fileB",
				dataB))
			test.AssertNoErr(syscall.Link(branchA+dirA+"/fileB",
				branchA+dirA+"/linkA"))
			test.AssertNoErr(syscall.Link(branchA+dirA+"/fileB",
				branchA+dirA+"/linkC"))

			test.AssertNoErr(syscall.Link(branchB+dirD+"/fileA",
				branchB+dirA+"/linkA"))

			return func(merged string) {
				test.CheckLink(merged+dirD+"/fileA", []byte(dataA),
					3)
				test.CheckLink(merged+dirA+"/linkA", []byte(dataA),
					3)
				test.CheckLink(merged+dirA+"/fileB", []byte(dataB),
					2)
			}
		})
	})
}

func TestMergeOneLeft(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, nil, func(branchA string,
			branchB string) mergeTestCheck {

			dataA := "dataA contents"
			dataB := "B data"

			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileA",
				dataA))
			test.AssertNoErr(syscall.Link(branchA+"/fileA", branchA+
				"/linkA"))

			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileB",
				dataB))
			test.AssertNoErr(syscall.Link(branchB+"/fileB", branchB+
				"/linkB"))

			dataC := "CCCC"
			dataD := "DDDD"
			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileA",
				dataC))
			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileB",
				dataD))

			return func(merged string) {
				test.CheckData(merged+"/fileA", []byte(dataC))
				test.CheckData(merged+"/fileB", []byte(dataD))
				test.CheckData(merged+"/linkA", []byte(dataA))
				test.CheckData(merged+"/linkB", []byte(dataB))
			}
		})
	})
}

func TestMergeRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, nil, func(branchA string,
			branchB string) mergeTestCheck {

			dataA := "dataA contents"
			dataB := "B data"

			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileA",
				dataA))
			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileB",
				dataB))

			test.AssertNoErr(syscall.Link(branchA+"/fileA", branchA+
				"/fileC"))
			test.AssertNoErr(syscall.Link(branchB+"/fileB", branchB+
				"/fileD"))

			test.AssertNoErr(syscall.Rename(branchA+"/fileC", branchA+
				"/fileB"))
			test.AssertNoErr(syscall.Rename(branchB+"/fileD", branchB+
				"/fileA"))

			return func(merged string) {
				test.CheckData(merged+"/fileA", []byte(dataB))
				test.CheckData(merged+"/fileB", []byte(dataA))
			}
		})
	})
}

func TestMergeSameFileId(t *testing.T) {
	runTest(t, func(test *testHelper) {
		dataA := "dataA contents"

		MergeTester(test, func(baseWorkspace string) {
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/fileA", dataA))

		}, func(branchA string, branchB string) mergeTestCheck {
			// fileB will exist in both local and remote, different from
			// base, but with the same FileId
			test.AssertNoErr(syscall.Link(branchB+"/fileA", branchB+
				"/fileB"))
			test.AssertNoErr(syscall.Link(branchB+"/fileA", branchB+
				"/fileC"))
			test.AssertNoErr(os.Rename(branchA+"/fileA",
				branchA+"/fileB"))

			return func(merged string) {
				test.assertNoFile(merged + "/fileA")

				testutils.PrintToFile(merged+"/fileC", "extra data")
				dataA += "extra data"

				// ensure the hardlinks are preserved
				test.CheckData(merged+"/fileC", []byte(dataA))
				test.CheckData(merged+"/fileB", []byte(dataA))
			}
		})
	})
}

func TestMergeDeletions(t *testing.T) {
	runTest(t, func(test *testHelper) {
		dirA := "/dirA3/dirA2/dirA"
		dirB := "/dirB3/dirB2/dirB"
		fileA := "/fileA"
		fileB := "/fileB"

		var dataA, dataB []byte

		MergeTester(test, func(baseWorkspace string) {
			dataA = test.MakeFile(baseWorkspace + dirA + fileA)
			dataB = test.MakeFile(baseWorkspace + dirB + fileB)
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(syscall.Link(branchA+dirA+fileA,
				branchA+fileA))
			test.AssertNoErr(syscall.Link(branchA+fileA, branchA+fileB))
			test.AssertNoErr(os.RemoveAll(branchA + dirA))

			test.AssertNoErr(syscall.Link(branchB+dirB+fileB,
				branchB+fileB))
			test.AssertNoErr(syscall.Link(branchB+fileB, branchB+fileA))
			test.AssertNoErr(os.RemoveAll(branchB + dirB))

			return func(merged string) {
				test.assertNoFile(merged + dirA)
				test.assertNoFile(merged + dirB)
				test.CheckData(merged+fileA, dataB)
				test.CheckData(merged+fileB, dataB)
			}
		})
	})
}

func TestMergeIntraFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(baseWorkspace string) {

		})
	})
}
