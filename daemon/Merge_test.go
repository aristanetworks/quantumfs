// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test workspace merging

import (
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

type baseSetup func(base string)
type mergeTestCheck func(merged string)
type mergeTestSetup func(branchA string, branchB string) mergeTestCheck

func MergeTester(test *testHelper, base baseSetup, setup mergeTestSetup) {
	MergeTesterWithSkip(test, []string{}, base, setup)
}

func MergeTesterWithSkip(test *testHelper, skipPaths []string, base baseSetup,
	setup mergeTestSetup) {

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
			test.RelPath(workspaceB), test.RelPath(tempBranch),
			quantumfs.PreferNewer, skipPaths)
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
				// intra file shouldn't happen with diff FileIds
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
			dataB := test.MakeFile(branchB + "/subdir/fileC")
			// Files are only ignorantly overwritten if one isn't the
			// same regular file type
			test.AssertNoErr(syscall.Symlink(branchB+"/subdir/fileC",
				branchB+"/subdir/fileA"))
			test.MakeFile(branchB + "/subdir/fileB")
			dataA2 := test.MakeFile(branchA + "/subdir/fileD")
			test.AssertNoErr(syscall.Symlink(branchA+"/subdir/fileD",
				branchA+"/subdir/fileB"))

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
				test.CheckLink(merged+"/fileA", []byte(dataC), 1)
				test.CheckLink(merged+"/fileB", []byte(dataD), 1)
				test.CheckLink(merged+"/linkA", []byte(dataA), 1)
				test.CheckLink(merged+"/linkB", []byte(dataB), 1)
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
				test.CheckLink(merged+"/fileA", []byte(dataB), 1)
				test.CheckLink(merged+"/fileB", []byte(dataA), 1)
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
				test.CheckLink(merged+"/fileC", []byte(dataA), 2)
				test.CheckLink(merged+"/fileB", []byte(dataA), 2)
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

		var dataB []byte

		MergeTester(test, func(baseWorkspace string) {
			test.MakeFile(baseWorkspace + dirA + fileA)
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

func TestMergeIntraFileNoBase(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(baseWorkspace string) {
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/fileA", ""))
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/fileB", ""))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			sharedDataA := string(GenData(1000000))
			conflictDataB1 := "This is some data"
			conflictDataB2 := "And..is this not?"
			sharedDataC := string(GenData(300000))
			conflictDataD1 := "Data at the end of file"
			conflictDataD2 := "Data that doesn't match"
			extendedDataE := "Extra data on one file"

			dataA := sharedDataA + conflictDataB1 + sharedDataC +
				conflictDataD1 + extendedDataE
			dataB := sharedDataA + conflictDataB2 + sharedDataC +
				conflictDataD2

			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileA",
				dataA))

			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileA",
				dataB))
			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileB",
				dataA))

			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileB",
				dataB))

			checkData := dataB + extendedDataE

			return func(merged string) {
				test.CheckData(merged+"/fileA", []byte(checkData))
				test.CheckData(merged+"/fileB", []byte(checkData))
			}
		})
	})
}

func TestMergeIntraFileDiffTypes(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(baseWorkspace string) {
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/fileA", ""))
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/fileB", ""))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			// create a small file and a large file and ensure that they
			// merge contents correctly
			dataC := GenData(1000 + (quantumfs.MaxBlockSize *
				quantumfs.MaxBlocksMediumFile()))
			mediumLen := 2 * quantumfs.MaxBlockSize
			dataB := GenData(3 * quantumfs.MaxBlockSize)[:mediumLen]
			dataA := "Small file data"

			// test small file merged with a medium file
			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileA",
				string(dataB)))
			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileA",
				dataA))

			// test a medium file merged with a large file
			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileB",
				string(dataB)))
			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileB",
				string(dataC)))

			return func(merged string) {
				resultA := make([]byte, len(dataB), len(dataB))
				copy(resultA, dataB)
				copy(resultA, dataA)

				// the merge result should be a simple combination
				test.CheckData(merged+"/fileA", resultA)
				test.CheckData(merged+"/fileB", dataC)
			}
		})
	})
}

func TestMergeIntraFileBase(t *testing.T) {
	runTest(t, func(test *testHelper) {
		sharedDataA := string(GenData(1000000))
		conflictDataB0 := "00000000000000000"
		conflictDataB1 := "This is some data"
		conflictDataB2 := "And..is this not?"
		sharedDataC := string(GenData(300000))
		conflictDataD0 := "0000AAAAA0000BBBBB0000"
		conflictDataD1 := "0000AAAAA0000FOODS0000"
		conflictDataD2 := "0000BEEFS0000BBBBB0000"
		extendedDataE := "Extra data on one file"

		MergeTester(test, func(baseWorkspace string) {
			baseData := sharedDataA + conflictDataB0 + sharedDataC +
				conflictDataD0
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+"/file",
				baseData))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(testutils.OverWriteFile(branchA+"/file",
				sharedDataA+conflictDataB1+sharedDataC+
					conflictDataD1+extendedDataE))
			test.AssertNoErr(testutils.OverWriteFile(branchB+"/file",
				sharedDataA+conflictDataB2+sharedDataC+
					conflictDataD2))

			return func(merged string) {
				resultD := conflictDataD2[:10] + conflictDataD1[10:]

				test.CheckData(merged+"/file", []byte(sharedDataA+
					conflictDataB2+sharedDataC+resultD+
					extendedDataE))
			}
		})
	})
}

// Test cases where conflicting files matching in FileId, but have no base ref
func TestMergeIntraFileMissingBase(t *testing.T) {
	runTest(t, func(test *testHelper) {
		extension := "12345"
		data1 := "0000AAAA00000000" + extension
		data2 := "000000000000BBBB"

		MergeTester(test, func(baseWorkspace string) {
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/fileB", ""))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(os.Rename(branchA+"/fileB",
				branchA+"/fileC"))
			test.AssertNoErr(os.Rename(branchB+"/fileB",
				branchB+"/fileC"))

			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileC",
				data1))
			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileC",
				data2))

			return func(merged string) {
				// Without a base reference, most of data1 is lost
				result := []byte(data2 + extension)

				test.CheckData(merged+"/fileC", result)
			}
		})
	})
}

func TestMergeIntraFileBaseHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		sharedDataA := string(GenData(1000000))
		conflictDataB0 := "00000000000000000"
		conflictDataB1 := "This is some data"
		conflictDataB2 := "And..is this not?"
		sharedDataC := string(GenData(300000))
		conflictDataD0 := "0000AAAAA0000BBBBB0000"
		conflictDataD1 := "0000AAAAA0000FOODS0000"
		conflictDataD2 := "0000BEEFS0000BBBBB0000"
		extendedDataE := "Extra data on one file"

		MergeTester(test, func(baseWorkspace string) {
			baseData := sharedDataA + conflictDataB0 + sharedDataC +
				conflictDataD0

			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/linkA", baseData))
			// Link it so base has a hardlink table entry for it
			test.AssertNoErr(syscall.Link(baseWorkspace+"/linkA",
				baseWorkspace+"/baseLinked"))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(testutils.OverWriteFile(branchA+"/linkA",
				sharedDataA+conflictDataB1+sharedDataC+
					conflictDataD1+extendedDataE))
			test.AssertNoErr(testutils.OverWriteFile(branchB+"/linkA",
				sharedDataA+conflictDataB2+sharedDataC+
					conflictDataD2))

			return func(merged string) {
				resultD := conflictDataD2[:10] + conflictDataD1[10:]

				test.CheckLink(merged+"/linkA", []byte(sharedDataA+
					conflictDataB2+sharedDataC+resultD+
					extendedDataE), 2)
			}
		})
	})
}

// Test cases where conflicting files matchin in FileId, but have no base ref
func TestMergeIntraFileMissingBaseHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		extension := "12345"
		data1 := "0000AAAA00000000" + extension
		data2 := "000000000000BBBB"

		MergeTester(test, func(baseWorkspace string) {
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/fileA", ""))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(syscall.Link(branchA+"/fileA", branchA+
				"/fileD"))
			test.AssertNoErr(syscall.Link(branchA+"/fileA", branchA+
				"/fileE"))
			test.AssertNoErr(os.Remove(branchA + "/fileA"))
			test.AssertNoErr(syscall.Link(branchB+"/fileA", branchB+
				"/fileD"))
			test.AssertNoErr(syscall.Link(branchB+"/fileA", branchB+
				"/fileF"))
			test.AssertNoErr(os.Remove(branchB + "/fileA"))

			test.AssertNoErr(testutils.PrintToFile(branchA+"/fileD",
				data1))
			test.AssertNoErr(testutils.PrintToFile(branchB+"/fileD",
				data2))

			return func(merged string) {
				// Without a base reference, most of data1 is lost
				result := []byte(data2 + extension)

				test.CheckLink(merged+"/fileD", result, 3)
			}
		})
	})
}

func TestMergeIntraRecordThreeWay(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(baseWorkspace string) {
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/file", "sample data"))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(os.Chmod(branchA+"/file", 0444))
			test.AssertNoErr(os.Chown(branchB+"/file", 123, 456))

			var statB syscall.Stat_t
			test.AssertNoErr(syscall.Stat(branchB+"/file", &statB))

			return func(merged string) {
				var stat syscall.Stat_t
				test.AssertNoErr(syscall.Stat(merged+"/file", &stat))

				test.Assert(stat.Uid == 123, "Uid not merged")
				test.Assert(stat.Gid == 456, "Gid not merged")
				test.Assert(stat.Mode&0777 == 0444,
					"Mode not merged")
				test.Assert(stat.Ctim == statB.Ctim,
					"Ctime not chosen correctly")
				test.Assert(stat.Mtim == statB.Mtim,
					"Mtime not chosen correctly")
			}
		})
	})
}

func TestMergeIntraRecordBaseMismatch(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(baseWorkspace string) {
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/file", "sample data"))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(os.Chmod(branchA+"/file", 0444))
			test.AssertNoErr(os.Chmod(branchB+"/file", 0333))
			test.AssertNoErr(os.Chown(branchB+"/file", 123, 456))
			test.AssertNoErr(os.Chown(branchA+"/file", 234, 345))

			var statA syscall.Stat_t
			test.AssertNoErr(syscall.Stat(branchA+"/file", &statA))

			return func(merged string) {
				var stat syscall.Stat_t
				test.AssertNoErr(syscall.Stat(merged+"/file", &stat))

				test.Assert(stat.Uid == 234, "Uid not merged")
				test.Assert(stat.Gid == 345, "Gid not merged")
				test.Assert(stat.Mode&0777 == 0444,
					"Mode not merged")
				test.Assert(stat.Ctim == statA.Ctim,
					"Ctime not chosen correctly")
				test.Assert(stat.Mtim == statA.Mtim,
					"Mtime not chosen correctly")
			}
		})
	})
}

func TestMergeExtendedAttrs(t *testing.T) {
	runTest(t, func(test *testHelper) {
		dataA := []byte("AAA")
		dataB := []byte("BBB")
		dataC := []byte("CCC")
		dataD := []byte("DDD")
		dataE := []byte("EEE")
		dataF := []byte("FFF")

		MergeTester(test, func(baseWorkspace string) {
			test.AssertNoErr(testutils.PrintToFile(baseWorkspace+
				"/file", "sample data"))

			test.AssertNoErr(syscall.Setxattr(baseWorkspace+"/file",
				"user.conflict", dataA, 0))
			test.AssertNoErr(syscall.Setxattr(baseWorkspace+"/file",
				"user.todelete", dataD, 0))
			test.AssertNoErr(syscall.Setxattr(baseWorkspace+"/file",
				"user.todelete2", dataD, 0))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(syscall.Setxattr(branchA+"/file",
				"user.conflict", dataE, 0))
			test.AssertNoErr(syscall.Setxattr(branchB+"/file",
				"user.conflict", dataF, 0))

			test.AssertNoErr(syscall.Setxattr(branchA+"/file",
				"user.todelete", []byte("throw away data"), 0))
			test.AssertNoErr(syscall.Removexattr(branchB+"/file",
				"user.todelete"))

			test.AssertNoErr(syscall.Removexattr(branchB+"/file",
				"user.todelete2"))
			test.AssertNoErr(syscall.Setxattr(branchA+"/file",
				"user.todelete2", []byte("throw away data2"), 0))

			test.AssertNoErr(syscall.Setxattr(branchA+"/file",
				"user.branchA", dataB, 0))
			test.AssertNoErr(syscall.Setxattr(branchB+"/file",
				"user.branchB", dataC, 0))

			return func(merged string) {
				test.verifyXattr(merged, "file", "user.conflict",
					dataF)
				test.verifyXattr(merged, "file", "user.branchA",
					dataB)
				test.verifyXattr(merged, "file", "user.branchB",
					dataC)
				test.verifyNoXattr(merged, "file", "user.todelete")
				test.verifyNoXattr(merged, "file", "user.todelete2")
			}
		})
	})
}

func TestMergeRecreateFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(bws string) {
			test.AssertNoErr(testutils.PrintToFile(bws+"/local",
				"base"))
			test.AssertNoErr(testutils.PrintToFile(bws+"/remote",
				"base"))
			test.AssertNoErr(testutils.PrintToFile(bws+"/newer",
				"base"))
		}, func(localWs string, remoteWs string) mergeTestCheck {
			test.AssertNoErr(testutils.OverWriteFile(remoteWs+"/remote",
				"remote"))
			test.AssertNoErr(os.Remove(remoteWs + "/newer"))
			test.AssertNoErr(testutils.PrintToFile(remoteWs+"/newer",
				"remote"))

			test.AssertNoErr(testutils.OverWriteFile(localWs+"/local",
				"local"))
			test.AssertNoErr(testutils.OverWriteFile(localWs+"/newer",
				"local"))

			return func(merged string) {
				test.CheckData(merged+"/local", []byte("local"))
				test.CheckData(merged+"/remote", []byte("remote"))
				test.CheckData(merged+"/newer", []byte("remote"))
			}
		})
	})
}

func TestMergeSkipDirectories(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTesterWithSkip(test, []string{"merge1/merge2/skip"},
			func(bws string) {
				test.AssertNoErr(utils.MkdirAll(
					bws+"/merge1/merge2/skip", 0777))
				test.AssertNoErr(testutils.PrintToFile(
					bws+"/merge1/merge2/skip/skipped",
					"skipped"))
				test.AssertNoErr(utils.MkdirAll(
					bws+"/merge3/merge4", 0777))
			}, func(branchA string, branchB string) mergeTestCheck {
				test.AssertNoErr(testutils.PrintToFile(
					branchA+"/merge1/local1", "local1"))
				test.AssertNoErr(testutils.PrintToFile(
					branchA+"/merge1/merge2/local2", "local2"))
				test.AssertNoErr(testutils.PrintToFile(
					branchA+"/merge1/merge2/skip/local3",
					"local3"))

				test.AssertNoErr(testutils.PrintToFile(
					branchB+"/merge1/remote1", "remote1"))
				test.AssertNoErr(testutils.PrintToFile(
					branchB+"/merge1/merge2/remote2", "remote2"))
				test.AssertNoErr(testutils.PrintToFile(
					branchB+"/merge1/merge2/skip/remote3",
					"remote3"))
				test.AssertNoErr(testutils.PrintToFile(
					branchB+"/merge3/merge4/remote4", "remote4"))

				return func(merged string) {
					test.assertFileExists(
						merged + "/merge1/local1")
					test.assertFileExists(
						merged + "/merge1/merge2/local2")
					test.assertFileExists(merged +
						"/merge1/merge2/skip/local3")

					test.assertFileExists(
						merged + "/merge1/remote1")
					test.assertFileExists(
						merged + "/merge1/merge2/remote2")
					test.assertNoFile(merged +
						"/merge1/merge2/skip/remote3")
					test.assertFileExists(
						merged + "/merge3/merge4/remote4")
				}
			})
	})
}

func TestMergeSkipFiles(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTesterWithSkip(test, []string{"merge1/merge2/skip", "skip"},
			func(bws string) {
				test.AssertNoErr(utils.MkdirAll(
					bws+"/merge1/merge2", 0777))
				test.AssertNoErr(testutils.PrintToFile(
					bws+"/merge1/merge2/skip",
					"base"))
				test.AssertNoErr(testutils.PrintToFile(
					bws+"/skip",
					"base"))
				test.AssertNoErr(utils.MkdirAll(
					bws+"/merge3/merge4", 0777))
			}, func(branchA string, branchB string) mergeTestCheck {
				test.AssertNoErr(testutils.PrintToFile(
					branchA+"/merge1/local1", "local1"))
				test.AssertNoErr(testutils.PrintToFile(
					branchA+"/merge1/merge2/local2", "local2"))
				test.AssertNoErr(testutils.OverWriteFile(
					branchA+"/merge1/merge2/skip", "local"))
				test.AssertNoErr(testutils.OverWriteFile(
					branchA+"/skip", "local"))

				test.AssertNoErr(testutils.PrintToFile(
					branchB+"/merge1/remote1", "remote1"))
				test.AssertNoErr(testutils.PrintToFile(
					branchB+"/merge1/merge2/remote2", "remote2"))
				test.AssertNoErr(testutils.OverWriteFile(
					branchB+"/merge1/merge2/skip", "remote"))
				test.AssertNoErr(testutils.OverWriteFile(
					branchB+"/skip", "remote"))
				test.AssertNoErr(testutils.PrintToFile(
					branchB+"/merge3/merge4/remote4", "remote4"))

				return func(merged string) {
					test.assertFileExists(
						merged + "/merge1/local1")
					test.assertFileExists(
						merged + "/merge1/merge2/local2")

					test.assertFileExists(
						merged + "/merge1/remote1")
					test.assertFileExists(
						merged + "/merge1/merge2/remote2")
					test.assertFileExists(
						merged + "/merge3/merge4/remote4")

					test.CheckData(merged+"/merge1/merge2/skip",
						[]byte("local"))
					test.CheckData(merged+"/skip",
						[]byte("local"))
				}
			})
	})
}

func TestMergeOmitIdentical(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(bws string) {
			test.AssertNoErr(utils.MkdirAll(bws+"/dirA/dirB/dirC", 0777))
			test.AssertNoErr(utils.MkdirAll(bws+"/dirA/dirD/dirE", 0777))
			test.AssertNoErr(testutils.PrintToFile(bws+
				"/dirA/dirB/dirC/fileA", "test data"))
			test.AssertNoErr(testutils.PrintToFile(bws+
				"/dirA/dirD/dirE/fileB", "unchanging data"))
		}, func(localWs string, remoteWs string) mergeTestCheck {
			test.AssertNoErr(testutils.OverWriteFile(localWs+
				"/dirA/dirB/dirC/fileA", "test dAtA changed AAA"))
			test.AssertNoErr(testutils.OverWriteFile(remoteWs+
				"/dirA/dirB/dirC/fileA", "test DaTa changed BBB"))

			return func(merged string) {
				test.CheckData(merged+"/dirA/dirB/dirC/fileA",
					[]byte("test DATA changed BBB"))
				test.CheckData(merged+"/dirA/dirD/dirE/fileB",
					[]byte("unchanging data"))
				fileMerges := test.CountLogStrings("mergeFile fileA")
				badMerges := test.CountLogStrings("mergeFile fileB")
				test.Assert(fileMerges == 1,
					"Incorrect number of mergeFiles called %d",
					fileMerges)
				test.Assert(badMerges == 0,
					"fileB in unchanged directory was touched")
			}
		})
	})
}

func TestMergeOverDecrement(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(bws string) {
			test.AssertNoErr(utils.MkdirAll(bws+"/dirA/dirB/dirC", 0777))
			test.AssertNoErr(utils.MkdirAll(bws+"/dirA2/dirB2/dirC2",
				0777))

			test.AssertNoErr(testutils.PrintToFile(bws+"/file", "data"))
			test.AssertNoErr(syscall.Link(bws+"/file", bws+"/link"))
			test.AssertNoErr(syscall.Link(bws+"/file",
				bws+"/dirA/dirB/dirC/linkA"))
			test.AssertNoErr(syscall.Link(bws+"/file",
				bws+"/dirA/dirB/dirC/linkB"))
			test.AssertNoErr(syscall.Link(bws+"/file",
				bws+"/dirA/dirB/dirC/linkC"))

			test.AssertNoErr(testutils.PrintToFile(bws+"/file2", "ASDF"))
			test.AssertNoErr(syscall.Link(bws+"/file2", bws+"/link2"))
			test.AssertNoErr(syscall.Link(bws+"/file2",
				bws+"/dirA2/dirB2/dirC2/linkA2"))
			test.AssertNoErr(syscall.Link(bws+"/file2",
				bws+"/dirA2/dirB2/dirC2/linkB2"))
			test.AssertNoErr(syscall.Link(bws+"/file2",
				bws+"/dirA2/dirB2/dirC2/linkC2"))
		}, func(branchA string,
			branchB string) mergeTestCheck {

			test.AssertNoErr(os.Remove(branchB +
				"/dirA/dirB/dirC/linkA"))
			test.AssertNoErr(os.Remove(branchB +
				"/dirA/dirB/dirC/linkB"))
			test.AssertNoErr(os.Remove(branchB +
				"/dirA/dirB/dirC/linkC"))
			test.AssertNoErr(syscall.Link(branchB+"/file",
				branchB+"/dirA2/dirB2/dirC2/linkA"))
			test.AssertNoErr(syscall.Link(branchB+"/file",
				branchB+"/dirA2/dirB2/dirC2/linkB"))
			test.AssertNoErr(syscall.Link(branchB+"/file",
				branchB+"/dirA2/dirB2/dirC2/linkC"))

			test.AssertNoErr(os.Remove(branchB +
				"/dirA2/dirB2/dirC2/linkA2"))
			test.AssertNoErr(os.Remove(branchB +
				"/dirA2/dirB2/dirC2/linkB2"))
			test.AssertNoErr(os.Remove(branchB +
				"/dirA2/dirB2/dirC2/linkC2"))
			test.AssertNoErr(syscall.Link(branchB+"/file2",
				branchB+"/dirA/dirB/dirC/linkA2"))
			test.AssertNoErr(syscall.Link(branchB+"/file2",
				branchB+"/dirA/dirB/dirC/linkB2"))
			test.AssertNoErr(syscall.Link(branchB+"/file2",
				branchB+"/dirA/dirB/dirC/linkC2"))

			return func(merged string) {
				wsr, cleanup := test.GetWorkspaceRoot(merged)
				defer cleanup()

				// make sure link is instantiated
				linkInodeNum := test.getInodeNum(merged + "/link")
				test.qfs.inode(&test.qfs.c, linkInodeNum)

				linkRecord := test.GetRecord(merged + "/link")
				nl := wsr.hardlinkTable.nlinks(linkRecord.FileId())
				test.Assert(nl == 5,
					"nlink count not merged correct %d", nl)

				// check the other set of hardlinks as well
				linkInodeNum = test.getInodeNum(merged + "/link2")
				test.qfs.inode(&test.qfs.c, linkInodeNum)

				linkRecord = test.GetRecord(merged + "/link2")
				nl = wsr.hardlinkTable.nlinks(linkRecord.FileId())
				test.Assert(nl == 5,
					"nlink count not merged correct %d", nl)
			}
		})
	})
}

// This test is to check the case where we deep merge two directories, but the
// merged result ends up being identical to remote
func TestMergeDeepMergeHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(bws string) {
			test.AssertNoErr(utils.MkdirAll(bws+"/dirA", 0777))
			test.AssertNoErr(testutils.PrintToFile(bws+"/dirA/file",
				"test data"))
			test.AssertNoErr(syscall.Link(bws+"/dirA/file",
				bws+"/dirA/link"))
			test.AssertNoErr(syscall.Link(bws+"/dirA/file",
				bws+"/dirA/linkB"))
			test.AssertNoErr(syscall.Link(bws+"/dirA/file",
				bws+"/dirA/linkC"))
		}, func(localWs string, remoteWs string) mergeTestCheck {
			// Make some remote only removals so we mergeDirectory
			test.AssertNoErr(os.Remove(remoteWs+"/dirA/file"))
			test.AssertNoErr(os.Remove(remoteWs+"/dirA/link"))
			test.AssertNoErr(os.Remove(remoteWs+"/dirA/linkB"))
			test.AssertNoErr(os.Remove(remoteWs+"/dirA/linkC"))

			return func(merged string) {
				// Nothing to check explicitly - we are just looking
				// to ensure no errors occur
			}
		})
	})
}

func TestMergeDeepMergeHardlinkPartial(t *testing.T) {
	runTest(t, func(test *testHelper) {
		MergeTester(test, func(bws string) {
			test.AssertNoErr(utils.MkdirAll(bws+"/dirA", 0777))
			test.AssertNoErr(testutils.PrintToFile(bws+"/dirA/file",
				"test data"))
			test.AssertNoErr(syscall.Link(bws+"/dirA/file",
				bws+"/dirA/link"))
			test.AssertNoErr(syscall.Link(bws+"/dirA/file",
				bws+"/dirA/linkB"))
			test.AssertNoErr(syscall.Link(bws+"/dirA/file",
				bws+"/dirA/linkC"))
		}, func(localWs string, remoteWs string) mergeTestCheck {
			// Make some remote only removals so we mergeDirectory
			test.AssertNoErr(os.Remove(remoteWs+"/dirA/linkB"))
			test.AssertNoErr(os.Remove(remoteWs+"/dirA/linkC"))

			return func(merged string) {
				test.CheckLink(merged+"/dirA/link",
					[]byte("test data"), 2)
			}
		})
	})
}

