// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Test the various operations on directories, such as creation and traversing

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

func TestDirectoryCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testFilename := workspace + "/" + "test"
		err := syscall.Mkdir(testFilename, 0124)
		test.Assert(err == nil, "Error creating directories: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test dir: %v", err)
		test.Assert(stat.Size > 0, "Size too small: %d", stat.Size)
		test.Assert(stat.Size <= int64(qfsBlockSize), "Size too big: %d",
			stat.Size)
		test.Assert(stat.Nlink == 2, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFDIR
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IWGRP |
			syscall.S_IROTH
		test.Assert(stat.Mode == expectedPermissions,
			"Directory permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirName := workspace + "/test/a/b"
		err := utils.MkdirAll(dirName, 0124)
		test.Assert(err == nil, "Error creating directories: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(dirName, &stat)
		test.Assert(err == nil, "Error stat'ing test dir: %v", err)
		test.Assert(stat.Size > 0, "Size too small: %d", stat.Size)
		test.Assert(stat.Size <= int64(qfsBlockSize), "Size too big: %d",
			stat.Size)
		test.Assert(stat.Nlink == 2, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFDIR
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IWGRP |
			syscall.S_IROTH
		test.Assert(stat.Mode == expectedPermissions,
			"Directory permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryFileCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirName := workspace + "/test/a/b"
		testFilename := dirName + "/c"

		err := utils.MkdirAll(dirName, 0124)
		test.Assert(err == nil, "Error creating directories: %v", err)

		fd, err := syscall.Creat(testFilename, 0124)
		test.Assert(err == nil, "Error creating file: %v", err)

		err = syscall.Close(fd)
		test.Assert(err == nil, "Error closing fd: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ing test file: %v", err)
		test.Assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
		test.Assert(stat.Nlink == 1, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFREG
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IWGRP |
			syscall.S_IROTH
		test.Assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryFileDescriptorDirtying(t *testing.T) {
	runTest(t, func(test *testHelper) {
		// Create a file and determine its inode numbers
		workspace := test.NewWorkspace()
		wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		dirName := workspace + "/test/a/b"
		testFilename := dirName + "/" + "test"

		err := utils.MkdirAll(dirName, 0124)
		test.Assert(err == nil, "Error creating directories: %v", err)

		fd, err := syscall.Creat(testFilename, 0124)
		test.Assert(err == nil, "Error creating file: %v", err)
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Error stat'ind test file: %v", err)
		test.Assert(stat.Ino >= quantumfs.InodeIdReservedEnd,
			"File had reserved inode number %d", stat.Ino)

		// Find the matching FileHandle
		descriptors := test.fileDescriptorFromInodeNum(stat.Ino)
		test.Assert(len(descriptors) == 1,
			"Incorrect number of fds found 1 != %d", len(descriptors))
		fileDescriptor := descriptors[0]
		file := fileDescriptor.file

		// Save the workspace rootId, change the File key, simulating
		// changing the data, then mark the matching FileDescriptor dirty.
		// This should trigger a refresh up the hierarchy and, after we
		// trigger a delayed sync, change the workspace rootId and mark the
		// fileDescriptor clean.
		oldRootId, _ := test.workspaceRootId(wsTypespaceName,
			wsNamespaceName, wsWorkspaceName)

		c := test.newCtx()
		_, err = file.accessor.writeBlock(c, 0, 0, []byte("update"))
		test.Assert(err == nil, "Failure modifying small file")
		fileDescriptor.dirty(c)

		test.SyncAllWorkspaces()
		newRootId, _ := test.workspaceRootId(wsTypespaceName,
			wsNamespaceName, wsWorkspaceName)

		test.Assert(!oldRootId.IsEqualTo(newRootId),
			"Workspace rootId didn't change")

		syscall.Close(fd)
	})
}

// If we modify a Directory we do not ever reload the data from the datastore. In
// order to confirm the data has been written correctly we need to branch the
// workspace after modifying the directory and confirm the newly loaded directory
// contains the changes in the update.
func TestDirectoryUpdate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		api := test.getApi()

		src := test.NewWorkspace()
		src = test.RelPath(src)

		dst := "branch/dirupdate/test"

		// First create a file
		testFilename := test.AbsPath(src + "/" + "test")
		fd, err := os.Create(testFilename)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		// Then branch the workspace
		err = api.Branch(src, dst)
		test.Assert(err == nil, "Failed to branch workspace: %v", err)

		// Ensure the new workspace has the correct file
		testFilename = test.AbsPath(dst + "/" + "test")
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Workspace copy doesn't match: %v", err)
	})
}

func TestDirectoryFileDeletion(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.Assert(err == nil, "Error creating file: %v", err)
		err = fd.Close()
		test.Assert(err == nil, "Error closing fd: %v", err)

		err = syscall.Unlink(testFilename)
		test.Assert(err == nil, "Error unlinking file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err != nil, "Error test file not deleted: %v", err)

		// Confirm after branch
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		testFilename = workspace + "/" + "test"
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err != nil, "Error test file not deleted: %v", err)
	})
}

func testPermissions(test *testHelper, onDirectory bool, asRoot bool,
	directoryUserMatches bool, directoryGroupMatches bool, directorySticky bool,
	permissions uint32, mustSucceed bool) {

	if directorySticky {
		permissions |= syscall.S_ISVTX
	}

	workspace := test.NewWorkspace()
	testDir := workspace
	if onDirectory {
		testDir = workspace + "/testDir"
		err := syscall.Mkdir(testDir, permissions)
		test.Assert(err == nil, "Failed creating testDir: %v", err)

		// ensure the sticky bit is set
		if directorySticky {
			checkInfo, err := os.Stat(testDir)
			test.AssertNoErr(err)
			test.Assert(utils.BitAnyFlagSet(uint(checkInfo.Mode()),
				uint(os.ModeSticky)),
				"Sticky bit missing after mkdir")
		}
	}
	testFile := testDir + "/file"

	err := syscall.Mknod(testFile, syscall.S_IFREG, 0)
	test.Assert(err == nil, "Unable to create test file: %v", err)

	var uid int
	var gid int

	if !directoryUserMatches {
		uid = 100
	} else if asRoot {
		uid = 0
	} else { // !asRoot
		uid = 99
	}

	if !directoryGroupMatches {
		gid = 100
	} else if asRoot {
		gid = 0
	} else { // !asRoot
		gid = 99
	}

	// Make a temporary directory so we can test MvChild
	tmpDir := testDir + "/tmpdir"
	err = os.Mkdir(tmpDir, 0777)
	test.AssertNoErr(err)
	err = os.Chown(tmpDir, uid, gid)
	test.AssertNoErr(err)

	if onDirectory {
		err = os.Chown(testDir, uid, gid)
		test.Assert(err == nil, "Error chowning test directory: %v",
			err)
	} else {
		// We cannot chown on a WorkspaceRoot, just leave the owner
		// as root.
		test.Assert(!asRoot,
			"Cannot chown workspaceroot, invalid arguments")
	}

	if !asRoot {
		defer test.SetUidGid(99, 99, nil).Revert()
	}

	// first check if we can rename
	newName := testFile + "b"
	err = os.Rename(testFile, newName)
	if mustSucceed {
		test.Assert(err == nil, "Failed to rename file: %v", err)
		testFile = newName
	} else {
		test.Assert(os.IsPermission(err), "Wrong error when renaming: %v",
			err)
	}

	// then check mvchild
	tmpName := tmpDir + "/tmpmoved"
	err = os.Rename(testFile, tmpName)
	if mustSucceed {
		test.Assert(err == nil, "Failed to move file: %v", err)
		err = os.Rename(tmpName, testFile)
		test.AssertNoErr(err)
	} else {
		test.Assert(os.IsPermission(err), "Wrong error when moving: %v",
			err)
	}

	err = syscall.Unlink(testFile)
	if mustSucceed {
		test.Assert(err == nil, "Failed to unlink file: %v", err)
	} else {
		test.Assert(os.IsPermission(err), "Wrong error when unlinking: %v",
			err)
	}
}

func TestPermissionsAsRootNoPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, true, false, false, false, 0000,
			true)
	})
}

func TestPermissionsAsRootNoPermsSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, true, false, false, true, 0000,
			true)
	})
}

func TestPermissionsAsRootNoPermsOwner(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, true, true, true, false, 0000,
			true)
	})
}

func TestPermissionsAsRootNoPermsOwnerSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, true, true, true, true, 0000,
			true)
	})
}

func TestPermissionsAsUserNoWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, false, false, 0555,
			false)
	})
}

func TestPermissionsAsUserNoWriteSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, false, true, 0555,
			false)
	})
}

func TestPermissionsAsUserNoWriteOwner(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, true, true, false, 0555,
			false)
	})
}

func TestPermissionsAsUserNoWriteOwnerSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, true, true, true, 0555,
			false)
	})
}

func TestPermissionsAsUserUserWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, false, false, 0755,
			false)
	})
}

func TestPermissionsAsUserUserWriteSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, false, true, 0755,
			false)
	})
}

func TestPermissionsAsUserUserWriteOwner(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, true, true, false, 0755,
			true)
	})
}

func TestPermissionsAsUserUserWriteOwnerSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, true, true, true, 0755,
			true)
	})
}

func TestPermissionsAsUserGroupWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, false, false, 0575,
			false)
	})
}

func TestPermissionsAsUserGroupWriteSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, false, true, 0575,
			false)
	})
}

func TestPermissionsAsUserGroupWriteOwner(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, true, true, false, 0575,
			false)
	})
}

func TestPermissionsAsUserGroupWriteOwnerSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, true, true, true, 0575,
			false)
	})
}

func TestPermissionsAsUserGroupWriteGroupMatch(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, true, false, 0575,
			true)
	})
}

func TestPermissionsAsUserGroupWriteGroupMatchSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, true, true, 0575,
			false)
	})
}

func TestPermissionsAsUserOtherWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, false, false, 0557,
			true)
	})
}

func TestPermissionsAsUserOtherWriteSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, false, false, true, 0557,
			false)
	})
}

func TestPermissionsAsUserOtherWriteOwner(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, true, true, false, 0557,
			false)
	})
}

func TestPermissionsAsUserOtherWriteOwnerSticky(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, true, false, true, true, true, 0557,
			false)
	})
}

func TestPermissionsAsUserInWorkspaceRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testPermissions(test, false, false, false, false, false, 0000,
			true)
	})
}

func TestPermissionsAsUserMissingFileInWorkspaceRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		defer test.SetUidGid(99, -1, nil).Revert()

		err := syscall.Unlink(workspace + "/file")
		test.Assert(err == syscall.ENOENT, "Wrong error when unlinking: %v",
			err)
	})
}

func TestDirectoryUnlinkDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/" + "test"
		err := syscall.Mkdir(testDir, 0124)
		test.Assert(err == nil, "Error creating directory: %v", err)

		err = syscall.Unlink(testDir)
		test.Assert(err != nil, "Expected error unlinking directory")
		test.Assert(err == syscall.EISDIR, "Error not EISDIR: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testDir, &stat)
		test.Assert(err == nil, "Error test directory was deleted")
	})
}

func TestDirectoryRmdirEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0124)
		test.Assert(err == nil, "Error creating directory: %v", err)

		err = syscall.Rmdir(testDir)
		test.Assert(err == nil, "Error deleting directory: %v", err)
	})
}

func TestDirectoryRmdirNewlyEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0324)
		test.Assert(err == nil, "Error creating directory: %v", err)

		testFile := testDir + "/file"
		fd, err := os.Create(testFile)
		test.Assert(err == nil, "Error creating file: %v", err)
		fd.Close()

		err = syscall.Unlink(testFile)
		test.Assert(err == nil, "Error unlinking file: %v", err)

		err = syscall.Rmdir(testDir)
		test.Assert(err == nil, "Error deleting directory: %v", err)
	})
}

func TestDirectoryRmdirNotEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0124)
		test.Assert(err == nil, "Error creating directory: %v", err)
		testFile := testDir + "/file"
		fd, err := os.Create(testFile)
		test.Assert(err == nil, "Error creating file: %v", err)
		fd.Close()

		err = syscall.Rmdir(testDir)
		test.Assert(err != nil, "Expected error when deleting directory")
		test.Assert(err == syscall.ENOTEMPTY,
			"Expected error ENOTEMPTY: %v", err)
	})
}

func TestDirectoryRmdirFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFile := workspace + "/test"
		fd, err := os.Create(testFile)
		test.Assert(err == nil, "Error creating file: %v", err)
		fd.Close()

		err = syscall.Rmdir(testFile)
		test.Assert(err != nil, "Expected error when deleting directory")
		test.Assert(err == syscall.ENOTDIR,
			"Expected error ENOTDIR: %v", err)
	})
}

// Confirm that when we reload a directory from the datastore that the classes it
// instantiates matches the type of the entry in the directory.
func TestDirectoryChildTypes(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testDir := workspace + "/testdir"
		testFile := testDir + "/testfile"

		err := syscall.Mkdir(testDir, 0124)
		test.Assert(err == nil, "Error creating directory: %v", err)

		fd, err := os.Create(testFile)
		test.Assert(err == nil, "Error creating file: %v", err)
		test.Log("Created file %s", testFile)

		fileContents := "testdata"
		_, err = fd.Write([]byte(fileContents))
		test.Assert(err == nil, "Error writing to file: %v", err)
		fd.Close()

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		testFile = workspace + "/testdir/testfile"

		data, err := ioutil.ReadFile(testFile)
		test.Assert(err == nil, "Error reading file in new workspace: %v",
			err)
		test.Assert(string(data) == fileContents,
			"File contents differ in branched workspace: %v",
			string(data))
	})
}

func TestLargeDirectory(t *testing.T) {
	runExpensiveTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testdir := workspace + "/testlargedir"
		err := syscall.Mkdir(testdir, 0777)
		test.Assert(err == nil, "Error creating directory:%v", err)
		numToCreate := quantumfs.MaxDirectoryRecords() +
			quantumfs.MaxDirectoryRecords()/4

		// Create enough children to overflow a single block
		for i := 0; i < numToCreate; i++ {
			testFile := fmt.Sprintf("%s/testfile%d", testdir, i)
			fd, err := os.Create(testFile)
			test.Assert(err == nil, "Error creating file: %v", err)
			fd.Close()
		}

		// Confirm all the children are accounted for in the original
		// workspace
		files, err := ioutil.ReadDir(testdir)
		test.Assert(err == nil, "Error reading directory %v", err)
		attendance := make(map[string]bool, numToCreate)

		for _, file := range files {
			attendance[file.Name()] = true
		}
		test.Assert(len(attendance) == numToCreate,
			"Incorrect number of files in directory %d", len(attendance))

		for i := 0; i < numToCreate; i++ {
			testFile := fmt.Sprintf("testfile%d", i)
			test.Assert(attendance[testFile], "File %s missing",
				testFile)
		}

		// Confirm all the children are accounted for in a branched
		// workspace
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		testdir = workspace + "/testlargedir"
		files, err = ioutil.ReadDir(testdir)
		test.Assert(err == nil, "Error reading directory %v", err)
		attendance = make(map[string]bool, numToCreate)

		for _, file := range files {
			attendance[file.Name()] = true
		}
		test.Assert(len(attendance) == numToCreate,
			"Incorrect number of files in directory %d", len(attendance))

		for i := 0; i < numToCreate; i++ {
			testFile := fmt.Sprintf("testfile%d", i)
			test.Assert(attendance[testFile], "File %s missing",
				testFile)
		}
	})
}

func TestDirectoryChmod(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/testdir"

		err := syscall.Mkdir(testDir, 0)
		test.Assert(err == nil, "Failed to create directory: %v", err)

		info, err := os.Stat(testDir)
		test.Assert(err == nil, "Failed getting dir info: %v", err)
		test.Assert(info.Mode()&os.ModePerm == 0,
			"Initial permissions incorrect %d", info.Mode()&os.ModePerm)

		err = os.Chmod(testDir, 0777)
		test.Assert(err == nil, "Error setting permissions: %v", err)

		info, err = os.Stat(testDir)
		test.Assert(err == nil, "Failed getting dir info: %v", err)
		test.Assert(info.Mode()&os.ModePerm == 0777,
			"Changed permissions incorrect %d", info.Mode()&os.ModePerm)
	})
}

func TestIntraDirectoryRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFilename1 := workspace + "/test"
		testFilename2 := workspace + "/test2"

		fd, err := os.Create(testFilename1)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(testFilename1, testFilename2)
		test.Assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename2, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		testFilename2 = workspace + "/test2"
		err = syscall.Stat(testFilename2, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)
	})
}

func TestInterDirectoryRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		interDirectoryRename(test)
	})
}

func interDirectoryRename(test *testHelper) {
	workspace := test.NewWorkspace()
	testDir1 := workspace + "/dir1"
	testDir2 := workspace + "/dir2"
	testFilename1 := testDir1 + "/test"
	testFilename2 := testDir2 + "/test2"

	err := syscall.Mkdir(testDir1, 0777)
	test.Assert(err == nil, "Failed to create directory: %v", err)
	err = syscall.Mkdir(testDir2, 0777)
	test.Assert(err == nil, "Failed to create directory: %v", err)

	fd, err := os.Create(testFilename1)
	fd.Close()
	test.Assert(err == nil, "Error creating test file: %v", err)

	err = os.Rename(testFilename1, testFilename2)
	test.Assert(err == nil, "Error renaming file: %v", err)

	var stat syscall.Stat_t
	err = syscall.Stat(testFilename2, &stat)
	test.Assert(err == nil, "Rename failed: %v", err)

	// Confirm after branch
	workspace = test.AbsPath(test.branchWorkspace(workspace))
	testFilename2 = workspace + "/dir2/test2"
	err = syscall.Stat(testFilename2, &stat)
	test.Assert(err == nil, "Rename failed: %v", err)
}

func TestRenameIntoParent(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		parent := workspace + "/parent"
		child := workspace + "/parent/child"
		childFile := child + "/test"
		parentFile := parent + "/test2"

		err := syscall.Mkdir(parent, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)
		err = syscall.Mkdir(child, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(childFile)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(childFile, parentFile)
		test.Assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(parentFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		parentFile = workspace + "/parent/test2"
		err = syscall.Stat(parentFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)
	})
}

func TestRenameIntoChild(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		parent := workspace + "/parent"
		child := workspace + "/parent/child"
		parentFile := parent + "/test"
		childFile := child + "/test2"

		err := syscall.Mkdir(parent, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)
		err = syscall.Mkdir(child, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(parentFile)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(parentFile, childFile)
		test.Assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(childFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		childFile = workspace + "/parent/child/test2"
		err = syscall.Stat(childFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)
	})
}

func TestRenameIntoIndirectParent(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		parent := workspace + "/parent"
		child := workspace + "/parent/indirect/child"
		childFile := child + "/test"
		parentFile := parent + "/test2"

		err := syscall.Mkdir(parent, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)
		err = utils.MkdirAll(child, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(childFile)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(childFile, parentFile)
		test.Assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(parentFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		parentFile = workspace + "/parent/test2"
		err = syscall.Stat(parentFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)
	})
}

func TestRenameIntoIndirectChild(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		parent := workspace + "/parent"
		child := workspace + "/parent/indirect/child"
		parentFile := parent + "/test"
		childFile := child + "/test2"

		err := syscall.Mkdir(parent, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)
		err = utils.MkdirAll(child, 0777)
		test.Assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(parentFile)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(parentFile, childFile)
		test.Assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(childFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		childFile = workspace + "/parent/indirect/child/test2"
		err = syscall.Stat(childFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)
	})
}

func TestDirectoryRenameDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := workspace + "/dir1"
		file1 := dir1 + "/file1"
		dir2 := workspace + "/dir2"

		test.AssertNoErr(os.Mkdir(dir1, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(os.Mkdir(dir2, 0777))

		test.AssertNoErr(syscall.Rename(dir1, dir2))

		_, err := os.Stat(dir1)
		test.AssertErr(err)
		_, err = os.Stat(dir2)
		test.AssertNoErr(err)
		_, err = os.Stat(dir2 + "/file1")
		test.AssertNoErr(err)
	})
}

func TestDirectoryRenameDirNotEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := workspace + "/dir1"
		file1 := dir1 + "/file1"
		dir2 := workspace + "/dir2"
		file2 := dir2 + "/file2"

		test.AssertNoErr(os.Mkdir(dir1, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(os.Mkdir(dir2, 0777))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		err := syscall.Rename(dir1, dir2)
		test.Assert(err != nil && err.(syscall.Errno) == syscall.ENOTEMPTY,
			"Non-empty directory shouldn't be destination in rename")
	})
}

func TestDirectoryRenameDirOntoFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := workspace + "/dir1"
		file1 := dir1 + "/file1"
		file2 := workspace + "/file2"

		test.AssertNoErr(os.Mkdir(dir1, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		err := syscall.Rename(dir1, file2)
		test.Assert(err != nil && err.(syscall.Errno) == syscall.ENOTDIR,
			"Directory cannot be renamed onto file")
	})
}

func TestDirectoryRenameFileOntoDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		file1 := workspace + "/file1"
		dir2 := workspace + "/dir2"
		file2 := dir2 + "/file2"

		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(os.Mkdir(dir2, 0777))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		err := syscall.Rename(file1, dir2)
		test.Assert(err != nil && err.(syscall.Errno) == syscall.EISDIR,
			"File cannot be renamed onto directory")
	})
}

func TestDirectoryMvChildDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		otherdir := workspace + "/otherdir"
		test.AssertNoErr(os.Mkdir(otherdir, 0777))
		dir1 := workspace + "/dir1"
		file1 := dir1 + "/file1"
		dir2 := otherdir + "/dir2"

		test.AssertNoErr(os.Mkdir(dir1, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(os.Mkdir(dir2, 0777))

		test.AssertNoErr(syscall.Rename(dir1, dir2))

		_, err := os.Stat(dir1)
		test.AssertErr(err)
		_, err = os.Stat(dir2)
		test.AssertNoErr(err)
		_, err = os.Stat(dir2 + "/file1")
		test.AssertNoErr(err)
	})
}

func TestDirectoryMvChildDirNotEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		otherdir := workspace + "/otherdir"
		test.AssertNoErr(os.Mkdir(otherdir, 0777))
		dir1 := workspace + "/dir1"
		file1 := dir1 + "/file1"
		dir2 := otherdir + "/dir2"
		file2 := dir2 + "/file2"

		test.AssertNoErr(os.Mkdir(dir1, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(os.Mkdir(dir2, 0777))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		err := syscall.Rename(dir1, dir2)
		test.Assert(err != nil && err.(syscall.Errno) == syscall.ENOTEMPTY,
			"Non-empty directory shouldn't be destination in rename")
	})
}

func TestDirectoryMvChildDirOntoFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		otherdir := workspace + "/otherdir"
		test.AssertNoErr(os.Mkdir(otherdir, 0777))
		dir1 := workspace + "/dir1"
		file1 := dir1 + "/file1"
		file2 := otherdir + "/file2"

		test.AssertNoErr(os.Mkdir(dir1, 0777))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		err := syscall.Rename(dir1, file2)
		test.Assert(err != nil && err.(syscall.Errno) == syscall.ENOTDIR,
			"Directory cannot be renamed onto file")
	})
}

func TestDirectoryMvChildFileOntoDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		otherdir := workspace + "/otherdir"
		test.AssertNoErr(os.Mkdir(otherdir, 0777))
		file1 := workspace + "/file1"
		dir2 := otherdir + "/dir2"
		file2 := dir2 + "/file2"

		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(os.Mkdir(dir2, 0777))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		err := syscall.Rename(file1, dir2)
		test.Assert(err != nil && err.(syscall.Errno) == syscall.EISDIR,
			"File cannot be renamed onto directory")
	})
}

func TestDirectoryMvChildOntoOpenFileSameDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir := workspace + "/dir"
		file1 := dir + "/file1"
		file2 := dir + "/file2"

		test.AssertNoErr(utils.MkdirAll(dir, 0124))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		f, err := os.Open(file2)
		test.AssertNoErr(err)
		defer f.Close()
		syncErr := test.SyncWorkspaceAsync(test.RelPath(workspace))
		test.AssertNoErr(syscall.Rename(file1, file2))
		test.AssertNoErr(<-syncErr)
	})
}

func TestDirectoryMvChildOntoOpenFileInParentDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir := workspace + "/dir"
		file1 := dir + "/file1"
		file2 := workspace + "/file2"

		test.AssertNoErr(utils.MkdirAll(dir, 0124))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		f, err := os.Open(file2)
		test.AssertNoErr(err)
		defer f.Close()
		syncErr := test.SyncWorkspaceAsync(test.RelPath(workspace))
		test.AssertNoErr(syscall.Rename(file1, file2))
		test.AssertNoErr(<-syncErr)
	})
}

func TestDirectoryMvChildOntoOpenFileInSibling(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := workspace + "/dir1"
		file1 := dir1 + "/file1"
		dir2 := workspace + "/dir2"
		file2 := workspace + "/file2"

		test.AssertNoErr(utils.MkdirAll(dir1, 0124))
		test.AssertNoErr(utils.MkdirAll(dir2, 0124))
		test.AssertNoErr(testutils.PrintToFile(file1, ""))
		test.AssertNoErr(testutils.PrintToFile(file2, ""))

		f, err := os.Open(file2)
		test.AssertNoErr(err)
		defer f.Close()
		syncErr := test.SyncWorkspaceAsync(test.RelPath(workspace))
		test.AssertNoErr(syscall.Rename(file1, file2))
		test.AssertNoErr(<-syncErr)
	})
}

func TestDirectoryRenameNonNormalizedHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := workspace + "/dir1"
		file1 := dir1 + "/file1"

		dir2 := workspace + "/dir2"
		file2 := dir2 + "/file2"

		utils.MkdirAll(dir1, 0777)
		utils.MkdirAll(dir2, 0777)

		fd, err := syscall.Creat(file1, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(syscall.Link(file1, file2))
		test.AssertNoErr(syscall.Unlink(file1))
		test.AssertNoErr(syscall.Rename(file2, file1))
		test.AssertNoErr(syscall.Unlink(file1))
	})
}

func TestDirectoryRenameHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := workspace + "/dir1"
		file0 := dir1 + "/file0"
		file1 := dir1 + "/file1"

		dir2 := workspace + "/dir2"
		file2 := dir2 + "/file2"

		utils.MkdirAll(dir1, 0777)
		utils.MkdirAll(dir2, 0777)

		fd, err := syscall.Creat(file0, syscall.O_CREAT)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Close(fd))
		test.AssertNoErr(syscall.Link(file0, file1))
		test.AssertNoErr(syscall.Rename(file0, file2))
		test.AssertNoErr(syscall.Unlink(file2))
		test.AssertNoErr(syscall.Unlink(file1))
	})
}

func TestSUIDPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testFilename := workspace + "/test"

		fd, err := os.Create(testFilename)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		mode := 0777 | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
		err = os.Chmod(testFilename, mode)
		test.Assert(err == nil, "Error setting permissions: %v", err)

		info, err := os.Stat(testFilename)
		test.Assert(err == nil, "Failed getting dir info: %v", err)
		test.Assert(info.Mode() == mode,
			"Changed permissions incorrect %d", info.Mode())

		// Confirm after branch
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		testFilename = workspace + "/test"
		info, err = os.Stat(testFilename)
		test.Assert(err == nil, "Failed getting dir info: %v", err)
		test.Assert(info.Mode() == mode,
			"Changed permissions incorrect %d", info.Mode())
	})
}

func TestLoadOnDemand(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirName := workspace + "/layerA/layerB/layerC"
		fileA := "/layerA/fileA"
		fileB := "/layerA/layerB/fileB"
		fileC := "/layerA/layerB/layerC/fileC"

		data := GenData(2400)
		dataA := data[0:800]
		dataB := data[800:1600]
		dataC := data[1600:2400]

		err := utils.MkdirAll(dirName, 0124)
		test.Assert(err == nil, "Error creating directories: %v", err)

		err = testutils.PrintToFile(workspace+fileA, string(dataA))
		test.Assert(err == nil, "Error creating file: %v", err)
		err = testutils.PrintToFile(workspace+fileB, string(dataB))
		test.Assert(err == nil, "Error creating file: %v", err)
		err = testutils.PrintToFile(workspace+fileC, string(dataC))
		test.Assert(err == nil, "Error creating file: %v", err)

		newspace := test.AbsPath(test.branchWorkspace(workspace))
		fileA = newspace + "/layerA/fileA"
		dirB := newspace + "/layerA/layerB"
		fileB = dirB + "/fileB"
		dirC := dirB + "/layerC"
		fileC = dirC + "/fileC"

		// Confirm the inode for fileA isn't instantiated. Getting the stat
		// attributes of an inode shouldn't instantiate it.
		fileNode := test.getInode(fileA)
		test.Assert(fileNode == nil, "File inode unexpectedly instantiated")

		// Stat'ing a file should instantiate its parents
		var stat syscall.Stat_t
		err = syscall.Stat(fileB, &stat)
		test.Assert(err == nil, "Error stat'ing FileB: %v", err)

		dirNode := test.getInode(dirB)
		test.Assert(dirNode != nil,
			"Intermediate directory unexpectedly nil")

		// But siblings shouldn't have been instantiated
		dirNode = test.getInode(dirC)
		test.Assert(dirNode == nil, "dirC unexpectedly instantiated")

		// Reading FileB shouldn't make any difference
		layerBData, err := ioutil.ReadFile(fileB)
		test.Assert(err == nil, "Unable to read fileB file contents %v", err)
		test.Assert(bytes.Equal(layerBData, dataB),
			"dynamically loaded inode data mismatch")
		dirNode = test.getInode(dirC)
		test.Assert(dirNode == nil, "dirC unexpectedly instantiated")

		// Ensure we can read without stat'ing first
		layerCData, err := ioutil.ReadFile(fileC)
		test.Assert(err == nil, "Unable to read fileC contents %v", err)
		test.Assert(bytes.Equal(layerCData, dataC),
			"dynamically loaded inode data mismatch")
	})
}

// Change the ownership of a file to be owned by the user and then confirm that the
// dummy user is used when viewing the permissions as root.
func TestChownUserAsRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testFilename := workspace + "/test"
		fd, err := syscall.Creat(testFilename, 0777)
		syscall.Close(fd)
		test.Assert(err == nil, "Error creating file: %v", err)

		err = os.Chown(testFilename, 22222, 22222)
		test.Assert(err == nil, "Failed to chown: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.Assert(err == nil, "Failed to stat file: %v", err)
		test.Assert(stat.Uid == 10000, "UID doesn't match: %d", stat.Uid)
		test.Assert(stat.Gid == 10000, "GID doesn't match: %d", stat.Gid)
	})
}

// Ensure that when we rewind the directory entry we get new files added after the
// directory was opened.
func TestDirectorySnapshotRefresh(t *testing.T) {
	runTest(t, func(test *testHelper) {
		parent := test.NewWorkspace()

		for i := 0; i < 2; i++ {
			dir, err := os.Open(parent)
			test.Assert(err == nil, "Failed to open workspace")
			defer dir.Close()

			// Create some filler to ensure we actually seek
			for n := 0; n < 10; n++ {
				file, _ := os.Create(fmt.Sprintf("%s/filler%d",
					parent, n))
				file.Close()
			}

			// Read some entries from the parent, then create a new child
			// and seek to ensure the still open directory entry sees the
			// new entry.
			_, err = dir.Readdirnames(2)
			test.Assert(err == nil, "Error reading two entries: %v", err)

			childName := parent + "/test"
			err = syscall.Mkdir(childName, 0777)
			test.Assert(err == nil, "Error creating child directory: %v",
				err)

			_, err = dir.Seek(0, os.SEEK_SET)
			test.Assert(err == nil, "Error seeking directory to start: "+
				"%v", err)

			children, err := dir.Readdirnames(-1)
			test.Assert(err == nil, "Error reading all entries of "+
				"directory: %v", err)

			dirExists := false
			for _, name := range children {
				if name == "test" {
					dirExists = true
				}
			}
			test.Assert(dirExists,
				"Failed to find new directory after rewind")

			// Run again with the directory instead of the workspace
			parent = childName
		}
	})
}

// Trigger GetAttr on a directory in order to confirm that it works correctly
func TestDirectoryGetAttr(t *testing.T) {
	runTestCustomConfig(t, cacheTimeout100Ms, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirName := workspace + "/dir"

		err := syscall.Mkdir(dirName, 0124)
		test.Assert(err == nil, "Error creating directory: %v", err)

		wsr, err := os.Open(workspace)
		test.Assert(err == nil, "Error opening workspace: %v", err)
		defer wsr.Close()

		dir, err := os.Open(dirName)
		test.Assert(err == nil, "Error opening dir: %v", err)
		defer dir.Close()

		time.Sleep(500 * time.Millisecond)

		fileInfo, err := wsr.Stat()
		test.Assert(err == nil, "Error stat'ing workspace: %v", err)
		test.Assert(fileInfo.Mode()&os.ModePerm == 0777,
			"Workspace permissions incorrect: %d", fileInfo.Mode())

		fileInfo, err = dir.Stat()
		test.Assert(err == nil, "Error stat'ing directory: %v", err)
		test.Assert(fileInfo.Mode()&os.ModePerm == 0124,
			"Directory permissions incorrect: %d", fileInfo.Mode())
	})
}

func testInodeCreatePermissions(test *testHelper, testDir string, mustSucceed bool,
	failMsg string) {

	check := func(err error) {
		if mustSucceed {
			test.Assert(err == nil, "%s: %v", failMsg, err)
		} else {
			test.Assert(err != nil, "%s", failMsg)
		}
	}

	// Test read permission
	_, err := ioutil.ReadDir(testDir)
	if mustSucceed {
		test.Assert(err == nil, "%s: Open of parent failed, %v", failMsg,
			err)
	} else {
		test.Assert(err != nil, "%s: Open of parent passed", failMsg)
	}

	// Test Mkdir (write permission)
	err = syscall.Mkdir(testDir+"/testMkdir", 777)
	check(err)

	// Test Mknod
	err = syscall.Mknod(testDir+"/testMknod",
		syscall.S_IFSOCK|syscall.S_IRWXU, 0x12345678)
	check(err)

	// Test Create
	fd, err := syscall.Creat(testDir+"/testCreate", 0777)
	syscall.Close(fd)
	check(err)

	// Test Symlink
	err = os.Symlink("arst", testDir+"/testSymlink")
	check(err)
}

func TestInodeCreatePermissionsAsRootNoPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0000)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		testInodeCreatePermissions(test, testDir, true,
			"Failed creating directory as root")
	})
}

func TestInodeCreatePermissionsAsRootUserPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0700)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		testInodeCreatePermissions(test, testDir, true,
			"Failed creating directory as root with ownership")
	})
}

func TestInodeCreatePermissionsAsRootGroupPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0070)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		testInodeCreatePermissions(test, testDir, true,
			"Failed creating directory as root with ownership")
	})
}

func TestInodeCreatePermissionsAsRootOtherPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0007)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		testInodeCreatePermissions(test, testDir, true,
			"Failed creating directory as root with ownership")
	})
}

func TestInodeCreatePermissionsAsNonOwnerNoPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0000)
		test.Assert(err == nil, "Failed creating directory: %v", err)
		defer test.SetUidGid(99, 99, nil).Revert()

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestInodeCreatePermissionsAsNonOwnerUserPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0700)
		test.Assert(err == nil, "Failed creating directory: %v", err)
		defer test.SetUidGid(99, 99, nil).Revert()

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestInodeCreatePermissionsAsNonOwnerGroupPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0070)
		test.Assert(err == nil, "Failed creating directory: %v", err)
		defer test.SetUidGid(99, 99, nil).Revert()

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestInodeCreatePermissionsAsNonOwnerOtherPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0007)
		test.Assert(err == nil, "Failed creating directory: %v", err)
		defer test.SetUidGid(99, 99, nil).Revert()

		testInodeCreatePermissions(test, testDir, true,
			"Failed creating directory as root with ownership")
	})
}

func TestInodeCreatePermissionsAsGroupMemberNoPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0000)
		test.Assert(err == nil, "Failed creating directory: %v", err)
		defer test.SetUidGid(99, -1, nil).Revert()

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestInodeCreatePermissionsAsGroupMemberUserPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0700)
		test.Assert(err == nil, "Failed creating directory: %v", err)
		defer test.SetUidGid(99, -1, nil).Revert()

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})

}

func TestInodeCreatePermissionsAsGroupMemberGroupPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0070)
		test.Assert(err == nil, "Failed creating directory: %v", err)
		defer test.SetUidGid(99, -1, nil).Revert()

		testInodeCreatePermissions(test, testDir, true,
			"Fail creating directory as group member")
	})
}

func TestInodeCreatePermissionsAsGroupMemberOtherPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		err := syscall.Mkdir(testDir, 0007)
		test.Assert(err == nil, "Failed creating directory: %v", err)
		defer test.SetUidGid(99, -1, nil).Revert()

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestInodeCreatePermissionsAsUserNoPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"

		defer test.SetUidGid(99, -1, nil).Revert()

		err := syscall.Mkdir(testDir, 0000)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestInodeCreatePermissionsAsUserUserPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"

		defer test.SetUidGid(99, -1, nil).Revert()

		err := syscall.Mkdir(testDir, 0700)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		testInodeCreatePermissions(test, testDir, true,
			"Failed creating directory I own")
	})
}

func TestInodeCreatePermissionsAsUserGroupPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"

		defer test.SetUidGid(99, -1, nil).Revert()

		err := syscall.Mkdir(testDir, 0070)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestInodeCreatePermissionsAsUserOtherPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"

		defer test.SetUidGid(99, -1, nil).Revert()

		err := syscall.Mkdir(testDir, 0007)
		test.Assert(err == nil, "Failed creating directory: %v", err)

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestDirectoryNlinkValues(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		dirs := [...]string{
			"/a1/b1/c1",
			"/a1/b2/c1",
			"/a1/b2/c2",
			"/a2/b3/c1",
		}

		for _, path := range dirs {
			err := os.MkdirAll(workspace+path, 0777)
			test.AssertNoErr(err)
		}

		w := test.RelPath(workspace)

		checkNlink(test, w+"/a1", 4, 4)
		checkNlink(test, w+"/a1/.", 4, 4)
		checkNlink(test, w+"/a1/b1/..", 4, 4)
		checkNlink(test, w+"/a1/b1", 3, 3)
		checkNlink(test, w+"/a1/b1/.", 3, 3)
		checkNlink(test, w+"/a1/b2", 4, 4)
		checkNlink(test, w+"/a1/b2/c1/..", 4, 4)
		checkNlink(test, w+"/a1/b2/c1", 2, 2)
		checkNlink(test, w+"/a1/b2/c1/.", 2, 2)
	})
}

func TestStickyDirPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"
		testFile := testDir + "/testFile"

		// ensure sticky bit is set
		err := syscall.Mkdir(testDir, 01777)
		test.AssertNoErr(err)

		err = testutils.PrintToFile(testFile, string(GenData(2000)))
		test.AssertNoErr(err)

		err = os.Chown(testFile, 99, 99)
		test.AssertNoErr(err)

		defer test.SetUidGid(99, 99, nil).Revert()

		// we should be able to remove it, even though sticky bit is set
		err = os.Remove(testFile)
		test.AssertNoErr(err)
	})
}

// This function generates a test that cannot run in parallel as it
// changes the current working directory of the process
func deleteCWDTestGen(runAsRoot bool) func(*testHelper) {
	return func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testdir"
		fullname := workspace + "/" + name

		if !runAsRoot {
			defer test.SetUidGid(99, -1, nil).Revert()
		}

		err := syscall.Mkdir(fullname, 0777)
		test.AssertNoErr(err)

		wd, err := os.Getwd()
		test.AssertNoErr(err)

		err = os.Chdir(fullname)
		test.AssertNoErr(err)

		defer func() {
			err = os.Chdir(wd)
			test.AssertNoErr(err)
		}()

		err = syscall.Rmdir(fullname)
		test.AssertNoErr(err)

		const ATCWD = -0x64
		f, err := syscall.Openat(ATCWD, ".",
			syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK, 0)
		test.AssertNoErr(err)

		buf := make([]byte, 1000)
		_, err = syscall.Getdents(f, buf)
		test.Assert(err == syscall.ENOENT, "Wrong error %v", err)

		err = syscall.Close(f)
		test.AssertNoErr(err)
	}
}

func TestDeleteCWDUnprivilegedUser(t *testing.T) {
	runExpensiveTest(t, deleteCWDTestGen(false))
}

func TestDeleteCWDRootUser(t *testing.T) {
	runExpensiveTest(t, deleteCWDTestGen(true))
}

func TestDeleteOpenDirWithChild(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name := "testdir"

		fullname := workspace + "/" + name

		defer test.SetUidGid(99, -1, nil).Revert()

		err := syscall.Mkdir(fullname, 0777)
		test.AssertNoErr(err)
		err = syscall.Mknod(fullname+"/file", syscall.S_IFREG, 0)
		test.Assert(err == nil, "Unable to create test file: %v", err)

		f, err := os.Open(fullname)
		test.AssertNoErr(err)

		list, err := f.Readdir(-1)
		test.AssertNoErr(err)
		test.Assert(len(list) == 1, "list %v has wrong size", list)

		err = syscall.Unlink(fullname + "/file")
		test.AssertNoErr(err)

		err = syscall.Rmdir(fullname)
		test.AssertNoErr(err)

		_, err = f.Seek(0, 0)
		test.AssertNoErr(err)
		list, err = f.Readdir(-1)
		test.Assert(len(list) == 0, "list %v has wrong size", list)

		const ATCWD = -0x64
		f2, err := syscall.Openat(int(f.Fd()), ".",
			syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK, 0)
		test.AssertNoErr(err)

		err = f.Close()
		test.AssertNoErr(err)

		err = syscall.Close(f2)
		test.AssertNoErr(err)
	})
}

func TestDirectorySeekMiddle(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		fullname := workspace + "/" + "testdir"
		test.AssertNoErr(syscall.Mkdir(fullname, 0777))
		test.AssertNoErr(CreateSmallFile(fullname+"/f0", ""))
		test.AssertNoErr(CreateSmallFile(fullname+"/f1", ""))

		f, err := os.Open(fullname)
		test.AssertNoErr(err)
		defer f.Close()

		_, err = f.Seek(0, os.SEEK_SET)
		buf := make([]byte, 1000)
		n, err := syscall.Getdents(int(f.Fd()), buf)
		test.AssertNoErr(err)
		_, count, offs := utils.ParseDirentOffsets(buf, n, nil)
		// There should be an entry for each file
		test.Assert(count == 2, "Bad number of directory entries %d", count)
		_, _, names := syscall.ParseDirent(buf, n, nil)
		for i, off := range offs {
			test.Log("Offset of entry %s is %d", names[i], off)
		}

		getNDirents := func(offset int64) int {
			test.Log("Seeking to offset %d", offset)
			_, err = syscall.Seek(int(f.Fd()), offset, os.SEEK_SET)
			test.AssertNoErr(err)
			buf := make([]byte, 1000)
			n, err = syscall.Getdents(int(f.Fd()), buf)
			test.AssertNoErr(err)
			_, count, names = syscall.ParseDirent(buf, n, nil)
			for i, name := range names {
				test.Log("Name %d is %s", i, name)
			}
			return count
		}

		res0 := getNDirents(offs[0])
		res1 := getNDirents(offs[1])
		test.Assert(res0+res1 == 1,
			"The offsets should result in 0 or 1. Got %d and %d.",
			res0, res1)
	})
}

func TestDirectoryReadStaleDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir := workspace + "/" + "testdir"
		test.AssertNoErr(syscall.Mkdir(dir, 0777))

		// The kernel uses the batch size of 25 when reading dentries of
		// a directory, therefore, we should use a larger number to make
		// sure not every file is read in the first readdirnames
		const nFiles = 26

		for i := 0; i < nFiles; i++ {
			test.AssertNoErr(CreateSmallFile(
				fmt.Sprintf("%s/f%d", dir, i), ""))
			test.AssertNoErr(CreateSmallFile(
				fmt.Sprintf("%s/g%d", dir, i), ""))
		}

		f, err := os.Open(dir)
		test.AssertNoErr(err)
		defer f.Close()

		_, err = f.Readdirnames(1)
		test.AssertNoErr(err)

		for i := 0; i < nFiles; i++ {
			test.AssertNoErr(
				syscall.Unlink(fmt.Sprintf("%s/f%d", dir, i)))
			test.AssertNoErr(syscall.Symlink(
				fmt.Sprintf("%s/g%d", dir, i),
				fmt.Sprintf("%s/f%d", dir, i)))
		}
		_, err = f.Readdirnames(100)
		test.AssertNoErr(err)

		for i := 0; i < nFiles; i++ {
			_, err = os.Readlink(fmt.Sprintf("%s/f%d", dir, i))
			test.AssertNoErr(err)
			test.AssertNoErr(syscall.Unlink(
				fmt.Sprintf("%s/f%d", dir, i)))
		}
	})
}

// Test our inherent assumption that underlying container size is independent of
// the data contents in the buffer
func TestProtoSizeConsistency(t *testing.T) {
	runTest(t, func(test *testHelper) {
		_, entry := quantumfs.NewDirectoryEntry(1)
		record := quantumfs.NewDirectoryRecord()

		entry.SetEntry(0, record.Publishable())
		sizeA := len(entry.Bytes())

		_, entry = quantumfs.NewDirectoryEntry(1)
		record = quantumfs.NewDirectoryRecord()
		record.SetType(255)
		record.SetPermissions(1048577)
		entry.SetEntry(0, record.Publishable())
		sizeB := len(entry.Bytes())

		_, entry = quantumfs.NewDirectoryEntry(1)
		record = quantumfs.NewDirectoryRecord()
		record.SetType(0)
		record.SetPermissions(0)
		entry.SetEntry(0, record.Publishable())
		sizeC := len(entry.Bytes())

		_, entry = quantumfs.NewDirectoryEntry(1)
		record = quantumfs.NewDirectoryRecord()
		record.SetType(16)
		record.SetPermissions(32)
		entry.SetEntry(0, record.Publishable())
		sizeD := len(entry.Bytes())

		_, entry = quantumfs.NewDirectoryEntry(1)
		record = quantumfs.NewDirectoryRecord()
		record.SetType(0)
		record.SetPermissions(0)
		entry.SetEntry(0, record.Publishable())
		sizeE := len(entry.Bytes())

		test.Assert(sizeA == sizeB && sizeB == sizeC && sizeC == sizeD &&
			sizeD == sizeE,
			"Underlying container size changes %d %d %d %d %d", sizeA,
			sizeB, sizeC, sizeD, sizeE)
	})
}

func TestMaxDirectoryRecordsSize(t *testing.T) {
	runTest(t, func(test *testHelper) {
		record := quantumfs.NewDirectoryRecord()

		_, entry := quantumfs.NewDirectoryEntry(0 +
			quantumfs.MaxDirectoryRecords())

		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		offset := int(r.Uint32() % 1000000)
		names := GenData(offset + (quantumfs.MaxDirectoryRecords() *
			quantumfs.MaxFilenameLength))
		names = names[offset:]

		// Capnproto compresses data, so make it random to minimize that
		for i := 0; i < quantumfs.MaxDirectoryRecords(); i++ {
			name := names[i*quantumfs.MaxFilenameLength:]
			name = name[:quantumfs.MaxFilenameLength]
			record.SetFilename(string(name))

			idData := name[:quantumfs.ObjectKeyLength]
			record.SetID(quantumfs.NewObjectKeyFromBytes(idData))
			record.SetType(quantumfs.ObjectType(r.Int()))
			record.SetPermissions(r.Uint32())
			record.SetOwner(quantumfs.UID(r.Uint32()))
			record.SetGroup(quantumfs.GID(r.Uint32()))
			record.SetSize(r.Uint64())
			extData := name[quantumfs.ObjectKeyLength:]
			extData = extData[:quantumfs.ObjectKeyLength]
			extKey := quantumfs.NewObjectKeyFromBytes(extData)
			record.SetExtendedAttributes(extKey)
			record.SetContentTime(quantumfs.Time(r.Uint64()))
			record.SetModificationTime(quantumfs.Time(r.Uint64()))
			record.SetFileId(quantumfs.FileId(r.Uint64()))

			entry.SetEntry(i, record.Publishable())
		}

		test.Assert(len(entry.Bytes()) < quantumfs.MaxBlockSize,
			"MaxDirectoryRecords is incorrect: %d block vs %d (%d)",
			len(entry.Bytes()), quantumfs.MaxBlockSize,
			quantumfs.MaxDirectoryRecords())
	})
}

func TestDirectorySetAttrUidPermsRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := workspace + "/file"

		test.AssertNoErr(os.Mkdir(filename, 0777))

		// Root is always allowed to change UID
		test.AssertNoErr(os.Chown(filename, 99, 99))

		// Nobody else is allowed to change UID
		defer test.SetUidGid(99, 99, []int{}).Revert()

		err := os.Chown(filename, 0, 99)
		test.Assert(err != nil && os.IsPermission(err),
			"unexpected chown error %s", err.Error())

		// However, a call to chown which doesn't change the UID is fine
		test.AssertNoErr(os.Chown(filename, 99, 99))
	})
}

func TestDirectoryUnlinkChildNoWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		defer test.SetUidGid(99, 99, nil).Revert()

		test.AssertNoErr(utils.MkdirAll(workspace+"/a/b/c", 0777))
		test.AssertNoErr(os.Chmod(workspace+"/a/b", 0555))

		err := syscall.Rmdir(workspace + "/a/b/c")
		test.AssertErr(err)
		test.Assert(err == syscall.EACCES, "Unexpected error: %s",
			err.Error())
	})
}

// Read the directory one entry at a time.
func (test *testHelper) smallReaddirnames(file *os.File, length int) []string {
	names := make([]string, length)
	count := 0
	for {
		d, err := file.Readdirnames(1)
		if err == io.EOF {
			break
		}
		test.AssertNoErr(err)
		test.Assert(len(d) > 0,
			"readdirnames %q returned empty slice", file.Name())
		names[count] = d[0]
		count++
	}
	return names[0:count]
}

// This test is a modified version of a test from golang's os_test with
// the same name
func TestReaddirnamesOneAtATime(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir := workspace + "/subdir"
		test.AssertNoErr(utils.MkdirAll(dir, 0777))

		for i := 0; i < 100; i++ {
			test.createFile(dir, fmt.Sprintf("file_%d", i), 0)
		}

		file, err := os.Open(dir)
		test.AssertNoErr(err)
		defer file.Close()

		all, err := file.Readdirnames(-1)
		test.AssertNoErr(err)

		file1, err := os.Open(dir)
		test.AssertNoErr(err)
		defer file1.Close()

		small := test.smallReaddirnames(file1, len(all)+100)
		test.Assert(len(small) >= len(all),
			"len(small) is %d, less than %d", len(small), len(all))

		for i, n := range all {
			test.Assert(small[i] == n,
				"small read %q mismatch: %v", small[i], n)
		}
	})
}

func TestDirectoryNlink(t *testing.T) {
	t.Skip("BUG198654")
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir := workspace + "/dir"

		test.AssertNoErr(utils.MkdirAll(dir+"/subdir1", 0124))
		test.AssertNoErr(utils.MkdirAll(dir+"/subdir2", 0124))
		test.createFile(workspace, "dir/file1", 1000)
		test.createFile(workspace, "dir/file2", 1000)
		test.createFile(workspace, "dir/file3", 1000)

		var stat syscall.Stat_t
		test.AssertNoErr(syscall.Stat(dir, &stat))
		test.Assert(stat.Nlink == 4, "wrong nlink %d", stat.Nlink)
	})
}

func TestRenameCtime(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name1 := "file1"
		name2 := "file2"
		var stat1, stat2 syscall.Stat_t

		test.createFile(workspace, name1, 1000)
		test.AssertNoErr(syscall.Stat(workspace+"/"+name1, &stat1))

		// Wait to make sure some time passes between the creation and
		// the move
		time.Sleep(10 * time.Millisecond)
		test.moveFile(workspace, name1, name2)
		test.AssertNoErr(syscall.Stat(workspace+"/"+name2, &stat2))

		test.Assert(stat1.Ctim.Nano() < stat2.Ctim.Nano(),
			"rename did not update ctime %d vs. %d",
			stat1.Ctim.Nano(), stat2.Ctim.Nano())
	})
}

func TestMvChildCtime(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		name1 := "dir1/file1"
		name2 := "file2"
		var stat1, stat2 syscall.Stat_t

		utils.MkdirAll(workspace+"/dir1", 0777)
		test.createFile(workspace, name1, 1000)
		test.AssertNoErr(syscall.Stat(workspace+"/"+name1, &stat1))

		// Wait to make sure some time passes between the creation and
		// the move
		time.Sleep(10 * time.Millisecond)
		test.moveFile(workspace, name1, name2)
		test.AssertNoErr(syscall.Stat(workspace+"/"+name2, &stat2))

		test.Assert(stat1.Ctim.Nano() < stat2.Ctim.Nano(),
			"rename did not update ctime %d vs. %d",
			stat1.Ctim.Nano(), stat2.Ctim.Nano())
	})
}

func TestMvChildRace(t *testing.T) {
	runTestCustomConfig(t, dirtyDelay100Ms, func(test *testHelper) {
		workspace := test.NewWorkspace()

		test.AssertNoErr(os.Mkdir(workspace+"/dirB", 0777))
		test.AssertNoErr(os.Mkdir(workspace+"/dirA", 0777))
		// We've now created an oddity: parent has a higher inode than child
		test.AssertNoErr(os.Rename(workspace+"/dirB",
			workspace+"/dirA/dirB"))
		testutils.PrintToFile(workspace+"/dirA/dirB/file", "some data")

		// sync workspace so we can order the dirty queue specifically
		test.SyncAllWorkspaces()

		// dirty dirB to cause it to flush (up operation)
		test.AssertNoErr(os.Chmod(workspace+"/dirA/dirB", 0766))
		time.Sleep(80 * time.Millisecond)

		// Race between an up and what may be a down operation
		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			time.Sleep(6 * time.Millisecond)
			// Down operation between dirA and dirB
			os.Rename(workspace+"/dirA/dirB/file",
				workspace+"/dirA/file")
			wg.Done()
		}()

		wg.Wait()
	})
}
