// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various operations on directories, such as creation and traversing

import "bytes"
import "fmt"
import "io/ioutil"
import "os"
import "syscall"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/testutils"
import "github.com/aristanetworks/quantumfs/utils"

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
		oldRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
			wsWorkspaceName)

		c := test.newCtx()
		_, err = file.accessor.writeBlock(c, 0, 0, []byte("update"))
		test.Assert(err == nil, "Failure modifying small file")
		fileDescriptor.dirty(c)

		test.SyncAllWorkspaces()
		newRootId := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
			wsWorkspaceName)

		test.Assert(oldRootId != newRootId, "Workspace rootId didn't change")

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
		testFilename := test.absPath(src + "/" + "test")
		fd, err := os.Create(testFilename)
		fd.Close()
		test.Assert(err == nil, "Error creating test file: %v", err)

		// Then branch the workspace
		err = api.Branch(src, dst)
		test.Assert(err == nil, "Failed to branch workspace: %v", err)

		// Ensure the new workspace has the correct file
		testFilename = test.absPath(dst + "/" + "test")
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
		workspace = test.absPath(test.branchWorkspace(workspace))
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
		test.SetUidGid(99, 99)
		defer test.SetUidGidToDefault()
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

		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

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

		workspace = test.absPath(test.branchWorkspace(workspace))
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
if false {

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
		workspace = test.absPath(test.branchWorkspace(workspace))
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
		workspace = test.absPath(test.branchWorkspace(workspace))
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
	workspace = test.absPath(test.branchWorkspace(workspace))
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
		workspace = test.absPath(test.branchWorkspace(workspace))
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
		workspace = test.absPath(test.branchWorkspace(workspace))
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
		workspace = test.absPath(test.branchWorkspace(workspace))
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
		workspace = test.absPath(test.branchWorkspace(workspace))
		childFile = workspace + "/parent/indirect/child/test2"
		err = syscall.Stat(childFile, &stat)
		test.Assert(err == nil, "Rename failed: %v", err)
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
		workspace = test.absPath(test.branchWorkspace(workspace))
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

		newspace := test.absPath(test.branchWorkspace(workspace))
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
		test.SetUidGid(99, 99)
		defer test.SetUidGidToDefault()

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
		test.SetUidGid(99, 99)
		defer test.SetUidGidToDefault()

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
		test.SetUidGid(99, 99)
		defer test.SetUidGidToDefault()

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
		test.SetUidGid(99, 99)
		defer test.SetUidGidToDefault()

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
		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

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
		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

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
		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

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
		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

		testInodeCreatePermissions(test, testDir, false,
			"Didn't fail creating directory")
	})
}

func TestInodeCreatePermissionsAsUserNoPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		testDir := workspace + "/test"

		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

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

		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

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

		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

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

		test.SetUidGid(99, -1)
		defer test.SetUidGidToDefault()

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

		test.SetUidGid(99, 99)
		defer test.SetUidGidToDefault()

		// we should be able to remove it, even though sticky bit is set
		err = os.Remove(testFile)
		test.AssertNoErr(err)
	})
}
