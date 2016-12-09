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

func TestDirectoryCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/" + "test"
		err := syscall.Mkdir(testFilename, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ing test dir: %v", err)
		test.assert(stat.Size == int64(qfsBlockSize), "Incorrect Size: %d",
			stat.Size)
		test.assert(stat.Nlink == 2, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFDIR
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IWGRP |
			syscall.S_IROTH
		test.assert(stat.Mode == expectedPermissions,
			"Directory permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		dirName := workspace + "/test/a/b"
		err := os.MkdirAll(dirName, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(dirName, &stat)
		test.assert(err == nil, "Error stat'ing test dir: %v", err)
		test.assert(stat.Size == int64(qfsBlockSize), "Incorrect Size: %d",
			stat.Size)
		test.assert(stat.Nlink == 2, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFDIR
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IWGRP |
			syscall.S_IROTH
		test.assert(stat.Mode == expectedPermissions,
			"Directory permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryFileCreation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		dirName := workspace + "/test/a/b"
		testFilename := dirName + "/c"

		err := os.MkdirAll(dirName, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		fd, err := syscall.Creat(testFilename, 0124)
		test.assert(err == nil, "Error creating file: %v", err)

		err = syscall.Close(fd)
		test.assert(err == nil, "Error closing fd: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)
		test.assert(stat.Size == 0, "Incorrect Size: %d", stat.Size)
		test.assert(stat.Nlink == 2, "Incorrect Nlink: %d", stat.Nlink)

		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFREG
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IWGRP |
			syscall.S_IROTH
		test.assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)
	})
}

func TestRecursiveDirectoryFileDescriptorDirtying(t *testing.T) {
	runTest(t, func(test *testHelper) {
		// Create a file and determine its inode numbers
		workspace := test.newWorkspace()
		wsNamespaceName, wsWorkspaceName :=
			test.getWorkspaceComponents(workspace)

		dirName := workspace + "/test/a/b"
		testFilename := dirName + "/" + "test"

		err := os.MkdirAll(dirName, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		fd, err := syscall.Creat(testFilename, 0124)
		test.assert(err == nil, "Error creating file: %v", err)
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ind test file: %v", err)
		test.assert(stat.Ino >= quantumfs.InodeIdReservedEnd,
			"File had reserved inode number %d", stat.Ino)

		// Find the matching FileHandle
		descriptors := test.fileDescriptorFromInodeNum(stat.Ino)
		test.assert(len(descriptors) == 1,
			"Incorrect number of fds found 1 != %d", len(descriptors))
		fileDescriptor := descriptors[0]
		file := fileDescriptor.file

		// Save the workspace rootId, change the File key, simulating
		// changing the data, then mark the matching FileDescriptor dirty.
		// This should trigger a refresh up the hierarchy and, after we
		// trigger a delayed sync, change the workspace rootId and mark the
		// fileDescriptor clean.
		oldRootId := test.workspaceRootId(wsNamespaceName,
			wsWorkspaceName)

		c := test.newCtx()
		_, err = file.accessor.writeBlock(c, 0, 0, []byte("update"))
		test.assert(err == nil, "Failure modifying small file")
		fileDescriptor.dirty(c)

		test.syncAllWorkspaces()
		newRootId := test.workspaceRootId(wsNamespaceName,
			wsWorkspaceName)

		test.assert(oldRootId != newRootId, "Workspace rootId didn't change")
		test.assert(!file.isDirty(),
			"FileDescriptor not cleaned after change")

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

		src := test.newWorkspace()
		src = test.relPath(src)

		dst := "dirupdate/test"

		// First create a file
		testFilename := test.absPath(src + "/" + "test")
		fd, err := os.Create(testFilename)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		// Then branch the workspace
		err = api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		// Ensure the new workspace has the correct file
		testFilename = test.absPath(dst + "/" + "test")
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Workspace copy doesn't match: %v", err)
	})
}

func TestDirectoryFileDeletion(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := os.Create(testFilename)
		test.assert(err == nil, "Error creating file: %v", err)
		err = fd.Close()
		test.assert(err == nil, "Error closing fd: %v", err)

		err = syscall.Unlink(testFilename)
		test.assert(err == nil, "Error unlinking file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err != nil, "Error test file not deleted: %v", err)

		// Confirm after branch
		workspace = test.absPath(test.branchWorkspace(workspace))
		testFilename = workspace + "/" + "test"
		err = syscall.Stat(testFilename, &stat)
		test.assert(err != nil, "Error test file not deleted: %v", err)
	})
}

func checkUnlink(test *testHelper, file string, expectedErr syscall.Errno,
	dir string, notOwn bool) {

	fd, err := syscall.Creat(file, 0)
	test.assert(err == nil, "Error creating file: %v", err)
	syscall.Close(fd)

	if dir != "" {
		var stat syscall.Stat_t
		err = syscall.Stat(dir, &stat)
		test.assert(err == nil,
			"Error getting file stat with %v: %d, %d, %o",
			err, stat.Uid, stat.Gid, stat.Mode)

		err = syscall.Chmod(dir, stat.Mode|syscall.S_ISVTX)
		test.assert(err == nil, "Error change the mode of file: %v", err)

		err = syscall.Stat(dir, &stat)
		test.assert(err == nil && stat.Mode&syscall.S_ISVTX != 0,
			"Error getting file stat with %v: %d, %d, %o",
			err, stat.Uid, stat.Gid, stat.Mode)
		if notOwn {
			err = syscall.Chown(file, 100, 100)
			test.assert(err == nil,
				"Error change the ownership of file: %v", err)
		}

	}

	err = syscall.Unlink(file)
	if expectedErr == 0 {
		test.assert(err == nil, "Error unlinking file %s : %v", file, err)
	} else {
		test.assert(err == expectedErr,
			"Incorrect error unlinking file %s : %v", file, err)
	}
}

func modifyVerifyChown(test *testHelper, dir string, uid int, gid int) {
	// Try to change the ownership of the rootDir to uid, gid
	err := os.Chown(dir, uid, gid)
	test.assert(err == nil, "Failed to chown: %v", err)

	// Verify the file's uid is 99 and gid is 0
	var stat syscall.Stat_t
	err = syscall.Stat(dir, &stat)
	test.assert(err == nil && int(stat.Uid) == uid && int(stat.Gid) == gid,
		"Error getting directory stat with %v: %d, %d",
		err, stat.Uid, stat.Gid)
}

// Check the root user bypassing all the permission
func TestUnlinkPermissionRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		// No Write/Excute permission on directory
		rootDir := workspace + "/" + "Root"
		err := os.Mkdir(rootDir, 0)
		test.assert(err == nil, "Failed to create a direcotry: %v", err)
		checkUnlink(test, rootDir+"/"+"NoPermissions", 0, "", false)

		// Directory and file are not owned when sticky bit is set
		modifyVerifyChown(test, rootDir, 100, 100)
		checkUnlink(test, rootDir+"/"+"NoOwnership", 0, rootDir, true)

	})
}

func TestUnlinkUserPermission(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		test.setUidGid(99, -1)
		defer test.setUidGidToDefault()

		// Check non-existing file
		nonExist := workspace + "/" + "nonExist"
		err := syscall.Unlink(nonExist)
		test.assert(err == syscall.ENOENT, "Error unlinking file: %v", err)

		// The directory is the WorkspaceRoot whose permission is 777,
		// It should be able to unlink files with any permission
		testFilename := workspace + "/" + "testFile"
		checkUnlink(test, testFilename, 0, "", false)

		testDir := workspace + "/" + "testDir"
		err = os.Mkdir(testDir, 0124)
		test.assert(err == nil, "Error creating directory: %v", err)

		// Directory without enough permission
		checkUnlink(test, testDir+"/"+"File", syscall.EACCES, "", false)

		// Give the parent directory enough user permission
		err = os.Chmod(testDir, 0300)
		test.assert(err == nil, "Error change the mode of directory: %v",
			err)
		checkUnlink(test, testDir+"/"+"Owner", 0, "", false)

		// Check the group/other permission
		err = os.Chmod(testDir, 0077)
		test.assert(err == nil, "Error change the mode of directory: %v",
			err)
		checkUnlink(test, testDir+"/"+"Grp", syscall.EACCES, "", false)
		checkUnlink(test, testDir+"/"+"Other", syscall.EACCES, "", false)

		// Testcases when sticky bit is set
		// Have ownership of directory or both but without permission
		checkUnlink(test, testDir+"/"+"Sticky-DirNoPerm",
			syscall.EACCES, testDir, true)
		checkUnlink(test, testDir+"/"+"Sticky-DualNoPerms",
			syscall.EACCES, testDir, false)

		// Have ownership of directory or both with permission
		err = os.Chmod(testDir, 0777)
		test.assert(err == nil, "Error change the mode of directory: %v",
			err)

		checkUnlink(test, testDir+"/"+"Sticky-DirPerm", 0, testDir, true)
		checkUnlink(test, testDir+"/"+"Sticky-DualPerms", 0, testDir, false)

	})
}

func TestUnlinkGroupPermission(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		test.setUidGid(99, -1)
		defer test.setUidGidToDefault()

		testDir := workspace + "/" + "testDir"
		err := os.Mkdir(testDir, 0747)
		test.assert(err == nil, "Error creating directory: %v", err)

		modifyVerifyChown(test, testDir, 100, 0)
		checkUnlink(test, testDir+"/"+"Grp-NoPerm",
			syscall.EACCES, "", false)

		// Have ownership of file or no ownership when sticky bit is set
		checkUnlink(test, testDir+"/"+"Sticky-FileNoPerm",
			syscall.EACCES, testDir, false)
		checkUnlink(test, testDir+"/"+"Sticky-NoOwnNoPerms",
			syscall.EACCES, testDir, true)

		// Have the group permission
		err = os.Chmod(testDir, 0030)
		test.assert(err == nil, "Error change the mode of directory: %v",
			err)
		checkUnlink(test, testDir+"/"+"Grp-Perm", 0, "", false)

		checkUnlink(test, testDir+"/"+"Sticky-FilePerm", 0, testDir, false)
		checkUnlink(test, testDir+"/"+"Sticky-NoOwnPerm",
			syscall.EACCES, testDir, true)
	})
}

func TestUnlinkOtherPermission(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		test.setUidGid(99, -1)
		defer test.setUidGidToDefault()

		testDir := workspace + "/" + "testDir"
		err := os.Mkdir(testDir, 0774)
		test.assert(err == nil, "Error creating directory: %v", err)

		modifyVerifyChown(test, testDir, 100, 100)
		checkUnlink(test, testDir+"/"+"Other-NoPerm",
			syscall.EACCES, "", false)

		// Have ownership of file or no ownership when sticky bit is set
		checkUnlink(test, testDir+"/"+"Sticky-FileNoPerm",
			syscall.EACCES, testDir, false)
		checkUnlink(test, testDir+"/"+"Sticky-NoOwnNoPerm",
			syscall.EACCES, testDir, true)

		// Have the other permission
		err = os.Chmod(testDir, 0003)
		test.assert(err == nil, "Error change the mode of directory: %v",
			err)
		checkUnlink(test, testDir+"/"+"Other-Perm", 0, "", false)

		checkUnlink(test, testDir+"/"+"Sticky-FilePerm", 0, testDir, false)
		checkUnlink(test, testDir+"/"+"Sticky-NoOwnPerm",
			syscall.EACCES, testDir, true)
	})
}

func TestDirectoryUnlinkDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testDir := workspace + "/" + "test"
		err := os.Mkdir(testDir, 0124)
		test.assert(err == nil, "Error creating directory: %v", err)

		err = syscall.Unlink(testDir)
		test.assert(err != nil, "Expected error unlinking directory")
		test.assert(err == syscall.EISDIR, "Error not EISDIR: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testDir, &stat)
		test.assert(err == nil, "Error test directory was deleted")
	})
}

func TestDirectoryRmdirEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testDir := workspace + "/test"
		err := os.Mkdir(testDir, 0124)
		test.assert(err == nil, "Error creating directory: %v", err)

		err = syscall.Rmdir(testDir)
		test.assert(err == nil, "Error deleting directory: %v", err)
	})
}

func TestDirectoryRmdirNewlyEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testDir := workspace + "/test"
		err := os.Mkdir(testDir, 0324)
		test.assert(err == nil, "Error creating directory: %v", err)

		testFile := testDir + "/file"
		fd, err := os.Create(testFile)
		test.assert(err == nil, "Error creating file: %v", err)
		fd.Close()

		err = syscall.Unlink(testFile)
		test.assert(err == nil, "Error unlinking file: %v", err)

		err = syscall.Rmdir(testDir)
		test.assert(err == nil, "Error deleting directory: %v", err)
	})
}

func TestDirectoryRmdirNotEmpty(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testDir := workspace + "/test"
		err := os.Mkdir(testDir, 0124)
		test.assert(err == nil, "Error creating directory: %v", err)
		testFile := testDir + "/file"
		fd, err := os.Create(testFile)
		test.assert(err == nil, "Error creating file: %v", err)
		fd.Close()

		err = syscall.Rmdir(testDir)
		test.assert(err != nil, "Expected error when deleting directory")
		test.assert(err == syscall.ENOTEMPTY,
			"Expected error ENOTEMPTY: %v", err)
	})
}

func TestDirectoryRmdirFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFile := workspace + "/test"
		fd, err := os.Create(testFile)
		test.assert(err == nil, "Error creating file: %v", err)
		fd.Close()

		err = syscall.Rmdir(testFile)
		test.assert(err != nil, "Expected error when deleting directory")
		test.assert(err == syscall.ENOTDIR,
			"Expected error ENOTDIR: %v", err)
	})
}

// Confirm that when we reload a directory from the datastore that the classes it
// instantiates matches the type of the entry in the directory.
func TestDirectoryChildTypes(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		testDir := workspace + "/testdir"
		testFile := testDir + "/testfile"

		err := os.Mkdir(testDir, 0124)
		test.assert(err == nil, "Error creating directory: %v", err)

		fd, err := os.Create(testFile)
		test.assert(err == nil, "Error creating file: %v", err)
		test.log("Created file %s", testFile)

		fileContents := "testdata"
		_, err = fd.Write([]byte(fileContents))
		test.assert(err == nil, "Error writing to file: %v", err)
		fd.Close()

		workspace = test.absPath(test.branchWorkspace(workspace))
		testFile = workspace + "/testdir/testfile"

		data, err := ioutil.ReadFile(testFile)
		test.assert(err == nil, "Error reading file in new workspace: %v",
			err)
		test.assert(string(data) == fileContents,
			"File contents differ in branched workspace: %v",
			string(data))
	})
}

func TestLargeDirectory(t *testing.T) {
	runExpensiveTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testdir := workspace + "/testlargedir"
		err := syscall.Mkdir(testdir, 0777)
		test.assert(err == nil, "Error creating directory:%v", err)
		numToCreate := quantumfs.MaxDirectoryRecords +
			quantumfs.MaxDirectoryRecords/4

		// Create enough children to overflow a single block
		for i := 0; i < numToCreate; i++ {
			testFile := fmt.Sprintf("%s/testfile%d", testdir, i)
			fd, err := os.Create(testFile)
			test.assert(err == nil, "Error creating file: %v", err)
			fd.Close()
		}

		// Confirm all the children are accounted for in the original
		// workspace
		files, err := ioutil.ReadDir(testdir)
		test.assert(err == nil, "Error reading directory %v", err)
		attendance := make(map[string]bool, numToCreate)

		for _, file := range files {
			attendance[file.Name()] = true
		}
		test.assert(len(attendance) == numToCreate,
			"Incorrect number of files in directory %d", len(attendance))

		for i := 0; i < numToCreate; i++ {
			testFile := fmt.Sprintf("testfile%d", i)
			test.assert(attendance[testFile], "File %s missing",
				testFile)
		}

		// Confirm all the children are accounted for in a branched
		// workspace
		workspace = test.absPath(test.branchWorkspace(workspace))
		testdir = workspace + "/testlargedir"
		files, err = ioutil.ReadDir(testdir)
		test.assert(err == nil, "Error reading directory %v", err)
		attendance = make(map[string]bool, numToCreate)

		for _, file := range files {
			attendance[file.Name()] = true
		}
		test.assert(len(attendance) == numToCreate,
			"Incorrect number of files in directory %d", len(attendance))

		for i := 0; i < numToCreate; i++ {
			testFile := fmt.Sprintf("testfile%d", i)
			test.assert(attendance[testFile], "File %s missing",
				testFile)
		}

	})
}

func TestDirectoryChmod(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testDir := workspace + "/testdir"

		err := os.Mkdir(testDir, 0)
		test.assert(err == nil, "Failed to create directory: %v", err)

		info, err := os.Stat(testDir)
		test.assert(err == nil, "Failed getting dir info: %v", err)
		test.assert(info.Mode()&os.ModePerm == 0,
			"Initial permissions incorrect %d", info.Mode()&os.ModePerm)

		err = os.Chmod(testDir, 0777)
		test.assert(err == nil, "Error setting permissions: %v", err)

		info, err = os.Stat(testDir)
		test.assert(err == nil, "Failed getting dir info: %v", err)
		test.assert(info.Mode()&os.ModePerm == 0777,
			"Changed permissions incorrect %d", info.Mode()&os.ModePerm)
	})
}

func TestIntraDirectoryRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename1 := workspace + "/test"
		testFilename2 := workspace + "/test2"

		fd, err := os.Create(testFilename1)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(testFilename1, testFilename2)
		test.assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename2, &stat)
		test.assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.absPath(test.branchWorkspace(workspace))
		testFilename2 = workspace + "/test2"
		err = syscall.Stat(testFilename2, &stat)
		test.assert(err == nil, "Rename failed: %v", err)
	})
}

func TestInterDirectoryRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		interDirectoryRename(test)
	})
}

func interDirectoryRename(test *testHelper) {
	workspace := test.newWorkspace()
	testDir1 := workspace + "/dir1"
	testDir2 := workspace + "/dir2"
	testFilename1 := testDir1 + "/test"
	testFilename2 := testDir2 + "/test2"

	err := os.Mkdir(testDir1, 0777)
	test.assert(err == nil, "Failed to create directory: %v", err)
	err = os.Mkdir(testDir2, 0777)
	test.assert(err == nil, "Failed to create directory: %v", err)

	fd, err := os.Create(testFilename1)
	fd.Close()
	test.assert(err == nil, "Error creating test file: %v", err)

	err = os.Rename(testFilename1, testFilename2)
	test.assert(err == nil, "Error renaming file: %v", err)

	var stat syscall.Stat_t
	err = syscall.Stat(testFilename2, &stat)
	test.assert(err == nil, "Rename failed: %v", err)

	// Confirm after branch
	workspace = test.absPath(test.branchWorkspace(workspace))
	testFilename2 = workspace + "/dir2/test2"
	err = syscall.Stat(testFilename2, &stat)
	test.assert(err == nil, "Rename failed: %v", err)
}

func TestRenameIntoParent(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		parent := workspace + "/parent"
		child := workspace + "/parent/child"
		childFile := child + "/test"
		parentFile := parent + "/test2"

		err := os.Mkdir(parent, 0777)
		test.assert(err == nil, "Failed to create directory: %v", err)
		err = os.Mkdir(child, 0777)
		test.assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(childFile)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(childFile, parentFile)
		test.assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(parentFile, &stat)
		test.assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.absPath(test.branchWorkspace(workspace))
		parentFile = workspace + "/parent/test2"
		err = syscall.Stat(parentFile, &stat)
		test.assert(err == nil, "Rename failed: %v", err)
	})
}

func TestRenameIntoChild(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		parent := workspace + "/parent"
		child := workspace + "/parent/child"
		parentFile := parent + "/test"
		childFile := child + "/test2"

		err := os.Mkdir(parent, 0777)
		test.assert(err == nil, "Failed to create directory: %v", err)
		err = os.Mkdir(child, 0777)
		test.assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(parentFile)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(parentFile, childFile)
		test.assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(childFile, &stat)
		test.assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.absPath(test.branchWorkspace(workspace))
		childFile = workspace + "/parent/child/test2"
		err = syscall.Stat(childFile, &stat)
		test.assert(err == nil, "Rename failed: %v", err)
	})
}

func TestRenameIntoIndirectParent(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		parent := workspace + "/parent"
		child := workspace + "/parent/indirect/child"
		childFile := child + "/test"
		parentFile := parent + "/test2"

		err := os.Mkdir(parent, 0777)
		test.assert(err == nil, "Failed to create directory: %v", err)
		err = os.MkdirAll(child, 0777)
		test.assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(childFile)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(childFile, parentFile)
		test.assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(parentFile, &stat)
		test.assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.absPath(test.branchWorkspace(workspace))
		parentFile = workspace + "/parent/test2"
		err = syscall.Stat(parentFile, &stat)
		test.assert(err == nil, "Rename failed: %v", err)
	})
}

func TestRenameIntoIndirectChild(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		parent := workspace + "/parent"
		child := workspace + "/parent/indirect/child"
		parentFile := parent + "/test"
		childFile := child + "/test2"

		err := os.Mkdir(parent, 0777)
		test.assert(err == nil, "Failed to create directory: %v", err)
		err = os.MkdirAll(child, 0777)
		test.assert(err == nil, "Failed to create directory: %v", err)

		fd, err := os.Create(parentFile)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		err = os.Rename(parentFile, childFile)
		test.assert(err == nil, "Error renaming file: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(childFile, &stat)
		test.assert(err == nil, "Rename failed: %v", err)

		// Confirm after branch
		workspace = test.absPath(test.branchWorkspace(workspace))
		childFile = workspace + "/parent/indirect/child/test2"
		err = syscall.Stat(childFile, &stat)
		test.assert(err == nil, "Rename failed: %v", err)
	})
}

func TestSUIDPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		testFilename := workspace + "/test"

		fd, err := os.Create(testFilename)
		fd.Close()
		test.assert(err == nil, "Error creating test file: %v", err)

		mode := 0777 | os.ModeSetuid | os.ModeSetgid | os.ModeSticky
		err = os.Chmod(testFilename, mode)
		test.assert(err == nil, "Error setting permissions: %v", err)

		info, err := os.Stat(testFilename)
		test.assert(err == nil, "Failed getting dir info: %v", err)
		test.assert(info.Mode() == mode,
			"Changed permissions incorrect %d", info.Mode())

		// Confirm after branch
		workspace = test.absPath(test.branchWorkspace(workspace))
		testFilename = workspace + "/test"
		info, err = os.Stat(testFilename)
		test.assert(err == nil, "Failed getting dir info: %v", err)
		test.assert(info.Mode() == mode,
			"Changed permissions incorrect %d", info.Mode())
	})
}

func TestLoadOnDemand(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		dirName := workspace + "/layerA/layerB/layerC"
		fileA := "/layerA/fileA"
		fileB := "/layerA/layerB/fileB"
		fileC := "/layerA/layerB/layerC/fileC"

		data := genData(2400)
		dataA := data[0:800]
		dataB := data[800:1600]
		dataC := data[1600:2400]

		err := os.MkdirAll(dirName, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		err = printToFile(workspace+fileA, string(dataA))
		test.assert(err == nil, "Error creating file: %v", err)
		err = printToFile(workspace+fileB, string(dataB))
		test.assert(err == nil, "Error creating file: %v", err)
		err = printToFile(workspace+fileC, string(dataC))
		test.assert(err == nil, "Error creating file: %v", err)

		newspace := test.absPath(test.branchWorkspace(workspace))
		fileA = newspace + "/layerA/fileA"
		dirB := newspace + "/layerA/layerB"
		fileB = dirB + "/fileB"
		dirC := dirB + "/layerC"
		fileC = dirC + "/fileC"

		// Confirm the inode for fileA isn't instantiated. Getting the stat
		// attributes of an inode shouldn't instantiate it.
		fileNode := test.getInode(fileA)
		test.assert(fileNode == nil, "File inode unexpectedly instantiated")

		// Stat'ing a file should instantiate its parents
		var stat syscall.Stat_t
		err = syscall.Stat(fileB, &stat)
		test.assert(err == nil, "Error stat'ing FileB: %v", err)

		dirNode := test.getInode(dirB)
		test.assert(dirNode != nil,
			"Intermediate directory unexpectedly nil")

		// But siblings shouldn't have been instantiated
		dirNode = test.getInode(dirC)
		test.assert(dirNode == nil, "dirC unexpectedly instantiated")

		// Reading FileB shouldn't make any difference
		layerBData, err := ioutil.ReadFile(fileB)
		test.assert(err == nil, "Unable to read fileB file contents %v", err)
		test.assert(bytes.Equal(layerBData, dataB),
			"dynamically loaded inode data mismatch")
		dirNode = test.getInode(dirC)
		test.assert(dirNode == nil, "dirC unexpectedly instantiated")

		// Ensure we can read without stat'ing first
		layerCData, err := ioutil.ReadFile(fileC)
		test.assert(err == nil, "Unable to read fileC contents %v", err)
		test.assert(bytes.Equal(layerCData, dataC),
			"dynamically loaded inode data mismatch")
	})
}

// Change the ownership of a file to be owned by the user and then confirm that the
// dummy user is used when viewing the permissions as root.
func TestChownUserAsRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()

		testFilename := workspace + "/test"
		fd, err := syscall.Creat(testFilename, 0777)
		syscall.Close(fd)
		test.assert(err == nil, "Error creating file: %v", err)

		err = os.Chown(testFilename, 22222, 22222)
		test.assert(err == nil, "Failed to chown: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Failed to stat file: %v", err)
		test.assert(stat.Uid == 10000, "UID doesn't match: %d", stat.Uid)
		test.assert(stat.Gid == 10000, "GID doesn't match: %d", stat.Gid)
	})
}

// Ensure that when we rewind the directory entry we get new files added after the
// directory was opened.
func TestDirectorySnapshotRefresh(t *testing.T) {
	runTest(t, func(test *testHelper) {
		parent := test.newWorkspace()

		for i := 0; i < 2; i++ {
			dir, err := os.Open(parent)
			test.assert(err == nil, "Failed to open workspace")
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
			test.assert(err == nil, "Error reading two entries: %v", err)

			childName := parent + "/test"
			err = os.Mkdir(childName, 0777)
			test.assert(err == nil, "Error creating child directory: %v",
				err)

			_, err = dir.Seek(0, os.SEEK_SET)
			test.assert(err == nil, "Error seeking directory to start: "+
				"%v", err)

			children, err := dir.Readdirnames(-1)
			test.assert(err == nil, "Error reading all entries of "+
				"directory: %v", err)

			dirExists := false
			for _, name := range children {
				if name == "test" {
					dirExists = true
				}
			}
			test.assert(dirExists,
				"Failed to find new directory after rewind")

			// Run again with the directory instead of the workspace
			parent = childName
		}
	})
}

// Trigger GetAttr on a directory in order to confirm that it works correctly
func TestDirectoryGetAttr(t *testing.T) {
	runTestNoQfsExpensiveTest(t, func(test *testHelper) {
		config := test.defaultConfig()
		config.CacheTimeSeconds = 0
		config.CacheTimeNsecs = 100000
		test.startQuantumFs(config)

		workspace := test.newWorkspace()
		dirName := workspace + "/dir"

		err := syscall.Mkdir(dirName, 0124)
		test.assert(err == nil, "Error creating directory: %v", err)

		wsr, err := os.Open(workspace)
		test.assert(err == nil, "Error opening workspace: %v", err)
		defer wsr.Close()

		dir, err := os.Open(dirName)
		test.assert(err == nil, "Error opening dir: %v", err)
		defer dir.Close()

		time.Sleep(500 * time.Millisecond)

		fileInfo, err := wsr.Stat()
		test.assert(err == nil, "Error stat'ing workspace: %v", err)
		test.assert(fileInfo.Mode()&os.ModePerm == 0777,
			"Workspace permissions incorrect: %d", fileInfo.Mode())

		fileInfo, err = dir.Stat()
		test.assert(err == nil, "Error stat'ing directory: %v", err)
		test.assert(fileInfo.Mode()&os.ModePerm == 0124,
			"Directory permissions incorrect: %d", fileInfo.Mode())
	})
}
