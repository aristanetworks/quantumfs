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

import "github.com/aristanetworks/quantumfs"

func TestDirectoryCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/" + "test"
		err := syscall.Mkdir(testFilename, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ing test dir: %v", err)
		test.assert(stat.Size == qfsBlockSize, "Incorrect Size: %d",
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

func TestRecursiveDirectoryCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		dirName := workspace + "/test/a/b"
		err := os.MkdirAll(dirName, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(dirName, &stat)
		test.assert(err == nil, "Error stat'ing test dir: %v", err)
		test.assert(stat.Size == qfsBlockSize, "Incorrect Size: %d",
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

func TestRecursiveDirectoryFileCreation_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
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

func TestRecursiveDirectoryFileDescriptorDirtying_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		// Create a file and determine its inode numbers
		workspace := test.nullWorkspace()
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
		oldRootId := test.workspaceRootId(quantumfs.NullNamespaceName,
			quantumfs.NullWorkspaceName)

		c := test.newCtx()
		_, err = file.accessor.writeBlock(c, 0, 0, []byte("update"))
		test.assert(err == nil, "Failure modifying small file")
		fileDescriptor.dirty(c)

		test.syncAllWorkspaces()
		newRootId := test.workspaceRootId(quantumfs.NullNamespaceName,
			quantumfs.NullWorkspaceName)

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
func TestDirectoryUpdate_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		api := test.getApi()

		src := test.nullWorkspaceRel()
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

func TestDirectoryFileDeletion_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
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

func TestDirectoryUnlinkDirectory_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
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

func TestDirectoryRmdirEmpty_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testDir := workspace + "/test"
		err := os.Mkdir(testDir, 0124)
		test.assert(err == nil, "Error creating directory: %v", err)

		err = syscall.Rmdir(testDir)
		test.assert(err == nil, "Error deleting directory: %v", err)
	})
}

func TestDirectoryRmdirNewlyEmpty_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		testDir := workspace + "/test"
		err := os.Mkdir(testDir, 0124)
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

func TestDirectoryRmdirNotEmpty_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

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

func TestDirectoryRmdirFile_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

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
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
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

		workspace = test.newWorkspace()
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
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.newWorkspace()
		numToCreate := quantumfs.MaxDirectoryRecords +
			quantumfs.MaxDirectoryRecords/4

		// Create enough children to overflow a single block
		for i := 0; i < numToCreate; i++ {
			testFile := fmt.Sprintf("%s/testfile%d", workspace, i)
			fd, err := os.Create(testFile)
			test.assert(err == nil, "Error creating file: %v", err)
			fd.Close()
		}

		// Confirm all the children are accounted for in the original
		// workspace
		files, err := ioutil.ReadDir(workspace)
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
		files, err = ioutil.ReadDir(workspace)
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
		test.startDefaultQuantumFs()
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
		test.startDefaultQuantumFs()
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
		test.startDefaultQuantumFs()
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
		test.startDefaultQuantumFs()
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
		test.startDefaultQuantumFs()
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

func TestSUIDPerms(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()
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
		test.startDefaultQuantumFs()
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

		newspace := test.branchWorkspace(workspace)

		// Grab the inode of fileA
		var stat syscall.Stat_t
		err = syscall.Stat(test.absPath(newspace+fileA), &stat)
		test.assert(err == nil, "Error grabbing file inode: %v", err)

		// Now grab the File for it
		fileNode := test.qfs.inode(&test.qfs.c, InodeId(stat.Ino))
		test.assert(fileNode != nil, "File inode for fileA is nil")
		filePtr, ok := fileNode.(*File)
		test.assert(ok, "Inode for fileA is not a File")

		// Now grab its parent so we can check to make sure B/C are shallow
		layerA := filePtr.parent()
		test.assert(layerA != nil, "Directory inode for layerA is nil")
		dirAPtr, ok := layerA.(*Directory)
		test.assert(ok, "Inode for layerA is not a Directory")
		test.assert(dirAPtr.dirChildren.data != nil,
			"Touched directory hasn't loaded children")

		layerBInode := dirAPtr.dirChildren.data.fileToInode["layerB"]
		layerB := test.qfs.inode(&test.qfs.c, InodeId(layerBInode))
		test.assert(layerB != nil, "Directory inode for layerB is nil")

		dirBPtr, ok := layerB.(*Directory)
		test.assert(ok, "Inode for layerB is not a Directory")
		test.assert(dirBPtr.dirChildren.data == nil,
			"Nested directory's children loaded unecessarily")

		// Now read layer B to trigger layer C to be shallow loaded
		layerBData, err := ioutil.ReadFile(test.absPath(newspace + fileB))
		test.assert(err == nil, "Unable to read fileB file contents %v", err)
		test.assert(bytes.Equal(layerBData, dataB),
			"dynamically loaded inode data mismatch")

		layerCInode := dirBPtr.dirChildren.data.fileToInode["layerC"]
		layerC := test.qfs.inode(&test.qfs.c, InodeId(layerCInode))
		test.assert(layerC != nil, "Directory inode for layerC is nil")

		dirCPtr, ok := layerC.(*Directory)
		test.assert(ok, "Inode for layerC is not a Directory")
		test.assert(dirCPtr.dirChildren.data == nil,
			"Nested directory's children loaded unecessarily")

		layerCData, err := ioutil.ReadFile(test.absPath(newspace + fileC))
		test.assert(err == nil, "Unable to read fileC contents %v", err)
		test.assert(bytes.Equal(layerCData, dataC),
			"dynamically loaded inode data mismatch")
	})
}

func TestInodeForget(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()
		workspace := test.newWorkspace()

		dirName := workspace + "/layerA/layerB"
		fileA := "/layerA/fileA"
		fileB := "/layerA/layerB/fileB"

		data := genData(2400)
		dataA := data[0:800]
		dataB := data[800:1600]

		err := os.MkdirAll(dirName, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		err = printToFile(workspace+fileA, string(dataA))
		test.assert(err == nil, "Error creating file: %v", err)
		err = printToFile(workspace+fileB, string(dataB))
		test.assert(err == nil, "Error creating file: %v", err)

		// Grab the inode of fileA
		var stat syscall.Stat_t
		err = syscall.Stat(workspace+fileA, &stat)
		test.assert(err == nil, "Error grabbing file inode: %v", err)

		// Grab layerA from fileA
		fileNode := test.qfs.inode(&test.qfs.c, InodeId(stat.Ino))
		test.assert(fileNode != nil, "File inode for fileA is nil")
		layerA := fileNode.parent()
		test.assert(layerA != nil, "FileA has no parent")

		// Now lets forget fileA
		test.qfs.allowForget = true
		test.qfs.Forget(stat.Ino, 1)

		// Make sure nothing else gets forgotten
		test.qfs.allowForget = false
		test.assert(test.qfs.inode(&test.qfs.c, layerA.inodeNum()) != nil,
			"Parent of forgotten node also forgotten")
		layerAPtr := layerA.(*Directory)
		layerBId, exists := layerAPtr.dirChildren.getInode(&test.qfs.c,
			"layerB")
		test.assert(exists, "layerB missing from inode structure")
		layerB := test.qfs.inode(&test.qfs.c, layerBId)
		test.assert(layerB != nil,
			"Sibling of forgotten node also forgotten")

		layerBPtr := layerB.(*Directory)
		fileBId, exists := layerBPtr.dirChildren.getInode(&test.qfs.c,
			"fileB")
		test.assert(exists, "fileB missing from inode structure")
		test.assert(test.qfs.inode(&test.qfs.c, fileBId) != nil,
			"Cousin of forgotten node also forgotten")

		// Make sure that fileA actually was forgotten
		test.assert(test.qfs.inode(&test.qfs.c, InodeId(stat.Ino)) == nil,
			"Forgotten inode not forgotten")

		// Now branch the workspace and check that fileA was synced
		newspace := test.branchWorkspace(workspace)

		var output []byte
		output, err = ioutil.ReadFile(test.absPath(newspace+fileA))
		test.assert(err == nil, "Error reading from branched fileA")
		test.assert(bytes.Equal(output, dataA),
			"FileA not synced before forget")
	})
}
