// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the various Api calls

import "os"
import "syscall"
import "testing"

func TestWorkspaceBranching(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		api := test.getApi()

		// First branch the null workspace
		src := test.nullWorkspaceRel()
		dst := "apitest/a"
		err := api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		// Branch the branch to have a writeable workspace
		src = dst
		dst = "apitest/b"
		err = api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		// Then create a file
		testFilename := test.absPath(dst + "/" + "test")
		fd, _ := os.Create(testFilename)
		fd.Close()
		var stat syscall.Stat_t
		err = syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Error stat'ing test file: %v", err)

		// Ensure the first branched workspace wasn't modified
		testFilename = test.absPath(src + "/" + "test")
		err = syscall.Stat(testFilename, &stat)
		test.assert(err != nil, "Original workspace was modified")
	})
}

func TestApiClearAccessList(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		accessList := make(map[string]bool)
		workspace := test.newWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.assert(err == nil, "Create file error:%v", err)
		accessList[filename] = true
		syscall.Close(fd)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		api := test.getApi()

		relpath := test.relPath(workspace)
		err = api.ClearAccessed(relpath)
		test.assert(err == nil,
			"Error clearing accessList with api")
		accessList = make(map[string]bool)
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error maps not clear")
	})
}

func TestApiDuplicateObject(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()
		api := test.getApi()

		// First branch the source workspace
		workspace := test.newWorkspace()
		src := test.relPath(workspace)
		dst := "apitest/duplicate"
		err := api.Branch(src, dst)
		test.assert(err == nil, "Failed to branch workspace: %v", err)

		dirName := workspace + "/test/a"
		dirName1 := dirName + "/b"
		testFilename := dirName1 + "/test"
		linkFilename := workspace + "/link"

		err = os.MkdirAll(dirName1, 0124)
		test.assert(err == nil, "Error creating directories: %v", err)

		fd, err := syscall.Creat(testFilename, 0777)
		syscall.Close(fd)
		test.assert(err == nil, "Error creating a small file: %v", err)

		err = syscall.Symlink(testFilename, linkFilename)
		test.assert(err == nil, "Error creating a symlink: %v", err)

		test.syncAllWorkspaces()

		// get the key from a file
		keyF := make([]byte, TypeKeyLength)
		sz, err := syscall.Getxattr(testFilename, XAttrTypeKey, keyF)
		test.assert(err == nil && sz == TypeKeyLength,
			"Error getting the self-defined typeKeyF: %v with a size of %d",
			err, sz)

		// get the key from a directory
		keyD := make([]byte, TypeKeyLength)
		sz, err = syscall.Getxattr(dirName1, XAttrTypeKey, keyD)
		test.assert(err == nil && sz == TypeKeyLength,
			"Error getting the self-defined typeKeyD: %v with a size of %d",
			err, sz)

		// get the key from a Symlink
		keyS := make([]byte, TypeKeyLength)
		sz, err, keyS = lGetXattr(linkFilename, XAttrTypeKey, TypeKeyLength)
		test.assert(err == nil && sz == TypeKeyLength,
			"Error getting the self-defined typeKeyS: %v with a size of %d",
			err, sz)

		dirNameD := test.absPath(dst + "/test/a")
		err = os.MkdirAll(dirNameD, 0124)
		test.assert(err == nil, "Error creating target directories: %v", err)

		// Ensure the workspace root cannot be duplicated
		err = api.DuplicateObject(dst, keyF, 0124, 0022, 0, 0, 0)
		test.assert(err != nil,
			"Error duplicating workspace root")

		// Ensure the non-existing intermediate Inode not be created
		err = api.DuplicateObject(dst+"/nonExist/b", keyF,
			0124, 0022, 0, 0, 0)
		test.assert(err != nil,
			"Error creating non-existing Inode")

		// Ensure the target node does not exist
		err = api.DuplicateObject(dst+"/test/a", keyF,
			0124, 0022, 0, 0, 0)
		test.assert(err != nil,
			"Error having the target node already")

		// Duplicate the file in the given path
		err = api.DuplicateObject(dst+"/test/a/file", keyF,
			0124, 0022, 0, 0, 0)
		test.assert(err == nil,
			"Error duplicating a file to workspaceT: %v", err)

		var stat syscall.Stat_t
		err = syscall.Stat(test.absPath(dst+"/test/a/file"), &stat)
		test.assert(err == nil, "Error get status of a file: %v", err)

		// check the mode of file
		var expectedPermissions uint32
		expectedPermissions |= syscall.S_IFREG
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IROTH
		test.assert(stat.Mode == expectedPermissions,
			"File permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)

		//Duplicate the directory in the given path
		err = api.DuplicateObject(dst+"/test/a/dirtest", keyD,
			0124, 0022, 0, 0, 0)
		test.assert(err == nil,
			"Error duplicating a directory to workspaceT: %v", err)

		err = syscall.Stat(test.absPath(dst+"/test/a/dirtest"), &stat)
		test.assert(err == nil, "Error getting status of directory: %v",
			err)

		// check the mode of directory
		expectedPermissions = syscall.S_IFDIR
		expectedPermissions |= syscall.S_IXUSR | syscall.S_IROTH
		test.assert(stat.Mode == expectedPermissions,
			"Directory permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)

		// testing the file inside of the directory
		err = syscall.Stat(test.absPath(dst+"/test/a/dirtest/test"), &stat)
		test.assert(err == nil, "Error getting status of child file: %v",
			err)

		// check the child is a file
		expectedPermissions = syscall.S_IFREG
		test.assert(stat.Mode&expectedPermissions != 0,
			"Directory permissions incorrect. Expected %x got %x",
			expectedPermissions, stat.Mode)

		// Ensure the symlink in the given path
		err = api.DuplicateObject(dst+"/symlink", keyS,
			0777, 0777, 0, 0, 0)
		test.assert(err == nil,
			"Error duplicating a symlink to workspace: %v", err)

		err = syscall.Lstat(test.absPath(dst+"/symlink"), &stat)
		test.assert(err == nil,
			"Error getting status of Symlink: %v", err)

		expectedPermissions = syscall.S_IFLNK
		test.assert(stat.Mode&expectedPermissions != 0,
			"Symlink permissions incorrect. Expected %x got %x %d",
			expectedPermissions, stat.Mode, stat.Size)
	})
}
