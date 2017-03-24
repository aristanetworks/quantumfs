// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test some special properties of workspaceroot type

import "bytes"
import "os"
import "syscall"
import "strings"
import "testing"
import "github.com/aristanetworks/quantumfs"

func TestWorkspaceRootApiAccess(t *testing.T) {
	runTest(t, func(test *testHelper) {
		// fix the api path as _null/_null/null/api so that we can verify
		// that api files in workspaceroot are really functional
		workspace := test.nullWorkspace()
		apiPath := workspace + "/" + quantumfs.ApiPath

		stat, err := os.Lstat(apiPath)
		test.assert(err == nil,
			"List api file error%v,%v", err, stat)
		stat_t := stat.Sys().(*syscall.Stat_t)
		test.assert(stat_t.Ino == quantumfs.InodeIdApi,
			"Wrong Inode number for api file")

		src := "_null/_null/null"
		dst := "wsrtest/wsrtest/wsrtest"
		api := quantumfs.NewApiWithPath(apiPath)
		assert(api != nil, "Api nil")
		err = api.Branch(src, dst)
		test.assert(err == nil,
			"Error branching with api in nullworkspace:%v", err)
		api.Close()
	})
}

func testPreparation(test *testHelper, subdirectory string) (string, string,
	[]byte, []byte) {

	baseWorkspace := test.newWorkspace()

	baseDirName := baseWorkspace + subdirectory + "/dir"
	err := os.MkdirAll(baseDirName, 0666)
	test.assert(err == nil, "Error creating a directory: %v", err)

	content := []byte("This is a test")
	baseFileName := baseWorkspace + subdirectory + "/file"
	file, err := os.Create(baseFileName)
	test.assert(err == nil, "Error creating a small file: %v", err)

	n, err := file.Write(content)
	defer file.Close()
	test.assert(err == nil && n == len(content),
		"Error writing to the small file: %v", err)

	attrData := "user.data"
	attrDataData := []byte("sampleData")
	err = syscall.Setxattr(baseFileName, attrData, attrDataData, 0)
	test.assert(err == nil, "Error setting data XAttr: %v", err)

	return baseWorkspace, attrData, attrDataData, content
}

func testWorkspaceWriteNoWritePermission(test *testHelper, subdirectory string) {
	baseWorkspace, attrData, attrDataData, _ :=
		testPreparation(test, subdirectory)

	wsr := test.branchWorkspaceWithoutWritePerm(baseWorkspace)
	workspace := test.absPath(wsr)

	dirName := workspace + subdirectory + "/dir1"
	err := os.Mkdir(dirName, 0666)
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error creating directories: %s", err.Error())

	fileName := workspace + subdirectory + "/file2"
	fd, err := syscall.Creat(fileName, 0777)
	syscall.Close(fd)
	test.assert(err == syscall.EPERM,
		"Error creating a small file: %v", err)

	fileName = workspace + subdirectory + "/dir" + "/file"
	fd, err = syscall.Creat(fileName, 0777)
	syscall.Close(fd)
	test.assert(err == syscall.EPERM,
		"Error creating a small file: %v", err)

	nodeName := workspace + subdirectory + "/node"
	err = syscall.Mknod(nodeName,
		syscall.S_IFBLK|syscall.S_IRWXU, 0x12345678)
	test.assert(err == syscall.EPERM, "Error creating node: %v", err)

	targetFile := workspace + subdirectory + "/file"
	linkName := workspace + subdirectory + "/link"
	err = os.Symlink(targetFile, linkName)
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error creating symlink: %v", err)

	err = os.Link(targetFile, linkName)
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error creating hardlink: %v", err)

	err = os.RemoveAll(targetFile)
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error unlinking file: %v", err)

	targetDir := workspace + subdirectory + "/dir"
	err = os.RemoveAll(targetDir)
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error unlinking directory: %v", err)

	file, err := os.OpenFile(targetFile, os.O_RDWR, 0)
	file.Close()
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error opening the file: %v", err)

	err = os.Rename(targetFile, workspace+"/newFile")
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error renaming file: %v", err)

	attrDataData = []byte("extendedattributedata")
	err = syscall.Setxattr(targetFile, attrData, attrDataData, 0)
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error setting data XAttr: %v", err)

	err = syscall.Removexattr(targetFile, attrData)
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error deleting data XAttr: %v", err)

	err = syscall.Chmod(targetFile, 0123)
	test.assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error Set file permission attribute: %v", err)
}

func testWorkspaceReadNoWritePermission(test *testHelper, subdirectory string) {
	baseWorkspace, attrData, attrDataData, content :=
		testPreparation(test, subdirectory)

	baseLinkName := baseWorkspace + subdirectory + "/link"
	err := syscall.Link(baseWorkspace+subdirectory+"/file", baseLinkName)
	test.assert(err == nil, "Error creating hardlink: %v", err)

	baseFileName := baseWorkspace + subdirectory + "/dir/file1"
	file, err := os.Create(baseFileName)
	file.Close()
	test.assert(err == nil, "Error creating a small file: %v", err)

	wsr := test.branchWorkspaceWithoutWritePerm(baseWorkspace)
	workspace := test.absPath(wsr)

	targetFile := workspace + subdirectory + "/file"
	file, err = os.OpenFile(targetFile, os.O_RDONLY, 0)
	defer file.Close()
	test.assert(err == nil, "Error opening the file: %v", err)

	buf := make([]byte, 256)
	n, err := file.Read(buf)
	test.assert(err == nil && bytes.Equal(buf[:n], content),
		"Error reading the exact content: %s with error: %v",
		string(buf[:n]), err)

	targetDir := workspace + subdirectory + "/dir"
	dir, err := os.Open(targetDir)
	defer dir.Close()
	test.assert(err == nil, "Error opening the directory: %v", err)

	names, err := dir.Readdirnames(0)
	test.assert(err == nil && len(names) == 1 && names[0] == "file1",
		"Error reading directory: %v with error: %v", names, err)

	stat, err := os.Lstat(targetFile)
	test.assert(err == nil && stat.Name() == "file" &&
		stat.Mode() == 0666, "List target file:%s, 0%o "+
		"with error: %v", stat.Name(), stat.Mode(), err)

	n, err = syscall.Getxattr(targetFile, attrData, buf)
	test.assert(err == nil && bytes.Equal(buf[:n], attrDataData),
		"Error reading the exact extended attribute: %s "+
			"with error %v", string(buf[:n]), err)

	targetLink := workspace + subdirectory + "/link"
	link, err := os.Open(targetLink)
	defer link.Close()
	test.assert(err == nil, "Error opening the link: %v", err)

	n, err = link.Read(buf)
	test.assert(err == nil && bytes.Equal(buf[:n], content),
		"Error reading the exact content: %s with error: %v",
		string(buf[:n]), err)

}

func TestWorkspaceWriteNoWritePermissionAtRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testWorkspaceWriteNoWritePermission(test, "")
	})
}

func TestWorkspaceWriteNoWritePermissionAtSubdirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testWorkspaceWriteNoWritePermission(test, "/test")
	})
}

func TestWorkspaceReadNoWritePermissionAtRoot(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testWorkspaceReadNoWritePermission(test, "")
	})
}

func TestWorkspaceReadNoWritePermissionAtSubdirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		testWorkspaceReadNoWritePermission(test, "/test")
	})
}

func TestWorkspaceDeleteAndRecreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		workspaceName := test.relPath(workspace)

		api := test.getApi()
		err := api.DeleteWorkspace(workspaceName)
		test.assert(err == nil, "Failed deleting the workspace %s",
			workspaceName)

		// Create a new workspace with the same name
		api.Branch(test.nullWorkspaceRel(), workspaceName)

		fileName := workspace + "/file"
		fd, err := syscall.Creat(fileName, 0777)
		defer syscall.Close(fd)
		test.assert(err == syscall.EPERM,
			"Error creating a small file: %v", err)
	})
}

func TestSetWorkspaceImmutable(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		workspaceName := test.relPath(workspace)

		api := test.getApi()
		err := api.SetWorkspaceImmutable(workspaceName)
		test.assert(err == nil, "Failed setting the workspace %s immutable",
			workspaceName)

		err = api.EnableRootWrite(workspaceName)
		test.assert(err == nil, "Failed enabling workspace %s write"+
			" permission", workspaceName)

		fileName := workspace + "/file"
		fd, err := syscall.Creat(fileName, 0777)
		defer syscall.Close(fd)
		test.assert(err == syscall.EPERM,
			"Error creating a small file: %v", err)

	})
}

func TestSetWorkspaceImmutableAfterDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		workspaceName := test.relPath(workspace)

		api := test.getApi()
		err := api.SetWorkspaceImmutable(workspaceName)
		test.assert(err == nil, "Failed setting the workspace %s immutable",
			workspaceName)

		fileName := workspace + "/file"
		fd, err := syscall.Creat(fileName, 0777)
		defer syscall.Close(fd)
		test.assert(err == syscall.EPERM,
			"Error creating a small file: %v", err)

		err = api.DeleteWorkspace(workspaceName)
		test.assert(err == nil, "Failed deleting the workspace %s",
			workspaceName)

		err = api.Branch(test.nullWorkspaceRel(), workspaceName)
		test.assert(err == nil, "Failed branching workspace: %v", err)

		err = api.EnableRootWrite(workspaceName)
		test.assert(err == nil,
			"Failed to enable write permission in workspace: %v", err)

		fd1, err := syscall.Creat(fileName, 0777)
		defer syscall.Close(fd1)
		test.assert(err == nil, "Error creating a small file: %v", err)
	})
}
