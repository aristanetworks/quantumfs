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
		test.Assert(err == nil,
			"List api file error%v,%v", err, stat)
		stat_t := stat.Sys().(*syscall.Stat_t)
		test.Assert(stat_t.Ino == quantumfs.InodeIdApi,
			"Wrong Inode number for api file")

		src := quantumfs.NullSpaceName + "/" + quantumfs.NullSpaceName +
			"/" + quantumfs.NullSpaceName
		dst := "wsrtest/wsrtest/wsrtest"

		api, err := quantumfs.NewApiWithPath(apiPath)

		test.Assert(err == nil, "Error retrieving Api: %v", err)
		test.Assert(api != nil, "Api nil")

		err = api.Branch(src, dst)
		test.Assert(err == nil,
			"Error branching with api in nullworkspace:%v", err)
		api.Close()
	})
}

func testPreparation(test *testHelper, subdirectory string) (string, string,
	[]byte, []byte) {

	baseWorkspace := test.NewWorkspace()

	baseDirName := baseWorkspace + subdirectory + "/dir"
	err := os.MkdirAll(baseDirName, 0666)
	test.Assert(err == nil, "Error creating a directory: %v", err)

	content := []byte("This is a test")
	baseFileName := baseWorkspace + subdirectory + "/file"
	file, err := os.Create(baseFileName)
	test.Assert(err == nil, "Error creating a small file: %v", err)

	n, err := file.Write(content)
	defer file.Close()
	test.Assert(err == nil && n == len(content),
		"Error writing to the small file: %v", err)

	attrData := "user.data"
	attrDataData := []byte("sampleData")
	err = syscall.Setxattr(baseFileName, attrData, attrDataData, 0)
	test.Assert(err == nil, "Error setting data XAttr: %v", err)

	return baseWorkspace, attrData, attrDataData, content
}

func testWorkspaceWriteNoWritePermission(test *testHelper, subdirectory string) {
	baseWorkspace, attrData, attrDataData, _ :=
		testPreparation(test, subdirectory)

	wsr := test.branchWorkspaceWithoutWritePerm(baseWorkspace)
	workspace := test.absPath(wsr)

	dirName := workspace + subdirectory + "/dir1"
	err := os.Mkdir(dirName, 0666)
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error creating directories: %s", err.Error())

	fileName := workspace + subdirectory + "/file2"
	fd, err := syscall.Creat(fileName, 0777)
	syscall.Close(fd)
	test.Assert(err == syscall.EPERM,
		"Error creating a small file: %v", err)

	fileName = workspace + subdirectory + "/dir" + "/file"
	fd, err = syscall.Creat(fileName, 0777)
	syscall.Close(fd)
	test.Assert(err == syscall.EPERM,
		"Error creating a small file: %v", err)

	nodeName := workspace + subdirectory + "/node"
	err = syscall.Mknod(nodeName,
		syscall.S_IFBLK|syscall.S_IRWXU, 0x12345678)
	test.Assert(err == syscall.EPERM, "Error creating node: %v", err)

	targetFile := workspace + subdirectory + "/file"
	linkName := workspace + subdirectory + "/link"
	err = os.Symlink(targetFile, linkName)
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error creating symlink: %v", err)

	err = os.Link(targetFile, linkName)
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error creating hardlink: %v", err)

	err = os.RemoveAll(targetFile)
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error unlinking file: %v", err)

	targetDir := workspace + subdirectory + "/dir"
	err = os.RemoveAll(targetDir)
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error unlinking directory: %v", err)

	file, err := os.OpenFile(targetFile, os.O_RDWR, 0)
	file.Close()
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error opening the file: %v", err)

	err = os.Rename(targetFile, workspace+"/newFile")
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error renaming file: %v", err)

	attrDataData = []byte("extendedattributedata")
	err = syscall.Setxattr(targetFile, attrData, attrDataData, 0)
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error setting data XAttr: %v", err)

	err = syscall.Removexattr(targetFile, attrData)
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error deleting data XAttr: %v", err)

	err = syscall.Chmod(targetFile, 0123)
	test.Assert(strings.Contains(err.Error(), "operation not permitted"),
		"Error Set file permission attribute: %v", err)
}

func testWorkspaceReadNoWritePermission(test *testHelper, subdirectory string) {
	baseWorkspace, attrData, attrDataData, content :=
		testPreparation(test, subdirectory)

	baseLinkName := baseWorkspace + subdirectory + "/link"
	err := syscall.Link(baseWorkspace+subdirectory+"/file", baseLinkName)
	test.Assert(err == nil, "Error creating hardlink: %v", err)

	baseFileName := baseWorkspace + subdirectory + "/dir/file1"
	file, err := os.Create(baseFileName)
	file.Close()
	test.Assert(err == nil, "Error creating a small file: %v", err)

	wsr := test.branchWorkspaceWithoutWritePerm(baseWorkspace)
	workspace := test.absPath(wsr)

	targetFile := workspace + subdirectory + "/file"
	file, err = os.OpenFile(targetFile, os.O_RDONLY, 0)
	defer file.Close()
	test.Assert(err == nil, "Error opening the file: %v", err)

	buf := make([]byte, 256)
	n, err := file.Read(buf)
	test.Assert(err == nil && bytes.Equal(buf[:n], content),
		"Error reading the exact content: %s with error: %v",
		string(buf[:n]), err)

	targetDir := workspace + subdirectory + "/dir"
	dir, err := os.Open(targetDir)
	defer dir.Close()
	test.Assert(err == nil, "Error opening the directory: %v", err)

	names, err := dir.Readdirnames(0)
	test.Assert(err == nil && len(names) == 1 && names[0] == "file1",
		"Error reading directory: %v with error: %v", names, err)

	stat, err := os.Lstat(targetFile)
	test.Assert(err == nil && stat.Name() == "file" &&
		stat.Mode() == 0666, "List target file:%s, 0%o "+
		"with error: %v", stat.Name(), stat.Mode(), err)

	n, err = syscall.Getxattr(targetFile, attrData, buf)
	test.Assert(err == nil && bytes.Equal(buf[:n], attrDataData),
		"Error reading the exact extended attribute: %s "+
			"with error %v", string(buf[:n]), err)

	targetLink := workspace + subdirectory + "/link"
	link, err := os.Open(targetLink)
	defer link.Close()
	test.Assert(err == nil, "Error opening the link: %v", err)

	n, err = link.Read(buf)
	test.Assert(err == nil && bytes.Equal(buf[:n], content),
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
		workspace := test.NewWorkspace()
		workspaceName := test.RelPath(workspace)

		api := test.getApi()
		err := api.DeleteWorkspace(workspaceName)
		test.Assert(err == nil, "Failed deleting the workspace %s",
			workspaceName)

		// Create a new workspace with the same name
		api.Branch(test.nullWorkspaceRel(), workspaceName)

		fileName := workspace + "/file"
		fd, err := syscall.Creat(fileName, 0777)
		defer syscall.Close(fd)
		test.Assert(err == syscall.EPERM,
			"Error creating a small file: %v", err)
	})
}

// We need isWorkspaceRoot() to be more robust than it has been
func TestWorkspaceRootChecker(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		wsr := test.getWorkspaceRoot(workspace)
		var inode Inode
		inode = wsr

		test.Assert(inode.isWorkspaceRoot() == true,
			"inode interface not routing")
		test.Assert(wsr.isWorkspaceRoot() == true, "wsr not recognized")
		test.Assert(wsr.Directory.isWorkspaceRoot() == true,
			"wsr dir not routing")
	})
}
