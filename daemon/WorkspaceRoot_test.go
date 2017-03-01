// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test some special properties of workspaceroot type

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

func TestWorkspaceRootWriteNoPermission(t *testing.T) {
	runTest(t, func(test *testHelper) {
		baseWorkspace := test.newWorkspace()

		baseFileName := baseWorkspace + "/file"
		file, err := os.Create(baseFileName)
		file.Close()
		test.assert(err == nil, "Error creating a small file: %v", err)

		attrData := "user.data"
		attrDataData := []byte("sampleData")
		err = syscall.Setxattr(baseFileName, attrData, attrDataData, 0)
		test.assert(err == nil, "Error setting data XAttr: %v", err)

		wsr := test.branchWorkspaceWithoutWritePerm(baseWorkspace)
		workspace := test.absPath(wsr)

		dirName := workspace + "/dir"
		err = os.Mkdir(dirName, 0666)
		test.assert(strings.Contains(err.Error(), "operation not permitted"),
			"Error creating directories: %s", err.Error())

		fileName := workspace + "/file2"
		fd, err := syscall.Creat(fileName, 0777)
		test.assert(err == syscall.EPERM, "Error creating a small file: %v", err)
		syscall.Close(fd)

		nodeName := workspace + "/node"
		err = syscall.Mknod(nodeName, syscall.S_IFBLK|syscall.S_IRWXU, 0x12345678)
		test.assert(err == syscall.EPERM, "Error creating node: %v", err)

		targetFile := workspace + "/file"
		linkName := workspace + "/link"
		err = os.Symlink(targetFile, linkName)
		test.assert(strings.Contains(err.Error(), "operation not permitted"),
			"Error creating symlink: %v", err)

		err = os.Link(targetFile, linkName)
		test.assert(strings.Contains(err.Error(), "operation not permitted"),
			"Error creating hardlink: %v", err)

		err = os.RemoveAll(targetFile)
		test.assert(strings.Contains(err.Error(), "operation not permitted"),
			"Error unlinking file: %v", err)

		file, err = os.OpenFile(targetFile, os.O_RDWR, 0)
		defer file.Close()
		test.assert(err == nil, "Error openning the file: %v", err)

		data := genData(500)
		_, err = file.Write(data)
		test.assert(strings.Contains(err.Error(), "operation not permitted"),
			"Error writing data: %v", err)

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

		err = file.Chmod(0123)
		test.assert(strings.Contains(err.Error(), "operation not permitted"),
			"Error Set file permission attribute: %v", err)
	})
}
