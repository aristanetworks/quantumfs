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

func TestWorkspaceWriteNoRootWritePermission(t *testing.T) {
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

		baseDirName := baseWorkspace + "/dir"
		err = os.Mkdir(baseDirName, 0666)
		test.assert(err == nil, "Error creating a directory: %v", err)

		wsr := test.branchWorkspaceWithoutWritePerm(baseWorkspace)
		workspace := test.absPath(wsr)

		dirName := workspace + "/dir1"
		err = os.Mkdir(dirName, 0666)
		test.assert(strings.Contains(err.Error(), "operation not permitted"),
			"Error creating directories: %s", err.Error())

		fileName := workspace + "/file2"
		fd, err := syscall.Creat(fileName, 0777)
		syscall.Close(fd)
		test.assert(err == syscall.EPERM,
			"Error creating a small file: %v", err)

		fileName = workspace + "/dir" + "/file"
		fd, err = syscall.Creat(fileName, 0777)
		syscall.Close(fd)
		test.assert(err == syscall.EPERM,
			"Error creating a small file: %v", err)

		nodeName := workspace + "/node"
		err = syscall.Mknod(nodeName,
			syscall.S_IFBLK|syscall.S_IRWXU, 0x12345678)
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

		targetDir := workspace + "/dir"
		err = os.RemoveAll(targetDir)
		test.assert(strings.Contains(err.Error(), "operation not permitted"),
			"Error unlinking directory: %v", err)

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

func TestWorkspaceReadNoRootWritePermission(t *testing.T) {
	runTest(t, func(test *testHelper) {
		baseWorkspace := test.newWorkspace()

		content := []byte("This is a test")
		baseFileName := baseWorkspace + "/file"
		file, err := os.Create(baseFileName)
		test.assert(err == nil, "Error creating a small file: %v", err)

		n, err := file.Write(content)
		test.assert(err == nil && n == len(content),
			"Error writing to the small file: %v", err)
		file.Close()

		attrData := "user.data"
		attrDataData := []byte("sampleData")
		err = syscall.Setxattr(baseFileName, attrData, attrDataData, 0)
		test.assert(err == nil, "Error setting data XAttr: %v", err)

		baseDirName := baseWorkspace + "/dir"
		err = os.Mkdir(baseDirName, 0666)
		test.assert(err == nil, "Error creating a directory: %v", err)

		baseFileName = baseDirName + "/file1"
		file, err = os.Create(baseFileName)
		file.Close()
		test.assert(err == nil, "Error creating a small file: %v", err)

		wsr := test.branchWorkspaceWithoutWritePerm(baseWorkspace)
		workspace := test.absPath(wsr)

		targetFile := workspace + "/file"
		file, err = os.OpenFile(targetFile, os.O_RDWR, 0)
		defer file.Close()
		test.assert(err == nil, "Error openning the file: %v", err)

		buf := make([]byte, 256)
		n, err = file.Read(buf)
		test.assert(err == nil && bytes.Equal(buf[:n], content),
			"Error reading the exact content: %s with error: %v",
			string(buf[:n]), err)

		targetDir := workspace + "/dir"
		dir, err := os.Open(targetDir)
		defer dir.Close()
		test.assert(err == nil, "Error openning the file: %v", err)

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

	})

}
