// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that workspaceroot maintains a list of accessed files
import "fmt"
import "os"
import "syscall"
import "reflect"
import "testing"

import "github.com/aristanetworks/quantumfs/utils"

func TestAccessListFileCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := make(map[string]bool)
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		accessList[filename] = true
		syscall.Close(fd)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListFileOpen(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		accessList := make(map[string]bool)
		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		path = absbworkspace + filename
		file, err := os.Open(path)
		test.Assert(err == nil, "Open file error:%v", err)
		accessList[filename] = false
		file.Close()
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path = workspace + filename
		file, err = os.Open(path)
		test.Assert(err == nil, "Open file error%v", err)
		file.Close()
		accessList[filename] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListFileDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		accessList := make(map[string]bool)
		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		path = absbworkspace + filename
		err = os.Remove(path)
		test.Assert(err == nil, "Remove file error:%v", err)
		accessList[filename] = false
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path = workspace + filename
		err = os.Remove(path)
		test.Assert(err == nil,
			"Remove file error:%v", err)
		accessList[filename] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListDirectoryCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := make(map[string]bool)
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		accessList[dirname] = true
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListDirectoryDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)

		accessList := make(map[string]bool)
		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		path = absbworkspace + dirname
		err = syscall.Rmdir(path)
		test.Assert(err == nil, "Delete directory error:%v", err)
		accessList[dirname] = false
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path = workspace + dirname
		err = syscall.Rmdir(path)
		test.Assert(err == nil, "Delete directory error:%v", err)
		accessList[dirname] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListRecursiveDirectoryCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := make(map[string]bool)
		workspace := test.NewWorkspace()
		dir1 := "/dir1"
		dir2 := "/dir2"
		path := workspace + dir1 + dir2
		err := utils.MkdirAll(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		accessList[dir1] = true
		accessList[dir1+dir2] = true
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListRecursiveDirectoryDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := "/dir1"
		dir2 := "/dir2"
		path := workspace + dir1 + dir2
		err := utils.MkdirAll(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)

		accessList := make(map[string]bool)
		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		path = absbworkspace + dir1
		err = os.RemoveAll(path)
		test.Assert(err == nil, "Delete directory error:%v", err)
		accessList[dir1] = false
		accessList[dir1+dir2] = false
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path = workspace + dir1
		err = os.RemoveAll(path)
		test.Assert(err == nil, "Delete directory error:%v", err)
		accessList[dir1] = true
		accessList[dir1+dir2] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListMvChild(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname1 := "/test1"
		dirname2 := "/test2"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		filename3 := "/test3.c"
		path := workspace + dirname1
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		path = workspace + dirname1 + filename1
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		path = workspace + dirname2
		err = syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		path = workspace + dirname2 + filename2
		fd, err = syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		accessList := make(map[string]bool)
		path1 := absbworkspace + dirname1 + filename1
		path2 := absbworkspace + dirname2 + filename3
		err = os.Rename(path1, path2)
		test.Assert(err == nil, "Move file error:%v", err)
		accessList[dirname1] = false
		accessList[dirname2] = false
		accessList[dirname1+filename1] = false
		accessList[dirname2+filename3] = true
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path1 = workspace + dirname1 + filename1
		path2 = workspace + dirname2 + filename3
		err = os.Rename(path1, path2)
		test.Assert(err == nil, "Move file error:%v", err)
		accessList[dirname1] = true
		accessList[dirname2] = true
		accessList[dirname1+filename1] = true
		accessList[dirname2+filename2] = true
		accessList[dirname2+filename3] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListRename(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		accessList := make(map[string]bool)
		path1 := absbworkspace + dirname + filename1
		path2 := absbworkspace + dirname + filename2
		err = os.Rename(path1, path2)
		test.Assert(err == nil, "Move file error:%v", err)
		accessList[dirname] = false
		accessList[dirname+filename1] = false
		accessList[dirname+filename2] = true
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path1 = workspace + dirname + filename1
		path2 = workspace + dirname + filename2
		err = os.Rename(path1, path2)
		test.Assert(err == nil, "Move file error:%v", err)
		accessList[dirname] = true
		accessList[dirname+filename1] = true
		accessList[dirname+filename2] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

	})
}

func TestAccessListHardLink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		accessList := make(map[string]bool)
		path1 := absbworkspace + dirname + filename1
		path2 := absbworkspace + dirname + filename2
		err = syscall.Link(path1, path2)
		test.Assert(err == nil, "Create hard link error:%v", err)
		accessList[dirname] = false
		accessList[filename1] = false
		accessList[dirname+filename1] = false
		accessList[dirname+filename2] = true
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path1 = workspace + dirname + filename1
		path2 = workspace + dirname + filename2
		err = syscall.Link(path1, path2)
		test.Assert(err == nil, "Create hard link error:%v", err)
		accessList[dirname] = true
		accessList[dirname+filename1] = true
		accessList[dirname+filename2] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListSymlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		accessList := make(map[string]bool)
		path1 := absbworkspace + dirname + filename1
		path2 := absbworkspace + dirname + filename2
		err = syscall.Symlink(path1, path2)
		test.Assert(err == nil, "Create symlink error:%v", err)
		accessList[dirname] = false
		accessList[dirname+filename2] = true
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path1 = workspace + dirname + filename1
		path2 = workspace + dirname + filename2
		err = syscall.Symlink(path1, path2)
		test.Assert(err == nil, "Create symlink error:%v", err)
		accessList[dirname] = true
		accessList[dirname+filename1] = true
		accessList[dirname+filename2] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessSpecialFiles(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := make(map[string]bool)
		workspace := test.NewWorkspace()

		path := workspace + "/test1"
		err := syscall.Mknod(path, syscall.S_IFBLK|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Make special file error:%v", err)
		accessList["/test1"] = true

		path = workspace + "/test2"
		err = syscall.Mknod(path, syscall.S_IFCHR|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Make special file error:%v", err)

		accessList["/test2"] = true

		path = workspace + "/test3"
		err = syscall.Mknod(path, syscall.S_IFSOCK|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Make special file error:%v", err)

		accessList["/test3"] = true

		path = workspace + "/test4"
		err = syscall.Mknod(path, syscall.S_IFREG|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Make special file error:%v", err)

		accessList["/test4"] = true

		wsr := test.getWorkspaceRoot(workspace)

		eq := reflect.DeepEqual(accessList, wsr.accessList)
		msg := fmt.Sprintf("testlist:%v wsrlist: %v",
			accessList, wsr.accessList)
		test.Assert(eq,
			"Error two maps not equal, map content:"+msg)
	})
}

func TestAccessListReadSymlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		path1 := workspace + dirname + filename1
		fd, err := syscall.Creat(path1, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)
		path2 := workspace + dirname + filename2
		err = syscall.Symlink(path1, path2)
		test.Assert(err == nil, "Create symlink error:%v", err)

		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		accessList := make(map[string]bool)
		path2 = absbworkspace + dirname + filename2
		path1, err = os.Readlink(path2)
		test.Assert(err == nil, "Read symlink error:%v", err)
		accessList[dirname] = false
		accessList[dirname+filename2] = false
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path2 = workspace + dirname + filename2
		path1, err = os.Readlink(path2)
		test.Assert(err == nil, "Read symlink error:%v", err)
		accessList[dirname] = true
		accessList[dirname+filename1] = true
		accessList[dirname+filename2] = true
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

	})
}

func TestAccessListOverwriteRemoval(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		bworkspace := test.branchWorkspace(workspace)
		absbworkspace := test.absPath(bworkspace)
		accessList := make(map[string]bool)
		path = absbworkspace + filename
		err = os.Remove(path)
		test.Assert(err == nil, "Remove file error:%v", err)
		fd, err = syscall.Creat(path, 0666)
		accessList[filename] = true
		syscall.Close(fd)
		wsrlist := test.getAccessList(absbworkspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		accessList = make(map[string]bool)
		path = workspace + filename
		err = os.Remove(path)
		test.Assert(err == nil, "Remove file error:%v", err)
		fd, err = syscall.Creat(path, 0666)
		accessList[filename] = true
		syscall.Close(fd)
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListClear(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := make(map[string]bool)
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		accessList[filename] = true
		syscall.Close(fd)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		wsr := test.getWorkspaceRoot(workspace)
		wsr.clearList()
		accessList = make(map[string]bool)
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error maps not clear")
	})
}
