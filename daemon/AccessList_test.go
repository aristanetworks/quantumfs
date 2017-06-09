// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that workspaceroot maintains a list of accessed files

import (
	"io/ioutil"
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

func TestAccessListFileCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := quantumfs.NewPathAccessList()
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		accessList.Paths[filename] = quantumfs.PathCreated
		syscall.Close(fd)
		wsrlist := test.getAccessList(workspace)
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

		accessList := quantumfs.NewPathAccessList()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + filename
		err = os.Remove(path)
		test.Assert(err == nil, "Remove file error:%v", err)
		accessList.Paths[filename] = quantumfs.PathDeleted
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListFileCreateDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		// Files which are created and then deleted are removed from the list
		accessList := quantumfs.NewPathAccessList()

		path = workspace + filename
		err = os.Remove(path)
		test.Assert(err == nil,
			"Remove file error:%v", err)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListFileTruncate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		accessList := quantumfs.NewPathAccessList()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + filename
		test.AssertNoErr(os.Truncate(path, 0))
		accessList.Paths[filename] = quantumfs.PathUpdated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListFileFtruncate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		accessList := quantumfs.NewPathAccessList()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + filename
		fd, err = syscall.Open(path, syscall.O_RDWR, 0000)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Ftruncate(fd, 0))
		syscall.Close(fd)
		accessList.Paths[filename] = quantumfs.PathUpdated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListFileReadWrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		err := testutils.PrintToFile(path, string(GenData(10*1024)))
		test.AssertNoErr(err)

		_, err = ioutil.ReadFile(path)
		test.AssertNoErr(err)

		accessList := quantumfs.NewPathAccessList()
		accessList.Paths[filename] = quantumfs.PathUpdated |
			quantumfs.PathRead | quantumfs.PathCreated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListDirectoryCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := quantumfs.NewPathAccessList()
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		accessList.Paths[dirname] = quantumfs.PathIsDir |
			quantumfs.PathCreated
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

		accessList := quantumfs.NewPathAccessList()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + dirname
		err = syscall.Rmdir(path)
		test.Assert(err == nil, "Delete directory error:%v", err)
		accessList.Paths[dirname] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListDirectoryCreateDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)

		// Directories which are created and then deleted are removed from
		// the list.
		accessList := quantumfs.NewPathAccessList()

		path = workspace + dirname
		err = syscall.Rmdir(path)
		test.Assert(err == nil, "Delete directory error:%v", err)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListDirectoryRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		err := syscall.Mkdir(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)

		accessList := quantumfs.NewPathAccessList()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + dirname
		_, err = ioutil.ReadDir(path)
		test.AssertNoErr(err)
		accessList.Paths[dirname] = quantumfs.PathIsDir | quantumfs.PathRead
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListRecursiveDirectoryCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := quantumfs.NewPathAccessList()
		workspace := test.NewWorkspace()
		dir1 := "/dir1"
		dir2 := "/dir2"
		path := workspace + dir1 + dir2
		err := utils.MkdirAll(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)
		accessList.Paths[dir1] = quantumfs.PathIsDir | quantumfs.PathCreated
		accessList.Paths[dir1+dir2] = quantumfs.PathIsDir |
			quantumfs.PathCreated
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

		accessList := quantumfs.NewPathAccessList()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + dir1
		test.AssertNoErr(os.Remove(workspace + dir1 + dir2))
		test.AssertNoErr(os.Remove(workspace + dir1))
		accessList.Paths[dir1] = quantumfs.PathIsDir | quantumfs.PathDeleted
		accessList.Paths[dir1+dir2] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListRecursiveDirectoryCreateDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := "/dir1"
		dir2 := "/dir2"
		path := workspace + dir1 + dir2
		err := utils.MkdirAll(path, 0666)
		test.Assert(err == nil, "Create directory error:%v", err)

		// Directories created and then deleted should be removed from the
		// list.
		accessList := quantumfs.NewPathAccessList()

		path = workspace + dir1
		err = os.RemoveAll(path)
		test.Assert(err == nil, "Delete directory error:%v", err)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListMvChildFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname1 := "/test1"
		dirname2 := "/test2"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
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

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path1 := workspace + dirname1 + filename1
		path2 := workspace + dirname2 + filename2
		err = os.Rename(path1, path2)
		test.Assert(err == nil, "Move file error:%v", err)
		accessList.Paths[dirname1+filename1] = quantumfs.PathDeleted
		accessList.Paths[dirname2+filename2] = quantumfs.PathCreated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListRenameFile(t *testing.T) {
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

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		err = os.Rename(path1, path2)
		test.Assert(err == nil, "Move file error:%v", err)
		accessList.Paths[dirname+filename1] = quantumfs.PathDeleted
		accessList.Paths[dirname+filename2] = quantumfs.PathCreated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListMvChildFileOverwrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname1 := "/test1"
		dirname2 := "/test2"
		filename1 := "/test1.c"
		filename2 := "/test2.c"

		path := workspace + dirname1
		test.AssertNoErr(syscall.Mkdir(path, 0666))
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

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path1 := workspace + dirname1 + filename1
		path2 := workspace + dirname2 + filename2
		err = os.Rename(path1, path2)
		test.Assert(err == nil, "Move file error:%v", err)
		accessList.Paths[dirname1+filename1] = quantumfs.PathDeleted
		accessList.Paths[dirname2+filename2] = quantumfs.PathUpdated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListRenameFileOverwrite(t *testing.T) {
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
		path = workspace + dirname + filename2
		fd, err = syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		test.AssertNoErr(os.Rename(path1, path2))
		accessList.Paths[dirname+filename1] = quantumfs.PathDeleted
		accessList.Paths[dirname+filename2] = quantumfs.PathUpdated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListMvChildDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname1 := "/test1"
		dirname2 := "/test2"
		leaf1 := "/leaf1"
		leaf2 := "/leaf2"
		path := workspace + dirname1
		err := syscall.Mkdir(path, 0777)
		test.Assert(err == nil, "Create directory error:%v", err)
		path = workspace + dirname1 + leaf1
		test.AssertNoErr(syscall.Mkdir(path, 0777))

		path = workspace + dirname2
		test.AssertNoErr(syscall.Mkdir(path, 0666))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path1 := workspace + dirname1 + leaf1
		path2 := workspace + dirname2 + leaf2
		test.AssertNoErr(os.Rename(path1, path2))
		accessList.Paths[dirname1+leaf1] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		accessList.Paths[dirname2+leaf2] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListRenameDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		leaf1 := "/leaf1"
		leaf2 := "/leaf2"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0777))
		path = workspace + dirname + leaf1
		test.AssertNoErr(syscall.Mkdir(path, 0777))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path1 := workspace + dirname + leaf1
		path2 := workspace + dirname + leaf2
		test.AssertNoErr(os.Rename(path1, path2))
		accessList.Paths[dirname+leaf1] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		accessList.Paths[dirname+leaf2] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListHardLinkCreate(t *testing.T) {
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

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		err = syscall.Link(path1, path2)
		test.Assert(err == nil, "Create hard link error:%v", err)
		accessList.Paths[dirname+filename2] = quantumfs.PathCreated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListHardLinkDelete(t *testing.T) {
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
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		err = syscall.Link(path1, path2)
		test.Assert(err == nil, "Create hard link error:%v", err)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()

		path1 = workspace + dirname + filename1
		path2 = workspace + dirname + filename2
		test.AssertNoErr(os.Remove(path1))
		test.AssertNoErr(os.Remove(path2))
		accessList.Paths[dirname+filename1] = quantumfs.PathDeleted
		accessList.Paths[dirname+filename2] = quantumfs.PathDeleted
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListHardLinkCreateDelete(t *testing.T) {
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

		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		err = syscall.Link(path1, path2)
		test.Assert(err == nil, "Create hard link error:%v", err)
		test.AssertNoErr(os.Remove(path1))
		test.AssertNoErr(os.Remove(path2))

		// Files created and then deleted should be removed from the list.
		accessList := quantumfs.NewPathAccessList()
		accessList.Paths[dirname] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		wsrlist := test.getAccessList(workspace)
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

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		err = syscall.Symlink(path1, path2)
		test.Assert(err == nil, "Create symlink error:%v", err)
		_, err = os.Readlink(path2)
		test.AssertNoErr(err)
		accessList.Paths[dirname+filename2] = quantumfs.PathCreated |
			quantumfs.PathRead
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessSpecialFiles(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := quantumfs.NewPathAccessList()
		workspace := test.NewWorkspace()

		path := workspace + "/test1"
		err := syscall.Mknod(path, syscall.S_IFBLK|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Make special file error:%v", err)
		accessList.Paths["/test1"] = quantumfs.PathCreated

		path = workspace + "/test2"
		err = syscall.Mknod(path, syscall.S_IFCHR|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Make special file error:%v", err)

		accessList.Paths["/test2"] = quantumfs.PathCreated

		path = workspace + "/test3"
		err = syscall.Mknod(path, syscall.S_IFSOCK|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Make special file error:%v", err)

		accessList.Paths["/test3"] = quantumfs.PathCreated

		path = workspace + "/test4"
		err = syscall.Mknod(path, syscall.S_IFREG|syscall.S_IRWXU,
			0x12345678)
		test.Assert(err == nil, "Make special file error:%v", err)

		accessList.Paths["/test4"] = quantumfs.PathCreated

		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps are different")
	})
}

func TestAccessListOverwriteRemovalFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path = workspace + filename
		err = os.Remove(path)
		test.Assert(err == nil, "Remove file error:%v", err)
		fd, err = syscall.Creat(path, 0666)
		// Deleted and then created files are counted as having been
		// truncated.
		accessList.Paths[filename] = quantumfs.PathUpdated
		syscall.Close(fd)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListOverwriteRemovalDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		test.AssertNoErr(os.Mkdir(path, 0777))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path = workspace + dirname
		test.AssertNoErr(os.Remove(path))
		test.AssertNoErr(os.Mkdir(path, 0777))
		// Deleted and then created directories are counted as having been
		// neither deleted nor created.
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListOverwriteRemovalDirectoryWithRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		test.AssertNoErr(os.Mkdir(path, 0777))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		accessList := quantumfs.NewPathAccessList()
		path = workspace + dirname
		_, err := ioutil.ReadDir(path)
		test.AssertNoErr(err)
		test.AssertNoErr(os.Remove(path))
		test.AssertNoErr(os.Mkdir(path, 0777))
		// Deleted and then created directories are counted as having been
		// neither deleted nor created, however, if it had been read, the
		// Read flag must persist.
		accessList.Paths[dirname] = quantumfs.PathIsDir | quantumfs.PathRead
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListInsertInode(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		syscall.Close(fd)

		key := getExtendedKeyHelper(test, path, "file")

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		filename = filename + "clone"
		path = workspace + filename

		api := test.getApi()
		err = api.InsertInode(test.RelPath(path), key, 0777, 0, 0)
		test.AssertNoErr(err)

		accessList := quantumfs.NewPathAccessList()
		accessList.Paths[filename] = quantumfs.PathCreated
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")
	})
}

func TestAccessListClear(t *testing.T) {
	runTest(t, func(test *testHelper) {
		accessList := quantumfs.NewPathAccessList()
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.Assert(err == nil, "Create file error:%v", err)
		accessList.Paths[filename] = quantumfs.PathCreated
		syscall.Close(fd)
		wsrlist := test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error two maps different")

		api := test.getApi()
		test.AssertNoErr(api.ClearAccessed(test.RelPath(workspace)))
		accessList = quantumfs.NewPathAccessList()
		wsrlist = test.getAccessList(workspace)
		test.assertAccessList(accessList, wsrlist,
			"Error maps not clear")
	})
}
