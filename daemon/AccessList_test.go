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
		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		expectedAccessList.Paths[filename] = quantumfs.PathCreated
		syscall.Close(fd)
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListFileDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + filename
		test.AssertNoErr(os.Remove(path))
		expectedAccessList.Paths[filename] = quantumfs.PathDeleted
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListFileCreateDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		// Files which are created and then deleted are removed from the list
		expectedAccessList := quantumfs.NewPathsAccessed()

		path = workspace + filename
		test.AssertNoErr(os.Remove(path))
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListFileTruncate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + filename
		test.AssertNoErr(os.Truncate(path, 0))
		expectedAccessList.Paths[filename] = quantumfs.PathUpdated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListFileFtruncate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + filename
		fd, err = syscall.Open(path, syscall.O_RDWR, 0000)
		test.AssertNoErr(err)
		test.AssertNoErr(syscall.Ftruncate(fd, 0))
		syscall.Close(fd)
		expectedAccessList.Paths[filename] = quantumfs.PathUpdated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
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

		expectedAccessList := quantumfs.NewPathsAccessed()
		expectedAccessList.Paths[filename] = quantumfs.PathUpdated |
			quantumfs.PathRead | quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListDirectoryCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		expectedAccessList.Paths[dirname] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListDirectoryDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))

		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + dirname
		test.AssertNoErr(syscall.Rmdir(path))
		expectedAccessList.Paths[dirname] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListDirectoryCreateDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))

		// Directories which are created and then deleted are removed from
		// the list.
		expectedAccessList := quantumfs.NewPathsAccessed()

		path = workspace + dirname
		test.AssertNoErr(syscall.Rmdir(path))
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListDirectoryRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))

		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + dirname
		_, err := ioutil.ReadDir(path)
		test.AssertNoErr(err)
		expectedAccessList.Paths[dirname] = quantumfs.PathIsDir |
			quantumfs.PathRead
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListRecursiveDirectoryCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace := test.NewWorkspace()
		dir1 := "/dir1"
		dir2 := "/dir2"
		path := workspace + dir1 + dir2
		test.AssertNoErr(utils.MkdirAll(path, 0666))
		expectedAccessList.Paths[dir1] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		expectedAccessList.Paths[dir1+dir2] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListRecursiveDirectoryDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := "/dir1"
		dir2 := "/dir2"
		path := workspace + dir1 + dir2
		test.AssertNoErr(utils.MkdirAll(path, 0666))

		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace = test.AbsPath(test.branchWorkspace(workspace))
		path = workspace + dir1
		test.AssertNoErr(os.Remove(workspace + dir1 + dir2))
		test.AssertNoErr(os.Remove(workspace + dir1))
		expectedAccessList.Paths[dir1] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		expectedAccessList.Paths[dir1+dir2] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListRecursiveDirectoryCreateDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dir1 := "/dir1"
		dir2 := "/dir2"
		path := workspace + dir1 + dir2
		test.AssertNoErr(utils.MkdirAll(path, 0666))

		// Directories created and then deleted should be removed from the
		// list.
		expectedAccessList := quantumfs.NewPathsAccessed()

		path = workspace + dir1
		test.AssertNoErr(os.RemoveAll(path))
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
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
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname1 + filename1
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		path = workspace + dirname2
		test.AssertNoErr(syscall.Mkdir(path, 0666))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname1 + filename1
		path2 := workspace + dirname2 + filename2
		test.AssertNoErr(os.Rename(path1, path2))
		expectedAccessList.Paths[dirname1+filename1] = quantumfs.PathDeleted
		expectedAccessList.Paths[dirname2+filename2] = quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListRenameFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		test.AssertNoErr(os.Rename(path1, path2))
		expectedAccessList.Paths[dirname+filename1] = quantumfs.PathDeleted
		expectedAccessList.Paths[dirname+filename2] = quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListRenameFileWithWrites(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		test.AssertNoErr(testutils.OverWriteFile(path1, "data1"))
		test.AssertNoErr(os.Rename(path1, path2))
		test.AssertNoErr(testutils.OverWriteFile(path2, "data2"))
		expectedAccessList.Paths[dirname+filename1] = quantumfs.PathUpdated |
			quantumfs.PathDeleted
		expectedAccessList.Paths[dirname+filename2] = quantumfs.PathCreated |
			quantumfs.PathUpdated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
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
		test.AssertNoErr(err)
		syscall.Close(fd)

		path = workspace + dirname2
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname2 + filename2
		fd, err = syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname1 + filename1
		path2 := workspace + dirname2 + filename2
		test.AssertNoErr(os.Rename(path1, path2))
		expectedAccessList.Paths[dirname1+filename1] = quantumfs.PathDeleted
		expectedAccessList.Paths[dirname2+filename2] = quantumfs.PathUpdated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListRenameFileOverwrite(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)
		path = workspace + dirname + filename2
		fd, err = syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		test.AssertNoErr(os.Rename(path1, path2))
		expectedAccessList.Paths[dirname+filename1] = quantumfs.PathDeleted
		expectedAccessList.Paths[dirname+filename2] = quantumfs.PathUpdated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
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
		test.AssertNoErr(syscall.Mkdir(path, 0777))
		path = workspace + dirname1 + leaf1
		test.AssertNoErr(syscall.Mkdir(path, 0777))

		path = workspace + dirname2
		test.AssertNoErr(syscall.Mkdir(path, 0666))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname1 + leaf1
		path2 := workspace + dirname2 + leaf2
		test.AssertNoErr(os.Rename(path1, path2))
		expectedAccessList.Paths[dirname1+leaf1] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		expectedAccessList.Paths[dirname2+leaf2] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
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
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname + leaf1
		path2 := workspace + dirname + leaf2
		test.AssertNoErr(os.Rename(path1, path2))
		expectedAccessList.Paths[dirname+leaf1] = quantumfs.PathIsDir |
			quantumfs.PathDeleted
		expectedAccessList.Paths[dirname+leaf2] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListHardLinkCreate(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		test.AssertNoErr(syscall.Link(path1, path2))
		expectedAccessList.Paths[dirname+filename2] = quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListHardLinkDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		test.AssertNoErr(syscall.Link(path1, path2))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()

		path1 = workspace + dirname + filename1
		path2 = workspace + dirname + filename2
		test.AssertNoErr(os.Remove(path1))
		test.AssertNoErr(os.Remove(path2))
		expectedAccessList.Paths[dirname+filename1] = quantumfs.PathDeleted
		expectedAccessList.Paths[dirname+filename2] = quantumfs.PathDeleted
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListHardLinkCreateDelete(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		test.AssertNoErr(syscall.Link(path1, path2))
		test.AssertNoErr(os.Remove(path1))
		test.AssertNoErr(os.Remove(path2))

		// Files created and then deleted should be removed from the list.
		expectedAccessList := quantumfs.NewPathsAccessed()
		expectedAccessList.Paths[dirname] = quantumfs.PathIsDir |
			quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListSymlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		filename1 := "/test1.c"
		filename2 := "/test2.c"
		path := workspace + dirname
		test.AssertNoErr(syscall.Mkdir(path, 0666))
		path = workspace + dirname + filename1
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path1 := workspace + dirname + filename1
		path2 := workspace + dirname + filename2
		test.AssertNoErr(syscall.Symlink(path1, path2))
		_, err = os.Readlink(path2)
		test.AssertNoErr(err)
		expectedAccessList.Paths[dirname+filename2] = quantumfs.PathCreated |
			quantumfs.PathRead
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessSpecialFiles(t *testing.T) {
	runTest(t, func(test *testHelper) {
		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace := test.NewWorkspace()

		path := workspace + "/test1"
		test.AssertNoErr(syscall.Mknod(path, syscall.S_IFBLK|syscall.S_IRWXU,
			0x12345678))
		expectedAccessList.Paths["/test1"] = quantumfs.PathCreated

		path = workspace + "/test2"
		test.AssertNoErr(syscall.Mknod(path, syscall.S_IFCHR|syscall.S_IRWXU,
			0x12345678))

		expectedAccessList.Paths["/test2"] = quantumfs.PathCreated

		path = workspace + "/test3"
		test.AssertNoErr(syscall.Mknod(path,
			syscall.S_IFSOCK|syscall.S_IRWXU, 0x12345678))

		expectedAccessList.Paths["/test3"] = quantumfs.PathCreated

		path = workspace + "/test4"
		test.AssertNoErr(syscall.Mknod(path, syscall.S_IFREG|syscall.S_IRWXU,
			0x12345678))

		expectedAccessList.Paths["/test4"] = quantumfs.PathCreated

		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListOverwriteRemovalFile(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path = workspace + filename
		test.AssertNoErr(os.Remove(path))
		fd, err = syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		// Deleted and then created files are counted as having been
		// truncated.
		expectedAccessList.Paths[filename] = quantumfs.PathUpdated
		syscall.Close(fd)
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListOverwriteRemovalDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		test.AssertNoErr(os.Mkdir(path, 0777))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path = workspace + dirname
		test.AssertNoErr(os.Remove(path))
		test.AssertNoErr(os.Mkdir(path, 0777))
		// Deleted and then created directories are counted as having been
		// neither deleted nor created.
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListOverwriteRemovalDirectoryWithRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		dirname := "/test"
		path := workspace + dirname
		test.AssertNoErr(os.Mkdir(path, 0777))

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		expectedAccessList := quantumfs.NewPathsAccessed()
		path = workspace + dirname
		_, err := ioutil.ReadDir(path)
		test.AssertNoErr(err)
		test.AssertNoErr(os.Remove(path))
		test.AssertNoErr(os.Mkdir(path, 0777))
		// Deleted and then created directories are counted as having been
		// neither deleted nor created, however, if it had been read, the
		// Read flag must persist.
		expectedAccessList.Paths[dirname] = quantumfs.PathIsDir |
			quantumfs.PathRead
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListInsertInode(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		syscall.Close(fd)

		key := getExtendedKeyHelper(test, path, "file")

		workspace = test.AbsPath(test.branchWorkspace(workspace))
		filename = filename + "clone"
		path = workspace + filename

		_, cleanup := test.GetWorkspaceRoot(workspace)
		// This will prevent the workspace from getting uninstantiated
		// which will lose the accesslist
		defer cleanup()

		api := test.getApi()
		err = api.InsertInode(test.RelPath(path), key, 0777, 0, 0)
		test.AssertNoErr(err)

		expectedAccessList := quantumfs.NewPathsAccessed()
		expectedAccessList.Paths[filename] = quantumfs.PathCreated
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}

func TestAccessListClear(t *testing.T) {
	runTest(t, func(test *testHelper) {
		expectedAccessList := quantumfs.NewPathsAccessed()
		workspace := test.NewWorkspace()
		filename := "/test"
		path := workspace + filename
		fd, err := syscall.Creat(path, 0666)
		test.AssertNoErr(err)
		expectedAccessList.Paths[filename] = quantumfs.PathCreated
		syscall.Close(fd)
		test.assertWorkspaceAccessList(expectedAccessList, workspace)

		api := test.getApi()
		test.AssertNoErr(api.ClearAccessed(test.RelPath(workspace)))
		expectedAccessList = quantumfs.NewPathsAccessed()
		test.assertWorkspaceAccessList(expectedAccessList, workspace)
	})
}
