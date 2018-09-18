// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/qlog"
)

// This file contains tests which test walker's
// error handling aspects.

// TestWalkPanicString verifies that panic(string) generated from
// walkFunc is available as error from Walk.
func TestWalkPanicString(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedString := "raised panic"
		expectedErr := fmt.Errorf(expectedString)

		// Write File 1
		filename := workspace + "/panicFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey, size uint64,
			objType quantumfs.ObjectType, err error) error {

			if err != nil {
				c.Qctx.Elog(qlog.LogTool, walkerErrLog, path,
					key.String(), err.Error())
				test.appendWalkFuncInputErr(err)
				return err
			}

			if strings.HasSuffix(path, "/panicFile") {
				panic(expectedString)
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.AssertErr(err)
		test.Assert(strings.Contains(err.Error(), expectedErr.Error()),
			"Walk did not get the %v, instead got %v", expectedErr,
			err)
		test.assertWalkFuncInputErrs([]string{expectedString})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestWalkPanicErr verifies that panic(err) generated from
// walkFunc is available as error from Walk.
func TestWalkPanicErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedErrString := "raised panic"
		expectedErr := fmt.Errorf(expectedErrString)

		// Write File 1
		filename := workspace + "/panicFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey, size uint64,
			objType quantumfs.ObjectType, err error) error {

			if err != nil {
				c.Qctx.Elog(qlog.LogTool, walkerErrLog, path,
					key.String(), err.Error())
				test.appendWalkFuncInputErr(err)
				return err
			}

			if strings.HasSuffix(path, "/panicFile") {
				panic(expectedErr)
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.AssertErr(err)
		test.Assert(strings.Contains(err.Error(),
			expectedErr.Error()),
			"Walk did not get the expectedErr value, instead got %v",
			err)
		test.assertWalkFuncInputErrs([]string{expectedErr.Error()})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestWalkLibraryPanicErr verifies that panic in walker
// goroutine aborts the walk.
func TestWalkLibraryPanicErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create files in the workspace
		for i := 0; i < quantumfs.MaxDirectoryRecords()+1; i++ {
			filename := fmt.Sprintf("%s/file-%d", workspace, i)
			data := daemon.GenData(1)
			err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
			test.Assert(err == nil, "Write failed (%s): %s",
				filename, err)
		}

		// setup hardlinks so that more than one HLE blocks
		// are used.
		for i := 0; i < quantumfs.MaxDirectoryRecords()+1; i++ {
			link := fmt.Sprintf("%s/link-%d", workspace, i)
			fname := fmt.Sprintf("%s/file-%d", workspace, i)
			err := os.Link(fname, link)
			test.Assert(err == nil, "Link failed (%s): %s",
				link, err)
		}

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		hleGetError := fmt.Errorf("hardlinkEntry error")
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeHardlink {
				return hleGetError
			}
			return ds.Get(c, key, buf)
		}

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey, size uint64,
			objType quantumfs.ObjectType, err error) error {

			if err == hleGetError {
				panic("walker library panic")
			}
			if err != nil {
				c.Qctx.Elog(qlog.LogTool, walkerErrLog, path,
					key.String(), err.Error())
				test.appendWalkFuncInputErr(err)
				return err
			}
			return nil
		}

		err = walkWithCtx(c, dsGet, rootID, wf)
		test.AssertErr(err)
		test.Assert(strings.Contains(err.Error(), "PANIC"),
			"Walk error did not contain PANIC, got %v", err)
		test.assertWalkFuncInputErrs([]string{"PANIC"})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestWalkErr tests if the error returned from
// walkFunc is returned from Walk.
func TestWalkErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		data := daemon.GenData(133)
		workspace := test.NewWorkspace()
		expectedErr := fmt.Errorf("send error")

		// Write File 1
		filename := workspace + "/errorFile"
		err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			filename, err)

		test.SyncAllWorkspaces()
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		// Use Walker to walk all the blocks in the workspace.
		c := &test.TestCtx().Ctx
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.Assert(err == nil, "Error getting rootID for %v: %v",
			root, err)

		wf := func(c *Ctx, path string, key quantumfs.ObjectKey, size uint64,
			objType quantumfs.ObjectType, err error) error {

			if err != nil {
				c.Qctx.Elog(qlog.LogTool, walkerErrLog, path,
					key.String(), err.Error())
				test.appendWalkFuncInputErr(err)
				return err
			}

			if strings.HasSuffix(path, "/errorFile") {
				return expectedErr
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.AssertErr(err)
		test.Assert(err.Error() == expectedErr.Error(),
			"Walk did not get the %v, instead got %v", expectedErr,
			err)
		// since errors generated in walkFunc aren't reflected back into
		// walkFunc.
		test.assertWalkFuncInputErrs(nil)
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestHLGetErr tests that Walk aborts
// when data store get of hardlink fails.
func TestHLGetErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create files in the workspace
		for i := 0; i < quantumfs.MaxDirectoryRecords()+1; i++ {
			filename := fmt.Sprintf("%s/file-%d", workspace, i)
			data := daemon.GenData(1)
			err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
			test.Assert(err == nil, "Write failed (%s): %s",
				filename, err)
		}

		// setup hardlinks so that more than one HLE blocks
		// are used.
		for i := 0; i < quantumfs.MaxDirectoryRecords()+1; i++ {
			link := fmt.Sprintf("%s/link-%d", workspace, i)
			fname := fmt.Sprintf("%s/file-%d", workspace, i)
			err := os.Link(fname, link)
			test.Assert(err == nil, "Link failed (%s): %s",
				link, err)
		}

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		hleGetError := fmt.Errorf("hardlinkEntry error")
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeHardlink {
				return hleGetError
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == hleGetError.Error(),
			"Walk did not get the %v, instead got %v", hleGetError,
			err)
		test.assertWalkFuncInputErrs([]string{hleGetError.Error()})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestDEGetErr tests that Walk aborts
// when data store get of directory entry fails.
func TestDEGetErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create directories in the workspace
		dirCount := 3
		for i := 0; i < dirCount; i++ {
			dirName := fmt.Sprintf("%s/dir-%d", workspace, i)
			err := os.MkdirAll(dirName, 0666)
			test.Assert(err == nil, "MkdirAll failed (%s): %s",
				dirName, err)
		}

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		deGetError := fmt.Errorf("directoryEntry error")
		// setup dsGetHelper return error upon Get of
		// HardLinkEntry. This is the second HardLinkEntry
		// since first one is embedded inside WSR.
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeDirectory &&
				path == "/dir-1" {
				return deGetError
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == deGetError.Error(),
			"Walk did not get the %v, instead got %v", deGetError,
			err)
		test.assertWalkFuncInputErrs([]string{deGetError.Error()})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestEAGetErr tests that Walk aborts
// when data store get of xattr block fails.
func TestEAGetErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create files in the workspace
		files := 2
		for f := 0; f < files; f++ {
			filename := fmt.Sprintf("%s/file-%d", workspace, f)
			data := daemon.GenData(1)
			err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
			test.Assert(err == nil, "Write failed (%s): %s",
				filename, err)
		}

		// add extattr to the first file (workspace/file-0)
		err := syscall.Setxattr(fmt.Sprintf("%s/file-0",
			workspace), xattrName, xattrData, 0)
		test.AssertNoErr(err)

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		eaGetError := fmt.Errorf("extended attributes error")
		// setup dsGetHelper return error upon Get of
		// extattr of file-0.
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeExtendedAttribute &&
				path == "/file-0" {
				return eaGetError
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == eaGetError.Error(),
			"Walk did not get the %v, instead got %v", eaGetError,
			err)
		test.assertWalkFuncInputErrs([]string{eaGetError.Error()})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestEAAttrGetErr tests that Walk aborts
// when data store get of xattr attr block fails.
func TestEAAttrGetErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create large files in the workspace.
		// use large files to be able to distinguish these from
		// extattr data which is of type ObjectTypeSmallFile
		files := 2
		for f := 0; f < files; f++ {
			filename := fmt.Sprintf("%s/file-%d", workspace, f)
			data := daemon.GenData(1024 * 1024 * 33)
			err := ioutil.WriteFile(filename, []byte(data), os.ModePerm)
			test.Assert(err == nil, "Write failed (%s): %s",
				filename, err)
		}

		// add extattrs to the first file (workspace/file-0)
		testXattrNameFmt := "extattr-%d"
		attrs := 10
		for a := 0; a < attrs; a++ {
			err := syscall.Setxattr(fmt.Sprintf("%s/file-0", workspace),
				fmt.Sprintf(testXattrNameFmt, a),
				daemon.GenData(10), 0)
			test.AssertNoErr(err)
		}

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		eaGetError := fmt.Errorf("extended attributes error")
		// setup dsGetHelper to return error upon Get of
		// second extattr on file-0.
		countAttrGet := 0
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeExtendedAttribute &&
				path == "/file-0" {
				countAttrGet++
				if countAttrGet == 2 {
					return eaGetError
				}
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == eaGetError.Error(),
			"Walk did not get the %v, instead got %v", eaGetError,
			err)
		test.assertWalkFuncInputErrs([]string{eaGetError.Error()})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestMultiBlockGetErr tests that Walk aborts
// when data store get of multiblock fails.
func TestMultiBlockGetErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create a large file and small file in the workspace.
		largeFname := fmt.Sprintf("%s/file-0", workspace)
		data := daemon.GenData(1024 * 1024 * 33)
		err := ioutil.WriteFile(largeFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			largeFname, err)

		smallFname := fmt.Sprintf("%s/file-1", workspace)
		data = daemon.GenData(1)
		err = ioutil.WriteFile(smallFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			smallFname, err)

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		mbGetBlock0Error := fmt.Errorf("multiblock error")
		// setup dsGetHelper return error upon Get of multiblock
		// buffer on file-0. Since large file has 1 multiblock
		// metadata block, failing to get that causes other
		// data blocks in that file to be skipped.
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeLargeFile &&
				path == "/file-0" {
				return mbGetBlock0Error
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == mbGetBlock0Error.Error(),
			"Walk did not get the %v, instead got %v", mbGetBlock0Error,
			err)
		test.assertWalkFuncInputErrs([]string{mbGetBlock0Error.Error()})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestVLFileGetFirstErr tests that Walk aborts
// when get of first metdata block fails.
func TestVLFileGetFirstErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create a small file
		vlFname := fmt.Sprintf("%s/file-0", workspace)
		data := daemon.GenData(1)
		err := ioutil.WriteFile(vlFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			vlFname, err)
		// now convert the small file to a very large file

		os.Truncate(vlFname,
			int64(int(quantumfs.MaxLargeFileSize())+
				quantumfs.MaxBlockSize))

		smallFname := fmt.Sprintf("%s/file-1", workspace)
		data = daemon.GenData(1)
		err = ioutil.WriteFile(smallFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			smallFname, err)

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		vlGetBlock0Error := fmt.Errorf("verylarge block0 error")
		// setup dsGetHelper return error upon Get of block0
		// (multiblock metadata block) on file-0.
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeVeryLargeFile &&
				path == "/file-0" {
				return vlGetBlock0Error
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == vlGetBlock0Error.Error(),
			"Walk did not get the %v, instead got %v", vlGetBlock0Error,
			err)
		test.assertWalkFuncInputErrs([]string{vlGetBlock0Error.Error()})
		test.expectQlogErrs([]string{walkerErrLog})

	})
}

// TestVLFileGetNextErr tests that Walk aborts
// when get of second level multiblock metadata
// block fails.
func TestVLFileGetNextErr(t *testing.T) {
	runTest(t, func(test *testHelper) {

		workspace := test.NewWorkspace()

		// create a small file
		vlFname := fmt.Sprintf("%s/file-0", workspace)
		data := daemon.GenData(1)
		err := ioutil.WriteFile(vlFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			vlFname, err)
		// now convert the small file to a very large file

		os.Truncate(vlFname,
			int64(int(quantumfs.MaxLargeFileSize())+
				quantumfs.MaxBlockSize))

		smallFname := fmt.Sprintf("%s/file-1", workspace)
		data = daemon.GenData(1)
		err = ioutil.WriteFile(smallFname, []byte(data), os.ModePerm)
		test.Assert(err == nil, "Write failed (%s): %s",
			smallFname, err)

		test.SyncAllWorkspaces()
		c := &test.TestCtx().Ctx
		db := test.GetWorkspaceDB()
		ds := test.GetDataStore()
		root := strings.Split(test.RelPath(workspace), "/")
		rootID, _, err := db.Workspace(c, root[0], root[1], root[2])
		test.AssertNoErr(err)

		vlGetBlock1Error := fmt.Errorf("verylarge block1 error")
		// setup dsGetHelper return error upon Get of block1
		// (second level multiblock metadata block) on file-0.
		seenVLFblock := false
		dsGet := func(c *quantumfs.Ctx, path string,
			key quantumfs.ObjectKey, typ quantumfs.ObjectType,
			buf quantumfs.Buffer) error {

			if typ == quantumfs.ObjectTypeVeryLargeFile &&
				path == "/file-0" {
				seenVLFblock = true
			}

			if typ == quantumfs.ObjectTypeLargeFile &&
				path == "/file-0" &&
				seenVLFblock {
				seenVLFblock = false
				return vlGetBlock1Error
			}
			return ds.Get(c, key, buf)
		}

		err = walkWithCtx(c, dsGet, rootID, test.nopWalkFn())
		test.AssertErr(err)
		test.Assert(err.Error() == vlGetBlock1Error.Error(),
			"Walk did not get the %v, instead got %v", vlGetBlock1Error,
			err)
		test.assertWalkFuncInputErrs([]string{vlGetBlock1Error.Error()})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}
