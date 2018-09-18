// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/qlog"
)

// This file contains tests which verify walker's
// fail-fast behaviour.

// TestFailFastWalkPanicString verifies that walk aborts when
// panic(string) is generated from walkFunc.
func TestFailFastWalkPanicString(t *testing.T) {
	runTest(t, doPanicStringTest(false))
}

// TestFailFastWalkPanicErr verifies that walk aborts when
// panic(err) is generated from walkFunc .
func TestFailFastWalkPanicErr(t *testing.T) {
	runTest(t, doPanicErrTest(false))
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
				test.appendWalkFuncInErr(err)
				return err
			}
			return nil
		}

		err = walkWithCtx(c, dsGet, rootID, wf)
		test.AssertErr(err)
		test.Assert(strings.Contains(err.Error(), "PANIC"),
			"Walk error did not contain PANIC, got %v", err)
		test.assertWalkFuncInErrs([]string{"PANIC"})
		test.expectQlogErrs([]string{walkerErrLog})
	})
}

// TestFailFastWalkErr tests that Walk aborts when
// walkFunc returns an error.
func TestFailFastWalkErr(t *testing.T) {
	runTest(t, doWalkErrTest(false))
}

// TestFailFastHLGetErr tests that Walk aborts
// when data store get of hardlink fails.
func TestFailFastHLGetErr(t *testing.T) {
	runTest(t, doHLGetErrTest(false))
}

// TestFailFastDEGetErr tests that Walk aborts
// when data store get of directory entry fails.
func TestFailFastDEGetErr(t *testing.T) {
	runTest(t, doDEGetErrTest(false))
}

// TestFailFastEAGetErr tests that Walk aborts
// when data store get of xattr block fails.
func TestFailFastEAGetErr(t *testing.T) {
	runTest(t, doEAGetErrTest(false))
}

// TestFailFastEAAttrGetErr tests that Walk aborts
// when data store get of xattr attr block fails.
func TestFailFastEAAttrGetErr(t *testing.T) {
	runTest(t, doEAAttrGetErrTest(false))
}

// TestFailFastMultiBlockGetErr tests that Walk aborts
// when data store get of multiblock fails.
func TestFailFastMultiBlockGetErr(t *testing.T) {
	runTest(t, doMultiBlockGetErrTest(false))
}

// TestFailFastVLFileGetFirstErr tests that Walk aborts
// when get of first metdata block fails.
func TestFailFastVLFileGetFirstErr(t *testing.T) {
	runTest(t, doVLFileGetFirstErrTest(false))
}

// TestFailFastVLFileGetNextErr tests that Walk aborts
// when get of second level multiblock metadata
// block fails.
func TestFailFastVLFileGetNextErr(t *testing.T) {
	runTest(t, doVLFileGetNextErrTest(false))
}
