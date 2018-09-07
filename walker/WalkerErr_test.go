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
			objType quantumfs.ObjectType) error {

			if strings.HasSuffix(path, "/panicFile") {
				panic(expectedString)
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.Assert(err.Error() == expectedErr.Error(),
			"Walk did not get the %v, instead got %v", expectedErr,
			err)
		test.expectWalkerErrors([]string{walkFailedLog,
			panicErrLog})
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
			objType quantumfs.ObjectType) error {

			if strings.HasSuffix(path, "/panicFile") {
				panic(expectedErr)
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.Assert(err == expectedErr,
			"Walk did not get the expectedErr value, instead got %v",
			err)
		test.expectWalkerErrors([]string{walkFailedLog,
			panicErrLog})
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
			objType quantumfs.ObjectType) error {

			if strings.HasSuffix(path, "/errorFile") {
				return expectedErr
			}
			return nil
		}
		err = Walk(c, ds, rootID, wf)
		test.Assert(err.Error() == expectedErr.Error(),
			"Walk did not get the %v, instead got %v", expectedErr,
			err)
		test.expectWalkerErrors([]string{walkFailedLog, walkerErrLog})
	})
}
