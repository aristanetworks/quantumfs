// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

func (th *testHelper) checkUploadMatches(checkPath string, workspace string,
	compare func(quantumfs.DirectoryRecord, quantumfs.DirectoryRecord)) {

	fd, err := os.Open(checkPath)
	th.AssertNoErr(err)
	fd.Sync()
	fd.Close()

	c := Ctx{
		Qctx: &(th.TestCtx().Ctx),
	}
	_, c.eCtx = errgroup.WithContext(context.Background())

	fromWalker := make(chan *pathInfo, 100)

	err = filepath.Walk(checkPath,
		func(path string, info os.FileInfo, err error) error {
			return pathWalker(&c, fromWalker, path, checkPath, info, err)
		})
	th.AssertNoErr(err)

	checkedRoot := false
	eof := false
	for {
		select {
		case msg := <-fromWalker:
			// we only care about the one file we're checking
			if msg.path != checkPath {
				continue
			}

			record, err := processPath(&c, msg)
			th.AssertNoErr(err)

			checkedRoot = true

			qfsRecord := th.GetRecord(msg.path)

			compare(qfsRecord, record)
		default:
			eof = true
		}

		if eof {
			break
		}
	}

	th.Assert(checkedRoot, "Didn't check %s", checkPath)
}

func TestFileMatches(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "testfile123"

		test.AssertNoErr(testutils.PrintToFile(workspace+"/"+filename,
			"some data"))

		test.checkUploadMatches(workspace+"/"+filename, workspace,
			func(recA quantumfs.DirectoryRecord,
				recB quantumfs.DirectoryRecord) {

				recADirect := recA.(*quantumfs.DirectRecord)
				recBDirect := recB.(*quantumfs.DirectRecord)

				// Everything should match, except fileId and nlinks
				recBDirect.SetFileId(recADirect.FileId())
				recBDirect.SetNlinks(recADirect.Nlinks())

				recAData, err := recADirect.MarshalJSON()
				test.AssertNoErr(err)
				recBData, err := recBDirect.MarshalJSON()
				test.AssertNoErr(err)

				test.Assert(bytes.Equal(recAData, recBData),
					"Record mismatch after qupload: %s\n%s\n",
					recAData, recBData)
			})
	})
}

func TestHardlinks(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		directory := workspace + "/dirA/dirB"

		// create files to compare
		test.AssertNoErr(os.MkdirAll(directory, 0777))

		// upload them
		dataStore = test.GetDataStore()
		wsDB = test.GetWorkspaceDB()
		ctx := newCtx("")
		ctx.Qctx = &test.TestCtx().Ctx
		var cliParams params
		cliParams.ws = "test/test/quploaded"
		cliParams.conc = 10
		cliParams.baseDir = workspace
		test.AssertNoErr(upload(ctx, &cliParams, "",
			exInfo))

		// now check that the uploaded workspace is the same
	})
}
