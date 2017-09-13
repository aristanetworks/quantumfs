// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils/excludespec"
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
	up := NewUploader()

	err = filepath.Walk(checkPath,
		func(path string, info os.FileInfo, err error) error {
			return up.pathWalker(&c, fromWalker, path, workspace, info,
				err)
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

func (test *testHelper) checkQuploadMatches(workspace string) {
	workspaceB := "test/test/quploaded"

	// setup exclude to ignore the api file
	test.AssertNoErr(testutils.PrintToFile(test.TempDir+"/exInfo",
		"api"))
	var err error
	exInfo, err = excludespec.LoadExcludeInfo(workspace,
		test.TempDir+"/exInfo")
	test.AssertNoErr(err)

	// trigger upload
	dataStore = test.GetDataStore()
	wsDB = test.GetWorkspaceDB()
	ctx := newCtx("")
	ctx.Qctx = &test.TestCtx().Ctx
	var cliParams params
	cliParams.ws = workspaceB
	cliParams.conc = 10
	cliParams.baseDir = workspace
	up := NewUploader()
	test.AssertNoErr(up.upload(ctx, &cliParams, "", exInfo))

	// now check that the uploaded workspace is the same
	checkCmd := exec.Command("rsync", "-nHvrc", "--delete",
		workspace+"/", test.TempDir+"/mnt/"+workspaceB+"/")

	output, err := checkCmd.CombinedOutput()
	test.AssertNoErr(err)
	outputLines := strings.Split(string(output), "\n")

	// Check that we only have rsync boilerplate
	diffMsg := "Difference in qupload: %s"
	test.Assert(strings.Index(outputLines[0], "sending incremental file") == 0,
		diffMsg, output)
	test.Assert(len(outputLines[1]) == 0, diffMsg, output)
	test.Assert(strings.Index(outputLines[2], "sent") == 0, diffMsg, output)
	test.Assert(strings.Index(outputLines[3], "total size") == 0, diffMsg,
		output)
	test.Assert(len(outputLines[4]) == 0, diffMsg, output)

	// If there are no differences, the output will be only 5 lines long
	test.Assert(len(outputLines) == 5, diffMsg, output)
}

func TestFilesAndDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		// create files to compare
		directory := workspace + "/dirA/dirB"
		test.AssertNoErr(os.MkdirAll(directory, 0777))

		test.AssertNoErr(testutils.PrintToFile(workspace+"/dirA/fileA",
			"sample data"))

		test.checkQuploadMatches(workspace)
	})
}
