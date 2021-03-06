// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils/excludespec"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

func (th *testHelper) checkUploadMatches(checkPath string, workspace string,
	compare func(quantumfs.ImmutableDirectoryRecord,
		quantumfs.ImmutableDirectoryRecord)) {

	th.SyncWorkspace(th.RelPath(workspace))

	c := Ctx{
		Qctx: &(th.TestCtx().Ctx),
	}
	_, c.eCtx = errgroup.WithContext(context.Background())

	fromWalker := make(chan *pathInfo, 100)
	up := NewUploader()
	up.dataStore = th.GetDataStore()

	err := filepath.Walk(checkPath,
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

			record, _, _, err := up.processPath(&c, msg)
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

type encoded *quantumfs.EncodedDirectoryRecord

func toEncoded(record quantumfs.ImmutableDirectoryRecord) encoded {
	rtn := quantumfs.NewDirectoryRecord()
	rtn.SetFilename(record.Filename())
	rtn.SetID(record.ID())
	rtn.SetType(record.Type())
	rtn.SetPermissions(record.Permissions())
	rtn.SetOwner(record.Owner())
	rtn.SetGroup(record.Group())
	rtn.SetSize(record.Size())
	rtn.SetExtendedAttributes(record.ExtendedAttributes())
	rtn.SetContentTime(record.ContentTime())
	rtn.SetModificationTime(record.ModificationTime())
	rtn.SetFileId(record.FileId())
	return rtn
}

func TestFileMatches(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()
		filename := "testfile123"

		test.AssertNoErr(testutils.PrintToFile(workspace+"/"+filename,
			"some data"))

		test.checkUploadMatches(workspace+"/"+filename, workspace,
			func(recA quantumfs.ImmutableDirectoryRecord,
				recB quantumfs.ImmutableDirectoryRecord) {

				A := toEncoded(recA)
				recADirect := (*quantumfs.EncodedDirectoryRecord)(A)
				B := toEncoded(recB)
				recBDirect := (*quantumfs.EncodedDirectoryRecord)(B)

				// Everything except fileId should match
				recBDirect.SetFileId(recADirect.FileId())

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

func (test *testHelper) checkQuploadMatches(workspace string,
	triggerErr func()) (dataWritten uint64, metadataWritten uint64) {

	// give it a random suffix to ensure multiple calls don't collide
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	workspaceB := fmt.Sprintf("test/test/quploaded_%d", r.Uint32())

	up := NewUploader()

	// setup exclude to ignore the api file
	os.Remove(test.TempDir + "/exInfo")
	test.AssertNoErr(testutils.PrintToFile(test.TempDir+"/exInfo",
		"api"))
	var err error
	up.exInfo, err = excludespec.LoadExcludeInfo(workspace,
		test.TempDir+"/exInfo")
	test.AssertNoErr(err)

	// trigger upload
	up.dataStore = test.GetDataStore()
	up.wsDB = test.GetWorkspaceDB()
	ctx := newCtx("")
	ctx.Qctx = &test.TestCtx().Ctx
	var cliParams params
	cliParams.ws = workspaceB
	cliParams.conc = 10
	cliParams.baseDir = workspace
	newWsr, err := up.upload(ctx, &cliParams, "")
	test.AssertNoErr(err)

	test.WaitForRefreshTo(workspaceB, newWsr)

	for i := 0; i < 2; i++ {
		// now check that the uploaded workspace is the same
		checkCmd := exec.Command("rsync", "-nHXvrc", "--delete", "--links",
			workspace+"/", test.TempDir+"/mnt/"+workspaceB+"/")

		output, err := checkCmd.CombinedOutput()
		test.AssertNoErr(err)
		outputLines := strings.Split(string(output), "\n")

		if i == 0 {
			// Check that we only have rsync boilerplate
			diffMsg := "Difference in qupload: %s"
			test.Assert(strings.Index(outputLines[0],
				"sending incremental file") == 0, diffMsg, output)
			test.Assert(len(outputLines[1]) == 0, diffMsg, output)
			test.Assert(strings.Index(outputLines[2], "sent") == 0,
				diffMsg, output)
			test.Assert(strings.Index(outputLines[3], "total size") == 0,
				diffMsg, output)
			test.Assert(len(outputLines[4]) == 0, diffMsg, output)

			// If there are no differences, the output will be 5 lines
			test.Assert(len(outputLines) == 5, diffMsg, output)

			triggerErr()
		} else {
			// On this iteration, the check should fail. Doing this
			// ensures that the test is always actually testing
			test.Assert(len(outputLines) != 5, "No diff found:\n%s",
				output)
		}
	}

	return up.dataBytesWritten, up.metadataBytesWritten
}

func TestFilesAndDir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		// create files to compare
		directory := workspace + "/dirA/dirB"
		test.AssertNoErr(os.MkdirAll(directory, 0777))

		test.AssertNoErr(testutils.PrintToFile(workspace+"/dirA/fileA",
			"sample data"))

		test.checkQuploadMatches(workspace, func() {
			test.AssertNoErr(testutils.PrintToFile(
				workspace+"/dirA/fileA",
				"sample data changed"))
		})
	})
}

func TestHardlinks(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		// create files to compare
		directory := workspace + "/dirA/dirB"
		test.AssertNoErr(os.MkdirAll(directory, 0777))

		fileA := workspace + "/dirA/fileA"
		fileB := workspace + "/dirA/dirB/fileB"
		linkA := workspace + "/linkA"
		linkB := workspace + "/dirA/dirB/linkB"
		linkC := workspace + "/dirA/dirB/linkC"
		test.AssertNoErr(testutils.PrintToFile(fileA, "sample data"))
		test.AssertNoErr(testutils.PrintToFile(fileB, "sample data"))
		test.AssertNoErr(os.Link(fileA, linkA))
		test.AssertNoErr(os.Link(linkA, linkB))
		test.AssertNoErr(os.Link(fileA, linkC))

		test.checkQuploadMatches(workspace, func() {
			test.AssertNoErr(os.Remove(linkA))
			test.AssertNoErr(os.Link(fileB, linkA))
		})
	})
}

func TestExtAttr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		directory := workspace + "/dirA/dirB"
		test.AssertNoErr(os.MkdirAll(directory, 0777))

		fileA := workspace + "/dirA/fileA"
		fileB := workspace + "/dirA/dirB/fileB"

		test.AssertNoErr(testutils.PrintToFile(fileA, "fileA has data"))
		test.AssertNoErr(testutils.PrintToFile(fileB, "fileB as well"))

		test.AssertNoErr(syscall.Setxattr(fileA, "user.noData", []byte{}, 0))
		test.AssertNoErr(syscall.Setxattr(fileB, "user.data", []byte("abc"),
			0))

		test.AssertNoErr(syscall.Setxattr(directory, "user.dirData",
			[]byte("dir data"), 0))

		linkA := workspace + "/linkA"
		test.AssertNoErr(os.Link(fileA, linkA))
		test.AssertNoErr(syscall.Setxattr(linkA, "user.linkData",
			[]byte("link"), 0))

		test.checkQuploadMatches(workspace, func() {
			test.AssertNoErr(syscall.Setxattr(linkA, "user.diffData",
				[]byte("something"), 0))
		})
	})
}

func TestSymlinks(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		directory := workspace + "/dirA/dirB"
		test.AssertNoErr(os.MkdirAll(directory, 0777))

		fileA := workspace + "/dirA/fileA"
		fileB := workspace + "/dirA/dirB/fileB"

		linkA := workspace + "/linkA"
		linkB := workspace + "/dirA/linkB"

		test.AssertNoErr(testutils.PrintToFile(fileA, "fileA has data"))
		test.AssertNoErr(testutils.PrintToFile(fileB, "fileB has data"))

		test.AssertNoErr(os.Symlink(fileA, linkA))
		test.AssertNoErr(os.Symlink(fileB, linkB))

		test.checkQuploadMatches(workspace, func() {
			test.AssertNoErr(os.Remove(linkB))
			test.AssertNoErr(testutils.PrintToFile(linkB,
				"fileB has data"))
		})
	})
}

func TestEmptyDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		test.checkQuploadMatches(workspace, func() {
			test.AssertNoErr(testutils.PrintToFile(workspace+"/somefile",
				"random file"))
		})
	})
}

func TestUploadBytes(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		// create a whole bunch of hardlinks and files to generate
		// as much possibility for concurrency issues as possible
		for i := 0; i < 20; i++ {
			prefix := workspace + fmt.Sprintf("/iter%d", i)
			test.AssertNoErr(testutils.PrintToFile(prefix+"_file",
				fmt.Sprintf("%d", i)))

			for j := 0; j < 4; j++ {
				syscall.Link(prefix+"_file", prefix+
					fmt.Sprintf("_link%d_%d", i, j))
			}

			test.AssertNoErr(os.Symlink(prefix, prefix+"_symlink"))
			test.AssertNoErr(os.Mkdir(prefix+"_dir", 0777))
		}

		var dataWritten, metadataWritten uint64
		for i := 0; i < 4; i++ {
			data, metadata := test.checkQuploadMatches(workspace,
				func() {
					test.AssertNoErr(testutils.PrintToFile(""+
						workspace+"/wrongfile", "some data"))
				})
			// Make sure to remove the "wrongfile"
			test.AssertNoErr(os.Remove(workspace + "/wrongfile"))

			if i == 0 {
				dataWritten = data
				metadataWritten = metadata
			}
			test.TestCtx().Vlog(qlog.LogTest,
				"Qupload iteration %d: %d %d", i, data,
				metadataWritten)

			test.Assert(data == dataWritten,
				"Number of data bytes changed: %d vs %d",
				dataWritten, data)
			test.Assert(metadata == metadataWritten,
				"Number of metadata bytes changed: %d vs %d",
				metadataWritten, metadata)
		}
	})
}
