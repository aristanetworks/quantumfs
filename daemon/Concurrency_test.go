// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test concurrent workspaces

import (
	"bytes"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs/testutils"
)

func TestConcurrentReadWrite(t *testing.T) {
	t.Skip() // BUG229656
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0, workspace1 := test.setupDual()

		dataA := []byte("abc")
		dataB := []byte("def")
		fileA := "/fileA"
		fileB := "/fileB"

		test.AssertNoErr(testutils.PrintToFile(workspace0+fileA,
			string(dataA)))

		test.waitForPropagate(workspace1+fileA, dataA)

		test.AssertNoErr(testutils.PrintToFile(workspace0+fileB,
			string(dataA)))
		test.AssertNoErr(testutils.PrintToFile(workspace1+fileB,
			string(dataB)))

		test.waitForPropagate(workspace0+fileB, dataB)
	})
}

func TestConcurrentWriteDeletion(t *testing.T) {
	t.Skip() // BUG229656
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0, workspace1 := test.setupDual()
		dataA := []byte("abc")
		dataB := []byte("def")
		dataC := []byte("ghijk")
		file := "/fileA"

		// Open a file handle to be orphaned and write some data
		fd, err := os.OpenFile(workspace0+file, os.O_RDWR|os.O_CREATE, 0777)
		_, err = fd.Write(dataA[:1])
		test.AssertNoErr(err)

		test.waitForPropagate(workspace1+file, dataA[:1])

		defer fd.Close()
		test.AssertNoErr(err)
		n, err := fd.Write(dataA[1:])
		test.Assert(n == len(dataA)-1, "Not all data written")
		test.AssertNoErr(err)

		// Orphan the file from the other workspace at the same time as write
		os.Remove(workspace1 + file)

		// Wait for file to be deleted
		test.waitForPropagate(workspace0+file, []byte{})

		// Check that our file was orphaned
		n, err = fd.Write(dataB)
		test.Assert(n == len(dataB), "Not all dataB written")
		test.AssertNoErr(err)

		// Now check that we can make a new file in its place with the orphan
		// still around
		test.AssertNoErr(testutils.PrintToFile(workspace1+file,
			string(dataC)))

		test.waitForPropagate(workspace0+file, dataC)

		// Check that we can still read everything from the orphan
		buf := make([]byte, 10)
		n, err = fd.ReadAt(buf, 0)
		test.Assert(err == io.EOF, "Didn't read all file contents")
		test.Assert(bytes.Equal(buf[:n], append(dataA, dataB...)),
			"Mismatched data in orphan: %s", buf[:n])
	})
}

func TestConcurrentHardlinks(t *testing.T) {
	t.Skip() // BUG229656
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0, workspace1 := test.setupDual()

		dataA := []byte("abc")
		fileA := "/fileA"
		fileB := "/fileB"

		test.AssertNoErr(testutils.PrintToFile(workspace0+fileA,
			string(dataA)))

		test.waitForPropagate(workspace1+fileA, dataA)

		test.AssertNoErr(syscall.Link(workspace1+fileA, workspace1+fileB))

		test.waitForPropagate(workspace0+fileB, dataA)

		test.AssertNoErr(os.Remove(workspace0 + fileA))

		test.waitForPropagate(workspace1+fileA, []byte{})
	})
}

func TestConcurrentHardlinkWrites(t *testing.T) {
	t.Skip() // BUG229656
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0, workspace1 := test.setupDual()

		dataA := []byte("abc")
		fileA := "/fileA"
		fileB := "/fileB"
		fileC := "/fileC"

		test.AssertNoErr(testutils.PrintToFile(workspace0+fileA,
			string(dataA)))

		test.waitForPropagate(workspace1+fileA, dataA)

		test.AssertNoErr(syscall.Link(workspace1+fileA, workspace1+fileB))
		test.AssertNoErr(syscall.Link(workspace0+fileA, workspace0+fileC))

		test.waitForPropagate(workspace0+fileB, dataA)

		test.SameLink(workspace0+fileA, workspace0+fileC)
		test.SameLink(workspace0+fileA, workspace0+fileB)
	})
}

func TestConcurrentIntraFileMerges(t *testing.T) {
	t.Skip() // BUG229656
	runDualQuantumFsTest(t, func(test *testHelper) {
		workspace0, workspace1 := test.setupDual()

		dataA := []byte("0000\n00\n0000")
		dataB := []byte("0000\n22\n0444")
		dataC := []byte("1110\n33\n0000")
		expect := []byte("1110\n33\n0444")
		file := "/file"

		test.AssertNoErr(testutils.PrintToFile(workspace0+file,
			string(dataA)))

		test.waitForPropagate(workspace1+file, dataA)

		test.AssertNoErr(testutils.OverWriteFile(workspace1+file,
			string(dataB)))

		test.AssertNoErr(testutils.OverWriteFile(workspace0+file,
			string(dataC)))

		// There should be a merge conflict, resolved by an intra-file
		// merge, that eventually is reflected in both workspaces
		test.waitForPropagate(workspace0+file, expect)
		test.waitForPropagate(workspace1+file, expect)
	})
}
