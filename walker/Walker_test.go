// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "flag"
import "fmt"
import "os"
import "runtime/debug"
import "runtime"
import "sync"
import "testing"

import "github.com/aristanetworks/quantumfs/daemon"
import "github.com/aristanetworks/quantumfs/testutils"

// TODO(sid)
// Test Walk of Dir
// Test Walk of File
// Test Walk of MedFile
// Test Walk of VeryLargeFile
// Test Walk of HardLink
// Test walk of small random dir structure

// Just a dummy Test. Replace with real walker related test.
func TestFileWriteBlockSize(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.NewWorkspace()

		testFilename := workspace + "/" + "testwsize"
		file, err := os.Create(testFilename)
		test.Assert(file != nil && err == nil,
			"Error creating file: %v", err)
		defer file.Close()

		data := []byte("HowdY")

		sz, err := file.Write(data)
		test.Assert(err == nil, "Error writing to new fd: %v", err)
		test.Assert(sz == len(data), "Incorrect numbers of blocks written:",
			" expected:%d   actual:%d,  %v", len(data), sz, err)
		test.AssertLogContains("operateOnBlocks offset 0 size 5",
			"Write block size not expected")
	})
}

func TestMain(m *testing.M) {
	flag.Parse()

	daemon.PreTestRuns()
	result := m.Run()
	daemon.PostTestRuns()

	os.Exit(result)
}
