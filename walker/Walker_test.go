// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Test walker library by uploading files to QFS and then walking them using
// the walker.
package walker

import "os"

//import "strconv"
import "testing"
import "github.com/aristanetworks/quantumfs/daemon"

//import "github.com/aristanetworks/quantumfs"

// Test Walk of Dir
// Test Walk of File
// Test Walk of MedFile
// Test Walk of VeryLargeFile
// Test Walk of HardLink

func TestFileWriteBlockSize(t *testing.T) {
	runTest(t, func(test *daemon.TestHelper) {
		workspace := test.NewWorkspace()

		testFilename := workspace + "/" + "testwsize"
		file, err := os.Create(testFilename)
		test.Assert(file != nil && err == nil,
			"Error creating file: %v", err)
		defer file.Close()

		//var data []byte
		//data = strconv.AppendInt(data, 21, 10)

		data := []byte("HowdY")

		sz, err := file.Write(data)
		test.Assert(err == nil, "Error writing to new fd: %v", err)
		test.Assert(sz == len(data), "Incorrect numbers of blocks written:",
			" expected:%d   actual:%d,  %v", len(data), sz, err)
		test.AssertLogContains("operateOnBlocks offset 0 size 5",
			"Write block size not expected")
	})
}

//runTestCommand
//TestHelper
//	createTestDirs
//	endTest
//	startDefaultQuantumFs
//  startQuantumFS
//
// 	execute
// 	shouldFail
//	log
//
