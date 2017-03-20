// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Test walker library by uploading files to QFS and then walking them using
// the walker.
package walker

import "os"

//import "strconv"

import "flag"
import "runtime/debug"
import "runtime"
import "fmt"
import "sync"

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

// Seperate for each package
func TestMain(m *testing.M) {
	flag.Parse()

	if os.Getuid() != 0 {
		panic("quantumfs.daemon tests must be run as root")
	}

	// Disable Garbage Collection. Because the tests provide both the filesystem
	// and the code accessing that filesystem the program is reentrant in ways
	// opaque to the golang scheduler. Thus we can end up in a deadlock situation
	// between two threads:
	//
	// ThreadFS is the filesystem, ThreadT is the test
	//
	//   ThreadFS                    ThreadT
	//                               Start filesystem syscall
	//   Start executing response
	//   <GC Wait>                   <Queue GC wait after syscal return>
	//                        DEADLOCK
	//
	// Because the filesystem request is blocked waiting on GC and the syscall
	// will never return to allow GC to progress, the test program is deadlocked.
	origGC := debug.SetGCPercent(-1)

	// Precompute a bunch of our genData to save time during tests
	//genData(40 * 1024 * 1024)

	// Setup an array for tests with errors to be logscanned later
	errorLogs = make([]logscanError, 0)

	result := m.Run()

	// We've finished running the tests and are about to do the full logscan.
	// This create a tremendous amount of garbage, so we must enable garbage
	// collection.
	runtime.GC()
	debug.SetGCPercent(origGC)

	errorMutex.Lock()
	fullLogs := make(chan string, len(errorLogs))
	var logProcessing sync.WaitGroup
	for i := 0; i < len(errorLogs); i++ {
		logProcessing.Add(1)
		go func(i int) {
			defer logProcessing.Done()
			//testSummary := outputLogError(errorLogs[i])
			//fullLogs <- testSummary
		}(i)
	}

	logProcessing.Wait()
	close(fullLogs)
	testSummary := ""
	for summary := range fullLogs {
		testSummary += summary
	}
	errorMutex.Unlock()
	fmt.Println("------ Test Summary:\n" + testSummary)

	os.RemoveAll(daemon.TestRunDir)
	os.Exit(result)
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
