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
	runTest(t, func(test *daemon.TestHelper) {
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

	if os.Getuid() != 0 {
		panic("quantumfs.walker tests must be run as root")
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

	// Setup an array for tests with errors to be logscanned later
	testutils.ErrorLogs = make([]testutils.LogscanError, 0)

	result := m.Run()

	// We've finished running the tests and are about to do the full logscan.
	// This create a tremendous amount of garbage, so we must enable garbage
	// collection.
	runtime.GC()
	debug.SetGCPercent(origGC)

	testutils.ErrorMutex.Lock()
	fullLogs := make(chan string, len(testutils.ErrorLogs))
	var logProcessing sync.WaitGroup
	for i := 0; i < len(testutils.ErrorLogs); i++ {
		logProcessing.Add(1)
		go func(i int) {
			defer logProcessing.Done()
			testSummary :=
				testutils.OutputLogError(testutils.ErrorLogs[i])
			fullLogs <- testSummary
		}(i)
	}

	logProcessing.Wait()
	close(fullLogs)
	testSummary := ""
	for summary := range fullLogs {
		testSummary += summary
	}
	testutils.ErrorMutex.Unlock()
	fmt.Println("------ Test Summary:\n" + testSummary)

	os.RemoveAll(daemon.TestRunDir)
	os.Exit(result)
}
