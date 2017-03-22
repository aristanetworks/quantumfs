// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import "bytes"
import "flag"
import "runtime/debug"
import "runtime"
import "testing"
import "fmt"
import "strings"
import "sync"
import "os"
import "time"

import "github.com/aristanetworks/quantumfs/testutils"

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
	genData(40 * 1024 * 1024)

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
	outputTimeGraph := strings.Contains(testSummary, "TIMED OUT")
	testutils.ErrorMutex.Unlock()
	fmt.Println("------ Test Summary:\n" + testSummary)

	if outputTimeGraph {
		outputTimeHistogram()
	}

	os.RemoveAll(TestRunDir)
	os.Exit(result)
}

func TestRandomNamespaceName(t *testing.T) {
	runTestNoQfs(t, func(test *TestHelper) {
		name1 := randomNamespaceName(8)
		name2 := randomNamespaceName(8)
		name3 := randomNamespaceName(10)

		test.Assert(len(name1) == 8, "name1 wrong length: %d", len(name1))
		test.Assert(name1 != name2, "name1 == name2: '%s'", name1)
		test.Assert(len(name3) == 10, "name3 wrong length: %d", len(name1))
	})
}

// If a quantumfs test fails then it may leave the filesystem mount hanging around in
// a blocked state. testHelper needs to forcefully abort and umount these to keep the
// system functional. Test this forceful unmounting here.
func TestPanicFilesystemAbort(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		test.ShouldFailLogscan = true

		api := test.getApi()

		// Introduce a panicing error into quantumfs
		test.qfs.mapMutex.Lock()
		for k, v := range test.qfs.fileHandles {
			test.qfs.fileHandles[k] = &crashOnWrite{FileHandle: v}
		}
		test.qfs.mapMutex.Unlock()

		// panic Quantumfs
		api.Branch("_null/_null/null", "branch/test/crash")
	})
}

// If a test never returns from some event, such as an inifinite loop, the test
// should timeout and cleanup after itself.
func TestTimeout(t *testing.T) {
	runTest(t, func(test *TestHelper) {
		test.ShouldFail = true
		time.Sleep(60 * time.Second)

		// If we get here then the test library didn't time us out and we
		// sould fail this test.
		test.ShouldFail = false
		test.Assert(false, "Test didn't fail due to timeout")
	})
}

func TestGenData(t *testing.T) {
	runTestNoQfs(t, func(test *TestHelper) {
		hardcoded := "012345678910111213141516171819202122232425262"
		data := genData(len(hardcoded))

		test.Assert(bytes.Equal([]byte(hardcoded), data),
			"Data gen function off: %s vs %s", hardcoded, data)
	})
}
