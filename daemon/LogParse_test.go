// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test to ensure Qparse works as desired

import "bytes"
import "fmt"
import "io/ioutil"
import "os"
import "sort"
import "strings"
import "sync"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs/qlog"

func TestQParse_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		var logOut bytes.Buffer
		var testMutex sync.Mutex
		test.qfs.c.Qlog.Write = func(format string,
			args ...interface{}) error {

			testMutex.Lock()
			logOut.WriteString(fmt.Sprintf(format+"\n", args...))
			testMutex.Unlock()
			return nil
		}
		// Enable *all* logs
		test.qfs.c.Qlog.LogLevels = 0
		test.qfs.c.Qlog.LogLevels--

		// Do some stuff that should generate some logs
		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := syscall.Creat(testFilename, 0124)
		test.assert(err == nil, "Error creating file: %v", err)
		syscall.Close(fd)

		data := genData(1024)
		err = printToFile(testFilename, string(data))
		test.assert(err == nil, "Couldn't write 1KB data to file")

		test.log("This is a test string, %d", 12345)

		err = os.Truncate(testFilename, 0)
		test.assert(err == nil, "Couldn't truncate file to zero")

		_, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Unable to read file contents")

		// Now grab the log file and compare against std out. Since logOut
		// started being appended to a bit late, and we sample it first, it
		// should be a subset of the qarsed logs
		testMutex.Lock()
		logOutCopy := string(logOut.Bytes())
		testMutex.Unlock()
		testLogs := qlog.ParseLogs(test.qfs.config.CachePath + "/qlog")

		// There's nothing ensuring the order is the same, so we have to sort
		testLogLines := strings.Split(testLogs, "\n")
		logOutLines := strings.Split(logOutCopy, "\n")
		sort.Sort(qlog.SortByTime(logOutLines))

		// Trim any excess empty lines
		for logOutLines[0] == "" {
			logOutLines = logOutLines[1:]
		}

		// Find out at what point logOut starts in testLog
		offset := 0
		for i := 0; i < len(testLogLines); i++ {
			if logOutLines[0] == testLogLines[i] {
				offset = i
				break
			}
		}

		debugStr := ""
		for i := 0; i < len(logOutLines); i++ {
			if logOutLines[i] != testLogLines[i+offset] {
				startOut := i - 5
				endOut := i + 5
				if startOut < 0 {
					startOut = 0
				}
				if endOut >= len(testLogLines) {
					endOut = len(testLogLines) - 1
				}

				for j := startOut; j <= endOut; j++ {
					debugStr += fmt.Sprintf("!!Q%d: %s\n",
						j, testLogLines[j+offset])
					debugStr += fmt.Sprintf("!!L%d: %s\n",
						j, logOutLines[j])
				}
				test.assert(false, "Qparse/stdout mismatch:\n"+
					debugStr)
			}
		}
	})
}

func TestQParsePartials_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		// Before we enable logs, let's cause all File logs to be
		// partially written
		test.qfs.c.Qlog.EnterTestMode("File::")

		// Enable *all* logs
		test.qfs.c.Qlog.LogLevels = 0
		test.qfs.c.Qlog.LogLevels--

		// Do some stuff that should generate some logs
		workspace := test.newWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := syscall.Creat(testFilename, 0124)
		test.assert(err == nil, "Error creating file: %v", err)
		syscall.Close(fd)

		data := genData(1024)
		err = printToFile(testFilename, string(data))
		test.assert(err == nil, "Couldn't write 1KB data to file")

		test.log("This is a test string, %d", 12345)

		err = os.Truncate(testFilename, 0)
		test.assert(err == nil, "Couldn't truncate file to zero")

		_, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Unable to read file contents")

		// Now grab the log file
		testLogs := qlog.ParseLogs(test.qfs.config.CachePath + "/qlog")

		testLogLines := strings.Split(testLogs, "\n")
		droppedEntry := false
		count := 0
		// Check to see if we see dropped packets interspersed with good ones
		for i := 0; i < len(testLogLines); i++ {
			test.assert(len(testLogLines[i]) < 6 ||
				strings.Compare(testLogLines[i][:6], "File::") != 0,
				"Not all File:: packets are broken")

			// Count the number of times we go from a good to broken log
			isPartial := strings.Contains(testLogLines[i],
				"incomplete packet")
			if isPartial && !droppedEntry {
				count++
			}
			droppedEntry = isPartial
		}

		test.assert(count >= 10,
			"Unable to confidently prove partial packet reading")
	})
}
