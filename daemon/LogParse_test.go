// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test to ensure Qparse works as desired

import "bytes"
import "fmt"
import "io/ioutil"
import "math"
import "os"
import "sort"
import "strings"
import "sync"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/testutils"

func trimToStr(test *testHelper, logs []string, boundary string) []string {
	boundaryCount := 0
	var boundaryStart, boundaryEnd int
	for i := 0; i < len(logs); i++ {
		if strings.Contains(logs[i], boundary) {
			if boundaryCount == 0 {
				boundaryStart = i
			} else if boundaryCount == 1 {
				boundaryEnd = i
			}
			boundaryCount++
		}
	}
	test.Assert(boundaryCount == 2, "Miscount of boundary markers %d",
		boundaryCount)
	return logs[boundaryStart : boundaryEnd+1]
}

func TestMaxStringFail_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		longStr := string(GenData(math.MaxUint16))

		test.qfs.c.wlog("%s %d", longStr, 255)

		testLogs := qlog.ParseLogs(test.qfs.config.CachePath + "/qlog")
		test.Assert(strings.Contains(testLogs,
			"Packet has been clipped"),
			"Over length string doesn't cause last parameter to drop")
	})
}

func TestMaxStringLast_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		longStr := string(GenData(math.MaxUint16))

		test.qfs.c.wlog("%s", longStr)

		testLogs := qlog.ParseLogs(test.qfs.config.CachePath + "/qlog")
		test.Assert(strings.Contains(testLogs,
			"Log data exceeds allowable length"),
			"Over length string doesn't trigger warning")
	})
}

func TestQParse(t *testing.T) {
	runTest(t, func(test *testHelper) {
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

		testLogBoundary := "TestQParseComparing12345"
		test.qfs.c.wlog(testLogBoundary)

		// Do some stuff that should generate some logs
		workspace := test.NewWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := syscall.Creat(testFilename, 0124)
		test.Assert(err == nil, "Error creating file: %v", err)
		syscall.Close(fd)

		data := GenData(1024)
		err = testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Couldn't write 1KB data to file")

		test.Log("This is a test string, %d", 12345)

		err = os.Truncate(testFilename, 0)
		test.Assert(err == nil, "Couldn't truncate file to zero")

		_, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Unable to read file contents")

		test.qfs.c.wlog(testLogBoundary)

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
		sort.Sort(qlog.SortString(logOutLines))

		// Trim any lines outside of our "comparison boundaries"
		testLogLines = trimToStr(test, testLogLines, testLogBoundary)
		logOutLines = trimToStr(test, logOutLines, testLogBoundary)
		test.Assert(len(testLogLines) == len(logOutLines),
			"log length mismatch %d %d", len(testLogLines),
			len(logOutLines))
		test.Assert(len(testLogLines) > 2, "trimToStr not working")

		debugStr := ""
		for i := 0; i < len(logOutLines); i++ {
			if logOutLines[i] != testLogLines[i] {
				startOut := i - 5
				endOut := i + 5
				if startOut < 0 {
					startOut = 0
				}
				if endOut >= len(testLogLines) {
					endOut = len(testLogLines) - 1
				}

				for j := startOut; j <= endOut; j++ {
					if j == i {
						debugStr += ("!!!!!!!!!!!!!!!!!!!\n")
					}
					debugStr += fmt.Sprintf("!!Q%d: %s\n",
						j, testLogLines[j])
					debugStr += fmt.Sprintf("!!L%d: %s\n",
						j, logOutLines[j])
					if j == i {
						debugStr += ("!!!!!!!!!!!!!!!!!!!\n")
					}
				}

				test.Assert(false, "Qparse/stdout mismatch:\n%s\n"+
					" |%v| |%v|", debugStr,
					[]byte(logOutLines[i]),
					[]byte(testLogLines[i]))
			}
		}
	})
}

func TestQParsePartials_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		// Before we enable logs, let's cause all File logs to be
		// partially written
		test.qfs.c.Qlog.EnterTestMode("---In File::")

		// Enable *all* logs
		test.qfs.c.Qlog.LogLevels = 0
		test.qfs.c.Qlog.LogLevels--

		// Do some stuff that should generate some logs
		workspace := test.NewWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := syscall.Creat(testFilename, 0124)
		test.Assert(err == nil, "Error creating file: %v", err)
		syscall.Close(fd)

		data := GenData(1024)
		err = testutils.PrintToFile(testFilename, string(data))
		test.Assert(err == nil, "Couldn't write 1KB data to file")

		test.Log("This is a test string, %d", 12345)

		err = os.Truncate(testFilename, 0)
		test.Assert(err == nil, "Couldn't truncate file to zero")

		_, err = ioutil.ReadFile(testFilename)
		test.Assert(err == nil, "Unable to read file contents")

		// Now grab the log file
		testLogs := qlog.ParseLogs(test.qfs.config.CachePath + "/qlog")

		testLogLines := strings.Split(testLogs, "\n")
		droppedEntry := false
		count := 0
		// Check to see if we see dropped packets interspersed with good ones
		for i := 0; i < len(testLogLines); i++ {
			test.Assert(len(testLogLines[i]) < 6 ||
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

		test.Assert(count >= 5,
			"Unable to confidently prove partial packet reading")
	})
}

func TestBooleanLogType(t *testing.T) {
	runTest(t, func(test *testHelper) {

		test.qfs.c.wlog("booleans %t %t", true, false)

		testLogs := qlog.ParseLogs(test.qfs.config.CachePath + "/qlog")
		test.Assert(strings.Contains(testLogs, "booleans true false"),
			"boolean log types incorrectly output: %s", testLogs)
	})
}
