// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test to ensure Qparse works as desired

import "fmt"
import "io/ioutil"
import "os"
import "strings"
import "syscall"
import "testing"
import "unsafe"

import "github.com/aristanetworks/quantumfs/qlog"

func TestQParse_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		var logOut string
		test.qfs.c.Qlog.Write = func(format string,
			args ...interface{}) (int, error) {

			logOut += fmt.Sprintf(format + "\n", args...)
			return len(format), nil
		}
		// Enable *all* logs
		test.qfs.c.Qlog.LogLevels = (1 <<
			(unsafe.Sizeof(test.qfs.c.Qlog.LogLevels) * 8)) - 1

		// Do some stuff that should generate some logs
		workspace := test.nullWorkspace()
		testFilename := workspace + "/" + "test"
		fd, err := syscall.Creat(testFilename, 0124)
		test.assert(err == nil, "Error creating file: %v", err)
		syscall.Close(fd)

		data := genFibonacci(1024)
		err = printToFile(testFilename, string(data))
		test.assert(err == nil, "Couldn't write 1KB data to file")

		err = os.Truncate(testFilename, 0)
		test.assert(err == nil, "Couldn't truncate file to zero")

		_, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Unable to read file contents")

		test.log("This is a test string, %d", 12345)

		// Now grab the log file and compare against std out. Since logOut
		// started being appended to a bit late, and we sample it first, it
		// should be a subset of the qarsed logs
		logOutCopy := logOut
		testLogs := qlog.ParseLogs(test.qfs.config.CachePath + "/qlog")
		test.assert(strings.Contains(testLogs, logOutCopy), fmt.Sprintf(
			"Qparse and stdout don't match:\n%s\n\n=========\n\n%s",
			testLogs, logOutCopy))
		if false && !strings.Contains(testLogs, logOutCopy) {
			testLogLines := strings.Split(testLogs, "\n")
			logOutLines := strings.Split(logOutCopy, "\n")

			for i := 0; i < len(testLogLines); i++ {
				test.assert(len(logOutLines) > i,
					"More test stdout lines than qparse lines")

				errStr := ""
				if testLogLines[i] != logOutLines[i] {
					errStr += "\n=====START=====\n"
					errStr += testLogs
					errStr += "\n======SPLIT====\n"
					errStr += logOutCopy
					errStr += "\n=====END=====\n"
				}

				test.assert(testLogLines[i] == logOutLines[i],
					"Line mismatch @ %d:\n%s", i, errStr)
			}
		}
	})
}
