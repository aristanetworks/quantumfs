// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package testutils

import "bytes"
import "runtime/debug"
import "fmt"
import "strings"
import "sync"
import "testing"
import "time"
import "io/ioutil"
import "os"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

type QuantumFsTest func(test *TestHelper)

type TestHelper struct {
	mutex             sync.Mutex // Protects a mishmash of the members
	t                 *testing.T
	testName          string
	cachePath         string
	logger            *qlog.Qlog
	tempDir           string
	fuseConnection    int
	api               *quantumfs.Api
	testResult        chan string
	startTime         time.Time
	shouldFail        bool
	shouldFailLogscan bool
}

// assert the condition is true. If it is not true then fail the test with the given
// message
func (th *TestHelper) Assert(condition bool, format string, args ...interface{}) {
	if !condition {
		msg := fmt.Sprintf(format, args...)
		panic(msg)
	}
}

/*
func (th *TestHelper) Init(t *testing.T, testName string, testResult chan string,
	startTime time.Time, cachePath string, logger *qlog.Qlog) {

	th.t = t
	th.testName = testName
	th.testResult = testResult
	th.startTime = startTime
	th.cachePath = cachePath
	th.logger = logger
}
*/

// Convert the given null terminated byte array into a string
// TODO: Move to utils
func BytesToString(data []byte) string {
	length := bytes.IndexByte(data, 0)
	if length == -1 {
		length = len(data)
	}
	return string(data[:length])
}

func (th *TestHelper) Execute(test QuantumFsTest) {
	// Catch any panics and covert them into test failures
	defer func(th *TestHelper) {
		err := recover()
		trace := ""

		// If the test passed pass that fact back to runTest()
		if err == nil {
			err = ""
		} else {
			// Capture the stack trace of the failure
			trace = BytesToString(debug.Stack())
			trace = strings.SplitN(trace, "\n", 8)[7]
		}

		var result string
		switch err.(type) {
		default:
			result = fmt.Sprintf("Unknown panic type: %v", err)
		case string:
			result = err.(string)
		case error:
			result = err.(error).Error()
		}

		if trace != "" {
			result += "\nStack Trace:\n" + trace
		}

		// This can hang if the channel isn't buffered because in some rare
		// situations the other side isn't there to read from the channel
		th.testResult <- result
	}(th)

	test(th)
}

func (th *TestHelper) EndTest() {

}

var TestRunDir string

func init() {

	var err error
	for i := 0; i < 10; i++ {
		TestRunDir, err = ioutil.TempDir("", "quantumfsTest")
		if err != nil {
			continue
		}
		if err := os.Chmod(TestRunDir, 777); err != nil {
			continue
		}
		return
	}
	panic(fmt.Sprintf("Unable to create temporary test directory: %v", err))
}
