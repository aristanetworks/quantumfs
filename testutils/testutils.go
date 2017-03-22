// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// testutils package provides helper functions for tools which do not need
// to have a running quantumfs instance.
package testutils

import "bytes"
import "errors"
import "fmt"
import "io"
import "io/ioutil"
import "os"
import "runtime/debug"
import "strings"
import "sync"
import "syscall"
import "testing"
import "time"

//import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/utils"

type QuantumFsTest func(test *TestHelper)

type TestHelper struct {
	mutex             sync.Mutex // Protects a mishmash of the members
	T                 *testing.T
	TestName          string
	CachePath         string
	Logger            *qlog.Qlog
	TempDir           string
	TestResult        chan string
	StartTime         time.Time
	ShouldFail        bool
	ShouldFailLogscan bool
}

// Assert the condition is true. If it is not true then fail the test with the given
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
			trace = utils.BytesToString(debug.Stack())
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
		th.TestResult <- result
	}(th)

	test(th)
}

func (th *TestHelper) EndTest() {

}

var TestRunDir string

func init() {

	syscall.Umask(0)
	var err error
	for i := 0; i < 100; i++ {
		TestRunDir, err = ioutil.TempDir("", "quantumfsTest")
		if err != nil {
			continue
		}
		if err := os.Chmod(TestRunDir, 0777); err != nil {
			continue
		}
		fmt.Printf("testutils testutils.TestRunDir %s\n", TestRunDir)
		return
	}
	panic(fmt.Sprintf("Unable to create temporary test directory: %v", err))
}

// CreateTestDirs makes the required directories for the test.
// This directories are inside TestRunDir
func (th *TestHelper) CreateTestDirs() {
	th.TempDir = TestRunDir + "/" + th.TestName

	mountPath := th.TempDir + "/mnt"
	os.MkdirAll(mountPath, 0777)
	th.Log("Using mountpath %s", mountPath)

	os.MkdirAll(th.TempDir+"/ether", 0777)
}

func (th *TestHelper) BaseInit(t *testing.T, testName string, testResult chan string,
	startTime time.Time, cachePath string, logger *qlog.Qlog) {

	th.T = t
	th.TestName = testName
	th.TestResult = testResult
	th.StartTime = startTime
	th.CachePath = cachePath
	th.Logger = logger
}

type TLA struct {
	MustContain bool   // Whether the log must or must not contain the text
	Text        string // text which must or must not be in the log
	FailMsg     string // Message to fail with
}

// Assert the test log contains the given text
func (th *TestHelper) AssertLogContains(text string, failMsg string) {
	th.AssertTestLog([]TLA{TLA{true, text, failMsg}})
}

// Assert the test log doesn't contain the given text
func (th *TestHelper) AssertLogDoesNotContain(text string, failMsg string) {
	th.AssertTestLog([]TLA{TLA{false, text, failMsg}})
}

func (th *TestHelper) AssertTestLog(logs []TLA) {
	contains := th.messagesInTestLog(logs)

	for i, tla := range logs {
		th.Assert(contains[i] == tla.MustContain, tla.FailMsg)
	}
}

func (th *TestHelper) messagesInTestLog(logs []TLA) []bool {
	logFile := th.TempDir + "/ramfs/qlog"
	logLines := qlog.ParseLogsRaw(logFile)

	containChecker := make([]bool, len(logs))

	for _, rawlog := range logLines {
		logOutput := rawlog.ToString()
		for idx, tla := range logs {
			exists := strings.Contains(logOutput, tla.Text)
			if exists {
				containChecker[idx] = true
			}
		}
	}

	return containChecker
}

func (th *TestHelper) TestLogContains(text string) bool {
	return th.allTestLogsMatch([]TLA{TLA{true, text, ""}})
}

func (th *TestHelper) TestLogDoesNotContain(text string) bool {
	return th.allTestLogsMatch([]TLA{TLA{false, text, ""}})
}

func (th *TestHelper) allTestLogsMatch(logs []TLA) bool {
	contains := th.messagesInTestLog(logs)

	for i, tla := range logs {
		if contains[i] != tla.MustContain {
			return false
		}
	}
	return true
}

func (th *TestHelper) FileSize(filename string) int64 {
	var stat syscall.Stat_t
	err := syscall.Stat(filename, &stat)
	th.Assert(err == nil, "Error stat'ing test file: %v", err)
	return stat.Size
}

type TimeData struct {
	Duration time.Duration
	TestName string
}

var TimeMutex sync.Mutex
var TimeBuckets []TimeData

func OutputTimeHistogram() {
	TimeMutex.Lock()
	histogram := make([][]string, 20)
	maxValue := 0
	msPerBucket := 100.0
	for i := 0; i < len(TimeBuckets); i++ {
		bucketIdx := int(TimeBuckets[i].Duration.Seconds() *
			(1000.0 / msPerBucket))
		if bucketIdx >= len(histogram) {
			bucketIdx = len(histogram) - 1
		}
		histogram[bucketIdx] = append(histogram[bucketIdx],
			TimeBuckets[i].TestName)

		if len(histogram[bucketIdx]) > maxValue {
			maxValue = len(histogram[bucketIdx])
		}
	}
	TimeMutex.Unlock()

	// Scale outputs to fit into 60 columns wide
	scaler := 1.0
	if maxValue > 60 {
		scaler = 60.0 / float64(maxValue)
	}

	fmt.Println("Test times:")
	for i := len(histogram) - 1; i >= 0; i-- {
		fmt.Printf("|%4dms|", (1+i)*int(msPerBucket))

		scaled := int(float64(len(histogram[i])) * scaler)
		if scaled == 0 && len(histogram[i]) > 0 {
			scaled = 1
		}

		for j := 0; j < scaled; j++ {
			fmt.Printf("#")
		}
		if len(histogram[i]) > 0 && len(histogram[i]) <= 4 {
			fmt.Printf("(")
			for j := 0; j < len(histogram[i]); j++ {
				if j != 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%s", histogram[i][j])
			}
			fmt.Printf(")")
		}
		fmt.Println()
	}
}

func PrintToFile(filename string, data string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0777)
	if file == nil || err != nil {
		return err
	}
	defer file.Close()

	written := 0
	for written < len(data) {
		var writeIt int
		writeIt, err = file.Write([]byte(data[written:]))
		written += writeIt
		if err != nil {
			return errors.New("Unable to write all data")
		}
	}

	return nil
}

func (th *TestHelper) ReadTo(file *os.File, offset int, num int) []byte {
	rtn := make([]byte, num)

	for totalCount := 0; totalCount < num; {
		readIt, err := file.ReadAt(rtn[totalCount:],
			int64(offset+totalCount))
		if err == io.EOF {
			return rtn[:totalCount+readIt]
		}
		th.Assert(err == nil, "Unable to read from file")
		totalCount += readIt
	}

	return rtn
}

func (th *TestHelper) CheckSparse(fileA string, fileB string, offset int,
	len int) {

	fdA, err := os.OpenFile(fileA, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileA for RDONLY")
	defer fdA.Close()

	fdB, err := os.OpenFile(fileB, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileB for RDONLY")
	defer fdB.Close()

	statA, err := fdA.Stat()
	th.Assert(err == nil, "Unable to fetch fileA stats")
	statB, err := fdB.Stat()
	th.Assert(err == nil, "Unable to fetch fileB stats")
	th.Assert(statB.Size() == statA.Size(), "file sizes don't match")

	rtnA := make([]byte, len)
	rtnB := make([]byte, len)

	for idx := int64(0); idx+int64(len) < statA.Size(); idx += int64(offset) {
		var readA int
		for readA < len {
			readIt, err := fdA.ReadAt(rtnA[readA:], idx+int64(readA))

			if err == io.EOF {
				return
			}
			th.Assert(err == nil,
				"Error while reading from fileA at %d", idx)
			readA += readIt
		}

		var readB int
		for readB < len {
			readIt, err := fdB.ReadAt(rtnB[readB:], idx+int64(readB))

			if err == io.EOF {
				return
			}
			th.Assert(err == nil,
				"Error while reading from fileB at %d", idx)
			readB += readIt
		}
		th.Assert(bytes.Equal(rtnA, rtnB), "data mismatch, %v vs %v",
			rtnA, rtnB)
	}
}

func (th *TestHelper) CheckZeroSparse(fileA string, offset int) {

	fdA, err := os.OpenFile(fileA, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileA for RDONLY")
	defer fdA.Close()

	statA, err := fdA.Stat()
	th.Assert(err == nil, "Unable to fetch fileA stats")

	rtnA := make([]byte, 1)
	for idx := int64(0); idx < statA.Size(); idx += int64(offset) {
		_, err := fdA.ReadAt(rtnA, idx)

		if err == io.EOF {
			return
		}
		th.Assert(err == nil,
			"Error while reading from fileA at %d", idx)

		th.Assert(bytes.Equal(rtnA, []byte{0}), "file %s not zeroed",
			fileA)
	}
}

// Repeatedly check the condition by calling the function until that function returns
// true.
//
// No timeout is provided beyond the normal test timeout.
func (th *TestHelper) WaitFor(description string, condition func() bool) {
	th.Log("Started waiting for %s", description)
	for {
		if condition() {
			th.Log("Finished waiting for %s", description)
			return
		} else {
			th.Log("Condition not satisfied")
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// Log using the logger in TestHelper
func (th *TestHelper) Log(format string, args ...interface{}) error {
	th.T.Logf(th.TestName+": "+format, args...)
	th.Logger.Log(qlog.LogTest, qlog.TestReqId, 1,
		"[%s] "+format, append([]interface{}{th.TestName},
			args...)...)

	return nil
}
