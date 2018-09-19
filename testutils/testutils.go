// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// testutils package supplies a general purpose test framework and
// useful helpers.
package testutils

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
)

type TestArg interface {
	Execute(test QuantumFsTest)
}

type QuantumFsTest func(test TestArg)

type LogscanError struct {
	LogFile           string
	ShouldFailLogscan bool
	TestName          string
}

type TestHelper struct {
	T                 *testing.T
	TestName          string
	CachePath         string
	Logger            *qlog.Qlog
	ExpectedErrors    map[string]struct{}
	TempDir           string
	TestResult        chan string
	Failed            chan struct{}
	StartTime         time.Time
	ShouldFail        bool
	ShouldFailLogscan bool
	Timeout           time.Duration
	Done              chan bool
}

// TestName returns name of the test by looking
// back through the call-stack. The testNameDepth
// argument refers to the stack depth from
// caller's (caller of TestName) frame to the frame
// of the test function
func TestName(testNameDepth int) string {
	// The +1 to the depth accounts for testutils.TestName
	// function on the stack.
	testPc, _, _, _ := runtime.Caller(testNameDepth + 1)
	testName := runtime.FuncForPC(testPc).Name()
	lastSlash := strings.LastIndex(testName, "/")
	testName = testName[lastSlash+1:]
	return testName
}

//NoStdOut prints nothing to stdout
func NoStdOut(format string, args ...interface{}) error {
	return nil
}

// NewTestHelper creates a TestHelper with reasonable
// defaults
func NewTestHelper(testName string, testRunDir string,
	t *testing.T) TestHelper {

	cachePath := testRunDir + "/" + testName

	printer := NoStdOut
	if testing.Verbose() {
		printer = qlog.PrintToStdout
	}

	logger, err := qlog.NewQlogExt(cachePath+"/ramfs",
		60*10000*24, "noVersion", printer)
	utils.AssertNoErr(err)

	if testing.Verbose() {
		logger.SetLogLevels("*/*")
	}

	return TestHelper{
		T:          t,
		TestName:   testName,
		TestResult: make(chan string, 0),
		Failed:     make(chan struct{}, 0),
		StartTime:  time.Now(),
		CachePath:  cachePath,
		Logger:     logger,
		TempDir:    TestRunDir + "/" + testName,
		Timeout:    1500 * time.Millisecond,
		Done:       make(chan bool),
	}
}

var singleton utils.AlternatingLocker

func AlternatingLocker() *utils.AlternatingLocker {
	return &singleton
}

func (th *TestHelper) RunTestCommonEpilog(testName string, testArg QuantumFsTest) {
	th.RunDaemonTestCommonEpilog(testName, testArg, nil, nil)
}

func (th *TestHelper) RunDaemonTestCommonEpilog(testName string,
	testArg QuantumFsTest, startChan <-chan struct{}, cleanup func()) {

	// Wait for any new tests starting a new server to finish starting up
	defer AlternatingLocker().RLock().RUnlock()
	th.Log("Finished test preamble, starting test proper")

	beforeTest := time.Now()

	go th.Execute(testArg)

	testResult := th.WaitForResult(startChan)
	failed := false
	defer func() {
		if failed {
			cleanup()
		}
	}()

	// Record how long the test took so we can make a histogram
	afterTest := time.Now()
	TimeMutex.Lock()
	TimeBuckets = append(TimeBuckets,
		TimeData{
			Duration: afterTest.Sub(beforeTest),
			TestName: testName,
		})
	TimeMutex.Unlock()

	if !th.ShouldFail && testResult != "" {
		th.Log("ERROR: Test failed unexpectedly:\n%s\n", testResult)
		failed = true
	} else if th.ShouldFail && testResult == "" {
		th.Log("ERROR: Test is expected to fail, but didn't")
		failed = true
	}
}

// Assert the condition is true. If it is not true then fail the test with the given
// message
func (th *TestHelper) Assert(condition bool, format string, args ...interface{}) {
	utils.Assert(condition, format, args...)
}

// A lot of times you're trying to do a test and you get error codes. The errors
// often describe the problem better than any th.Assert message, so use them
func (th *TestHelper) AssertNoErr(err error) {
	utils.AssertNoErr(err)
}

func (th *TestHelper) AssertErr(err error) {
	th.Assert(err != nil, "An error was expected")
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

		select {
		case th.TestResult <- result:
		case <-th.Failed:
		}
	}(th)

	test(th)
}

func (th *TestHelper) EndTest() {
	exception := recover()

	if th.TempDir != "" {
		time.Sleep(1 * time.Second)

		if testFailed := th.Logscan(); !testFailed {
			if err := os.RemoveAll(th.TempDir); err != nil {
				th.T.Fatalf("Failed to cleanup temporary mount "+
					"point: %v", err)
			}
		}
	} else {
		th.T.Fatalf("No temporary directory available for logs")
	}

	if exception != nil {
		th.T.Fatalf("Test failed with exception: %v", exception)
	}
}

func (th *TestHelper) WaitForResult(startChan <-chan struct{}) (testResult string) {
	defer func() {
		if testResult != "" {
			close(th.Failed)
		}
	}()
	if startChan != nil {
		select {
		case <-time.After(3 * time.Second):
			testResult = "ERROR: Timed out starting the qfs server"
			return
		case <-startChan:
		}
	}
	select {
	case <-time.After(th.Timeout):
		// Send a signal to any WaitFors
		close(th.Done)

		// WaitFors have a little extra time to affect the result
		select {
		case <-time.After(100 * time.Millisecond):
			// This will only happen on a real hard timeout
			testResult = "ERROR: Timed out running the test"
		case testResult = <-th.TestResult:
		}

	case testResult = <-th.TestResult:
	}
	return
}

var TestRunDir string

func init() {
	syscall.Umask(0)
	TestRunDir = SetupTestspace("quantumfsTest")
}

type TLA struct {
	MustContain bool   // Whether the log must or must not contain the text
	Text        string // text which must or must not be in the log
	FailMsg     string // Message to fail with
}

func (th *TestHelper) CountLogStrings(text string) int {
	contains := th.messagesInTestLog([]TLA{TLA{true, text, "unable to get log"}})
	return contains[0]
}

// Assert the test log contains the given text
// N.B. This is an expensive call as it parses all of the logs every 20ms
// Try minimizing the usage of this function.
func (th *TestHelper) WaitForNLogStrings(text string, n int, failMsg string) {
	th.WaitFor(failMsg, func() bool {
		contains := th.messagesInTestLog([]TLA{TLA{true, text, failMsg}})

		return contains[0] >= n
	})
	th.AssertTestLog([]TLA{TLA{true, text, failMsg}})
}

// Assert the test log contains the given text
func (th *TestHelper) WaitForLogString(text string, failMsg string) {
	th.WaitForNLogStrings(text, 1, failMsg)
}

// Assert the test log doesn't contain the given text
func (th *TestHelper) AssertLogDoesNotContain(text string, failMsg string) {
	th.AssertTestLog([]TLA{TLA{false, text, failMsg}})
}

func (th *TestHelper) AssertTestLog(logs []TLA) {
	contains := th.messagesInTestLog(logs)

	for i, tla := range logs {
		th.Assert(contains[i] > 0 == tla.MustContain, tla.FailMsg)
	}
}

func (th *TestHelper) messagesInTestLog(logs []TLA) []int {
	err := th.Logger.Sync()
	th.Assert(err == 0, "Sync failed with errno %d", err)

	logFile := th.TempDir + "/ramfs/qlog"
	logLines, parseErr := qlog.ParseLogsRaw(logFile)
	utils.AssertNoErr(parseErr)

	nLines := 0
	nFound := make([]int, len(logs))

	for _, rawlog := range logLines {
		nLines++
		logOutput := rawlog.String()
		for idx, tla := range logs {
			exists := strings.Contains(logOutput, tla.Text)
			if exists {
				nFound[idx] += 1
			}
		}
	}
	th.Log("Inspected %d log lines looking for patterns", nLines)

	return nFound
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
		if contains[i] > 0 != tla.MustContain {
			return false
		}
	}
	return true
}

func (th *TestHelper) ShutdownLogger() error {
	return th.Logger.Close()
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

func writeToFile(file *os.File, data string) error {
	written := 0
	for written < len(data) {
		writeIt, err := file.Write([]byte(data[written:]))
		written += writeIt
		if err != nil {
			return errors.New("Unable to write all data")
		}
	}
	return nil
}

func OverWriteFile(filename string, data string) error {
	file, err := os.OpenFile(filename, os.O_RDWR, 0777)
	if file == nil || err != nil {
		return err
	}
	defer file.Close()
	return writeToFile(file, data)
}

func PrintToFile(filename string, data string) error {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0777)
	if file == nil || err != nil {
		return err
	}
	defer file.Close()
	return writeToFile(file, data)
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

// Repeatedly check the condition by calling the function until that function returns
// true.
//
// Wait for most of the test timeout before asserting - so that we can actually tell
// which WaitFor is triggering in more complicated tests
func (th *TestHelper) WaitFor(description string, condition func() bool) {
	th.Log("Started waiting for %s", description)

	for {
		select {
		case <-th.Done:
			th.Assert(false, "WaitFor '%s' failed.", description)
		case <-time.After(20 * time.Millisecond):
		}

		if condition() {
			th.Log("Finished waiting for %s", description)
			return
		}
		th.Log("Condition not satisfied")
	}
}

// Log using the logger in TestHelper
func (th *TestHelper) Log(format string, args ...interface{}) error {
	if th.T != nil {
		th.T.Logf(th.TestName+": "+format, args...)
	}
	th.Logger.Log(qlog.LogTest, qlog.TestReqId, 1,
		"[%s] "+format, append([]interface{}{th.TestName},
			args...)...)

	return nil
}

var ErrorMutex sync.Mutex
var ErrorLogs []LogscanError

// Check the test output for errors
func (th *TestHelper) Logscan() (foundErrors bool) {
	// Check the format string map for the log first to speed this up
	logFile := th.TempDir + "/ramfs/qlog"
	errorsPresent := qlog.LogscanSkim(logFile, th.ExpectedErrors)

	// Nothing went wrong if either we should fail and there were errors,
	// or we shouldn't fail and there weren't errors
	if th.ShouldFailLogscan == errorsPresent {
		return false
	}

	// There was a problem
	ErrorMutex.Lock()
	ErrorLogs = append(ErrorLogs, LogscanError{
		LogFile:           logFile,
		ShouldFailLogscan: th.ShouldFailLogscan,
		TestName:          th.TestName,
	})
	ErrorMutex.Unlock()

	if !th.ShouldFailLogscan {
		th.T.Fatalf("Test FAILED due to FATAL messages\n")
	} else {
		th.T.Fatalf("Test FAILED due to missing FATAL messages\n")
	}

	return true
}

type SetDefaultUidGids struct {
	originalSupplementaryGroups []int
	test                        *TestHelper
}

func (sdug SetDefaultUidGids) Revert() {
	// Set the UID and GID back to the defaults
	defer runtime.UnlockOSThread()

	// Test always runs as root, so its euid and egid is 0
	err1 := syscall.Setreuid(-1, 0)
	err2 := syscall.Setregid(-1, 0)

	sdug.test.Assert(err1 == nil, "Failed to set test EGID back to 0: %v", err1)
	sdug.test.Assert(err2 == nil, "Failed to set test EUID back to 0: %v", err2)

	syscall.Setgroups(sdug.originalSupplementaryGroups)
}

// Change the UID/GID the test thread to the given values. Use -1 not to change
// either the UID or GID. nil sets an empty supplementaryGid set.
//
// Use this like "defer test.SetUidGid(...).Revert()".
func (th *TestHelper) SetUidGid(uid int, gid int,
	supplementaryGids []int) SetDefaultUidGids {

	// The quantumfs tests are run as root because some tests require
	// root privileges. However, root can read or write any file
	// irrespective of the file permissions. Obviously if we want to
	// test permissions then we cannot run as root.
	//
	// To accomplish this we lock this goroutine to a particular OS
	// thread, then we change the EUID of that thread to something which
	// isn't root. Finally at the end we need to restore the EUID of the
	// thread before unlocking ourselves from that thread. If we do not
	// follow this precise cleanup order other tests or goroutines may
	// run using the other UID incorrectly.
	runtime.LockOSThread()

	oldGroups, err := syscall.Getgroups()
	th.AssertNoErr(err)

	if supplementaryGids == nil {
		supplementaryGids = []int{}
	}

	err = syscall.Setgroups(supplementaryGids)
	th.AssertNoErr(err)

	if gid != -1 {
		err := syscall.Setregid(-1, gid)
		if err != nil {
			runtime.UnlockOSThread()
		}
		th.Assert(err == nil, "Failed to change test EGID: %v", err)
	}

	if uid != -1 {
		err := syscall.Setreuid(-1, uid)
		if err != nil {
			syscall.Setregid(-1, 0)
			runtime.UnlockOSThread()
		}
		th.Assert(err == nil, "Failed to change test EUID: %v", err)
	}

	return SetDefaultUidGids{
		originalSupplementaryGroups: oldGroups,
		test:                        th,
	}
}

func ShowSummary() {
	ErrorMutex.Lock()
	fullLogs := make(chan string, len(ErrorLogs))
	var logProcessing sync.WaitGroup
	for i := 0; i < len(ErrorLogs); i++ {
		logProcessing.Add(1)
		go func(i int) {
			defer logProcessing.Done()
			testSummary :=
				OutputLogError(ErrorLogs[i])
			fullLogs <- testSummary
		}(i)
	}

	logProcessing.Wait()
	close(fullLogs)
	testSummary := ""
	for summary := range fullLogs {
		testSummary += summary
	}
	outputTimeGraph := strings.Contains(testSummary, "Timed out")
	ErrorMutex.Unlock()
	fmt.Println("------ Test Summary:\n" + testSummary)

	if outputTimeGraph {
		OutputTimeHistogram()
	}
}

func OutputLogError(errInfo LogscanError) (summary string) {
	errors := make([]string, 0, 10)
	testOutputRaw, err := qlog.ParseLogsRaw(errInfo.LogFile)
	utils.AssertNoErr(err)
	sort.Sort(qlog.SortByTimePtr(testOutputRaw))

	var buffer bytes.Buffer

	extraLines := 0
	for _, rawLine := range testOutputRaw {
		line := rawLine.String()
		buffer.WriteString(line)

		if strings.Contains(line, "PANIC") ||
			strings.Contains(line, "WARN") ||
			strings.Contains(line, "ERROR") {
			extraLines = 2
		}

		// Output a couple extra lines after an ERROR
		if extraLines > 0 {
			// ensure a single line isn't ridiculously long
			if len(line) > 255 {
				line = line[:255] + "...TRUNCATED"
			}

			errors = append(errors, line)
			extraLines--
		}
	}

	if !errInfo.ShouldFailLogscan {
		fmt.Printf("Test %s FAILED due to ERROR. Dumping Logs:\n%s\n"+
			"--- Test %s FAILED\n\n\n", errInfo.TestName,
			buffer.String(), errInfo.TestName)
		return fmt.Sprintf("--- Test %s FAILED due to errors:\n%s\n",
			errInfo.TestName, strings.Join(errors, "\n"))
	}
	fmt.Printf("Test %s FAILED due to missing FATAL messages."+
		" Dumping Logs:\n%s\n--- Test %s FAILED\n\n\n",
		errInfo.TestName, buffer.String(), errInfo.TestName)
	return fmt.Sprintf("--- Test %s FAILED\nExpected errors, but found"+
		" none.\n", errInfo.TestName)
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

func PreTestRuns() {
	ErrorLogs = make([]LogscanError, 0)
}

func PostTestRuns() {
	ShowSummary()
	os.RemoveAll(TestRunDir)
}
