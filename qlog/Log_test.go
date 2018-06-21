// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// Test the logging subsystem

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aristanetworks/quantumfs/utils"
)

func TestLogSet(t *testing.T) {
	qlog, err := NewQlog("")
	utils.AssertNoErr(err)
	// let's redirect the log writer in qfs
	var logs string
	qlog.SetWriter(ioPipe(&logs))

	qlog.SetLogLevels("Daemon|2")
	qlog.Log(LogDaemon, MuxReqId, 1, "TestToken1")
	if !strings.Contains(logs, "TestToken1") {
		t.Fatal("Enabled log doesn't show up")
	}
	qlog.Log(LogDaemon, MuxReqId, 0, "TestToken0")
	if strings.Contains(logs, "TestToken0") {
		t.Fatal("Log level 0 not disabled by mask")
	}
	qlog.Log(LogWorkspaceDb, MuxReqId, 0, "TestToken2")
	if !strings.Contains(logs, "TestToken2") {
		t.Fatal("Different subsystem erroneously affected by log setting")
	}

	qlog.SetLogLevels("")
	logs = ""
	for i := 1; i < int(maxLogLevels); i++ {
		qlog.Log(LogDaemon, MuxReqId, uint8(i), "TestToken")
		if strings.Contains(logs, "TestToken") {
			t.Fatal("Disabled log appeared")
		}
	}
	qlog.Log(LogDaemon, MuxReqId, 0, "TestToken")
	if !strings.Contains(logs, "TestToken") {
		t.Fatal("Default log level not working")
	}

	// Test variable arguments
	a := 12345
	b := 98765
	qlog.Log(LogDaemon, MuxReqId, 0, "Testing args %d %d", a, b)
	if !strings.Contains(logs, "12345") ||
		!strings.Contains(logs, "98765") {
		t.Fatal("Variable insertion in logs not working.")
	}
}

func TestLogLevels(t *testing.T) {
	qlog, err := NewQlog("")
	utils.AssertNoErr(err)

	qlog.SetLogLevels("")
	defaultLevels := qlog.LogLevels

	qlog.SetLogLevels("Daemon/*")
	expLevels := (defaultLevels & ^(uint32(0xF))) | uint32(0xF)
	if qlog.LogLevels != expLevels {
		t.Fatalf("Wildcard log levels incorrectly set: %x != %x",
			expLevels, qlog.LogLevels)
	}

	// test out of order, combo setting, and general bitmask
	qlog.SetLogLevels("Daemon/1,WorkspaceDb/*,Datastore|10")
	expLevels = (defaultLevels & ^(uint32(0xFFF))) | uint32(0xFA3)
	if qlog.LogLevels != expLevels {
		t.Fatalf("Out of order, combo setting, or general bitmask broken %x",
			qlog.LogLevels)
	}

	// test misspelling ignores misspelt entry. Ensure case insensitivity
	qlog.SetLogLevels("DaeMAN/1,WORKSPACEDB/*,Datastored|10")
	expLevels = (defaultLevels & ^(uint32(0xFFF))) | uint32(0xF11)
	if qlog.LogLevels != expLevels {
		t.Fatalf("Case insensitivity broken / mis-spelling not ignored %x",
			qlog.LogLevels)
	}
}

func TestLogFnPair(t *testing.T) {
	if !IsLogFnPair(FnEnterStr+"String A1234", FnExitStr+"String A1234") {
		t.Fatalf("Easy string match doesn't match")
	}

	if !IsLogFnPair(FnEnterStr+"String A1234Extra1234",
		FnExitStr+"String A1234") {

		t.Fatal("Extra suffix on first string breaks matching")
	}

	if !IsLogFnPair(FnEnterStr+"String A1234",
		FnExitStr+"String A1234Extra4321") {

		t.Fatal("Extra suffix on second string breaks matching")
	}

	if IsLogFnPair(FnEnterStr+"String A1234", FnEnterStr+"String A1234") {
		t.Fatal("Two enter functions matching")
	}

	if IsLogFnPair(FnExitStr+"String A1234", FnExitStr+"String A1234") {
		t.Fatal("Two exit functions matching")
	}

	if IsLogFnPair(FnEnterStr+"asdlkj234", FnExitStr+"kjl23") {
		t.Fatal("Obvious mismatch matches")
	}

	if !IsLogFnPair("---In Mux::ReleaseDirEnter Fh %d\n",
		"Out-- Mux::ReleaseDir\n") {

		t.Fatal("Real example mismatch")
	}
}

func trimToStr(logs []string, boundary string) []string {
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
	utils.Assert(boundaryCount == 2,
		"Only found %d boundary markers in %d log lines",
		boundaryCount, len(logs))
	return logs[boundaryStart : boundaryEnd+1]
}

func parseLogs(logger *Qlog, tmpDir string) string {
	logger.Sync()
	return ParseLogs(tmpDir + "/qlog")
}

func setupQlog() (logger *Qlog, tmpDir string) {
	var err error
	tmpDir, err = ioutil.TempDir("", "")
	utils.AssertNoErr(err)

	logger, err = NewQlog(tmpDir)
	utils.AssertNoErr(err)

	return logger, tmpDir
}

func genData(num int) []byte {
	rtn := make([]byte, num)
	for i := 0; i < num; i++ {
		rtn[i] = byte(i)
	}

	return rtn
}

func TestMaxStringLast_test(t *testing.T) {
	logger, tmpDir := setupQlog()
	longStr := string(genData(math.MaxUint16))

	logger.Log(LogTest, 0, 1, "%s", longStr)
	logger.Log(LogTest, 0, 3, "Second log to confirm continuity")

	testLogs := parseLogs(logger, tmpDir)
	utils.Assert(strings.Contains(testLogs,
		"Log data exceeds allowable length"),
		"Over length string doesn't trigger warning")
	utils.Assert(strings.Contains(testLogs,
		"Second log to confirm continuity"),
		"Over length string breaks later logs")
}

func wlog(logger *Qlog, format string, args ...interface{}) {
	logger.Log(LogTest, 123, 1, format, args...)
}

func TestQParse(t *testing.T) {
	logger, tmpDir := setupQlog()

	var logOut bytes.Buffer
	var testMutex sync.Mutex
	logger.Write = func(format string, args ...interface{}) error {
		testMutex.Lock()
		logOut.WriteString(fmt.Sprintf(format+"\n", args...))
		testMutex.Unlock()
		return nil
	}
	// Enable *all* logs
	logger.LogLevels = 0
	logger.LogLevels--

	testLogBoundary := "testLogBoundary"
	wlog(logger, testLogBoundary)
	wlog(logger, "Test single log.")
	wlog(logger, "Test with parameter %d", 111)
	wlog(logger, testLogBoundary)

	// Now grab the log file and compare against std out. Since logOut
	// started being appended to a bit late, and we sample it first, it
	// should be a subset of the qparsed logs
	testMutex.Lock()
	logOutCopy := string(logOut.Bytes())
	testMutex.Unlock()

	testLogs := parseLogs(logger, tmpDir)
	// There's nothing ensuring the order is the same, so we have to sort
	testLogLines := strings.Split(testLogs, "\n")
	logOutLines := strings.Split(logOutCopy, "\n")
	sort.Strings(logOutLines)

	// Trim any lines outside of our "comparison boundaries"
	testLogLines = trimToStr(testLogLines, testLogBoundary)
	logOutLines = trimToStr(logOutLines, testLogBoundary)
	utils.Assert(len(testLogLines) == len(logOutLines),
		"log length mismatch %d %d", len(testLogLines),
		len(logOutLines))
	utils.Assert(len(testLogLines) > 2, "trimToStr not working")

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

			utils.Assert(false, "Qparse/stdout mismatch:\n%s\n"+
				" |%v| |%v|", debugStr,
				[]byte(logOutLines[i]),
				[]byte(testLogLines[i]))
		}
	}
}

func TestQParsePartials_test(t *testing.T) {
	logger, tmpDir := setupQlog()

	// Before we enable logs, let's cause all File logs to be
	// partially written
	prefixToBreak := "BREAKTHIS: "
	logger.EnterTestMode(prefixToBreak)

	// Enable *all* logs
	logger.LogLevels = 0
	logger.LogLevels--

	// Generate some logs
	for i := 0; i < 10; i++ {
		wlog(logger, "Log shouldn't be broken")
		wlog(logger, prefixToBreak+"Msc log")
	}

	testLogs := parseLogs(logger, tmpDir)

	testLogLines := strings.Split(testLogs, "\n")
	droppedEntry := false
	count := 0
	// Check to see if we see dropped packets interspersed with good ones
	for i := 0; i < len(testLogLines); i++ {
		utils.Assert(len(testLogLines[i]) < 6 ||
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

	utils.Assert(count >= 5,
		"Unable to confidently prove partial packet reading")
}

func TestBooleanLogType(t *testing.T) {
	logger, tmpDir := setupQlog()

	wlog(logger, "booleans %t %t", true, false)
	testLogs := parseLogs(logger, tmpDir)
	utils.Assert(strings.Contains(testLogs, "booleans true false"),
		"boolean log types incorrectly output: %s", testLogs)
}

func TestPartialQlogHeader(t *testing.T) {
	logger, tmpDir := setupQlog()
	wlog(logger, "This is a log")

	os.Truncate(tmpDir+"/qlog", 100)
	// This will panic if a truncated qlog header isn't handled
	logs := parseLogs(logger, tmpDir)
	utils.Assert(strings.Contains(logs, "Qlog version incompatible"),
		"Readable header from bad qlog")

	os.Truncate(tmpDir+"/qlog", 4)
	logs = parseLogs(logger, tmpDir)
	utils.Assert(strings.Contains(logs, "Qlog version incompatible"),
		"Readable header from bad qlog")
}

func TestQlogWrapAround(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	utils.AssertNoErr(err)

	logger, err := NewQlogExt(tmpDir, 10000+MmapStrMapSize, "noVersion",
		PrintToStdout)
	utils.AssertNoErr(err)

	// Overflow the qlog file
	for i := 0; i < 10000; i++ {
		logger.Log(LogTest, TestReqId, 3,
			"Filler %s", "12345678901234567890")
	}

	// At this point the qlog file should have wrapped around several
	// times. When the test harness goes to parse it, it will panic if
	// the offset isn't properly adjusted with respect to the file size
	// with a "bounds out of range" failure in readBack().
	// If we can parse the logs then this passed.
	parseLogs(logger, tmpDir)
}
