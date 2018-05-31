// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// Test the logging subsystem

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"sort"
	"strconv"
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

func TestPatternSmall(t *testing.T) {
	dataA := make([]LogOutput, 8, 8)
	dataA[0].Format = "C-in\n"
	dataA[1].Format = "A-in\n"
	dataA[2].Format = "A-in\n"
	dataA[3].Format = "A-in\n"
	dataA[4].Format = "A-out\n"
	dataA[5].Format = "A-out\n"
	dataA[6].Format = "A-out\n"
	dataA[7].Format = "C-out\n"

	pattern := make([]LogOutput, 2)
	pattern[0].Format = "A-in\n"
	pattern[1].Format = "A-out\n"

	if PatternMatches(pattern, []bool{}, dataA[:7]) {
		t.Fatalf("Unwildcarded sequence allows wildcards at beginning")
	}

	if PatternMatches(pattern, []bool{}, dataA[1:]) {
		t.Fatalf("Unwildcarded sequence allows wildcards at end")
	}

	if PatternMatches(pattern, []bool{}, dataA) {
		t.Fatalf("Unwildcarded sequence somehow matches when substring")
	}

	if PatternMatches(pattern, []bool{}, dataA[1:7]) {
		t.Fatalf("Unwildcarded sequence allowing internal wildcards")
	}

	if !PatternMatches(pattern, []bool{}, dataA[3:5]) {
		t.Fatalf("Regular matching not working")
	}

	if PatternMatches(pattern, []bool{}, dataA[3:6]) {
		t.Fatalf("Similar string longer data matches against short pattern")
	}

	if PatternMatches(pattern, []bool{}, dataA[2:5]) {
		t.Fatalf("Similar string matches against short pattern with prefix")
	}
}

func TestRealWild(t *testing.T) {
	data := make([]LogOutput, 12, 12)
	data[0].Format = "---In Directory::dirty"
	data[1].Format = "---In Directory::dirtyChild"
	data[2].Format = "---In Directory::dirty"
	data[3].Format = "---In Directory::dirtyChild"
	data[4].Format = "---In Directory::dirty"
	data[5].Format = "---In Directory::dirtyChild"
	data[6].Format = "Out-- Directory::dirtyChild"
	data[7].Format = "Out-- Directory::dirty"
	data[8].Format = "Out-- Directory::dirtyChild"
	data[9].Format = "Out-- Directory::dirty"
	data[10].Format = "Out-- Directory::dirtyChild"
	data[11].Format = "Out-- Directory::dirty"

	wildcards := make([]bool, 12, 12)
	for i := 1; i <= 10; i++ {
		wildcards[i] = true
	}

	if !PatternMatches(data, wildcards, data) {
		t.Fatalf("Sequence doesn't match self with wildcards")
	}

	if !PatternMatches(data, []bool{}, data) {
		t.Fatalf("Sequence doesn't match self")
	}
}

func TestReal(t *testing.T) {
	data := make([]LogOutput, 16, 16)
	data[0].Format = "---In Directory::delChild_\n"
	data[1].Format = "Unlinking inode %d\n"
	data[2].Format = "---In Directory::updateSize_\n"
	data[3].Format = "---In Directory::setChildAttr\n"
	data[4].Format = "Size now %d\n"
	data[5].Format = "---In Directory::dirty\n"
	data[6].Format = "---In Directory::dirtyChild\n"
	data[7].Format = "---In Directory::dirty\n"
	data[8].Format = "---In Directory::dirtyChild\n"
	data[9].Format = "Out-- Directory::dirtyChild\n"
	data[10].Format = "Out-- Directory::dirty\n"
	data[11].Format = "Out-- Directory::dirtyChild\n"
	data[12].Format = "Out-- Directory::dirty\n"
	data[13].Format = "Out-- Directory::setChildAttr\n"
	data[14].Format = "Out-- Directory::updateSize_\n"
	data[15].Format = "Out-- Directory::delChild_\n"

	wildcards := make([]bool, 16, 16)
	wildcards[2] = true
	wildcards[6] = true
	wildcards[7] = true
	wildcards[8] = true
	wildcards[9] = true
	wildcards[10] = true
	wildcards[14] = true

	if !PatternMatches(data, []bool{}, data) {
		t.Fatalf("Sequence doesn't match self")
	}

	if !PatternMatches(data, wildcards, data) {
		t.Fatalf("Sequence doesn't match self with wildcards")
	}
}

func TestPatternSubSeq(t *testing.T) {
	short := make([]LogOutput, 3, 3)
	short[0].Format = "A-in\n"
	short[1].Format = "Bline\n"
	short[2].Format = "A-out\n"
	wildcards := make([]bool, 5, 5)
	wildcards[1] = true
	wildcards[2] = true
	wildcards[3] = true

	long := make([]LogOutput, 5, 5)
	long[0].Format = "A-in\n"
	long[1].Format = "C-in\n"
	long[2].Format = "Dline\n"
	long[3].Format = "C-out\n"
	long[4].Format = "A-out\n"

	if !PatternMatches(short, wildcards[:2], long) {
		t.Fatalf("Wildcard of shorter pattern doesn't match longer data")
	}

	if PatternMatches(short, []bool{}, long) {
		t.Fatalf("Wildcard matched too much")
	}

	if !PatternMatches(long, wildcards, short) {
		t.Fatalf("Unable to match shorter data against longer pattern")
	}

	if !PatternMatches(long, wildcards, long) {
		t.Fatalf("Unable to match against self with wildcards")
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

func TestFileReload(t *testing.T) {
	file, err := ioutil.TempFile("", "reload")
	if err != nil {
		t.Fatalf("Unable to create temporary file")
	}

	const seqLen = 13
	data := make([]PatternData, defaultChunkSize*300, defaultChunkSize*300)
	for i := 0; i < len(data); i++ {
		data[i].SeqStrRaw = "Pattern" + strconv.Itoa(i)
		data[i].Wildcards = make([]bool, seqLen, seqLen)
		for j := i; j < i+seqLen; j++ {
			var newTimeData TimeData
			newTimeData.Delta = int64(12345 + j)
			newTimeData.StartTime = int64(j)
			newTimeData.LogIdxLoc = i
			data[i].Data.Times = append(data[i].Data.Times, newTimeData)
			var newLog LogOutput
			newLog.Subsystem = LogDaemon
			newLog.ReqId = uint64(i)
			newLog.T = int64(i * j)
			newLog.Format = strconv.Itoa(j)
			newLog.Args = []interface{}{i, j}
			data[i].Data.Seq = append(data[i].Data.Seq, newLog)
			if j%2 == 0 {
				data[i].Wildcards[j-i] = true
			}
		}
		data[i].Avg = int64(i * seqLen)
		data[i].Sum = int64(i * seqLen * seqLen)
		data[i].Stddev = int64(seqLen - i)
	}

	SaveToStat(file, data)
	_, err = file.Seek(0, 0)
	if err != nil {
		t.Fatalf("Unable to seek to beginning of temporary file")
	}
	loaded := LoadFromStat(file)

	for i := 0; i < len(data); i++ {
		if i >= len(loaded) {
			t.Fatalf("Loaded is missing entries from data")
		}

		matchFn := func() bool {
			if data[i].SeqStrRaw != loaded[i].SeqStrRaw ||
				data[i].Avg != loaded[i].Avg ||
				data[i].Sum != loaded[i].Sum ||
				data[i].Stddev != loaded[i].Stddev {

				return false

			}

			if len(data[i].Data.Times) != len(loaded[i].Data.Times) ||
				len(data[i].Data.Seq) != len(loaded[i].Data.Seq) ||
				len(data[i].Wildcards) != len(loaded[i].Wildcards) {

				return false
			}

			for j := 0; j < len(data[i].Data.Times); j++ {
				if data[i].Data.Times[j] != loaded[i].Data.Times[j] {
					return false
				}
			}

			for j := 0; j < len(data[i].Data.Seq); j++ {
				if reflect.DeepEqual(data[i].Data.Seq[j],
					loaded[i].Data.Seq[j]) == false {

					return false
				}
			}

			for j := 0; j < len(data[i].Wildcards); j++ {
				if data[i].Wildcards[j] != loaded[i].Wildcards[j] {
					return false
				}
			}

			return true
		}

		if matchFn() == false {
			t.Fatalf("Save/Load data mismatch at %d. %v vs %v\n", i,
				data[i], loaded[i])
		}
	}

	if len(loaded) != len(data) {
		t.Fatalf("Loaded has more entries than in original data")
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
	sort.Sort(SortString(logOutLines))

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

/*
func TestBooleanLogType(t *testing.T) {
	runTest(t, func(test *testHelper) {

		test.qfs.c.wlog("booleans %t %t", true, false)
		testLogs := test.parseLogs()
		test.Assert(strings.Contains(testLogs, "booleans true false"),
			"boolean log types incorrectly output: %s", testLogs)
	})
}

func TestQlogWrapAround(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		test.ShouldFailLogscan = true

		// Overflow the qlog file
		for i := 0; i < 100000000; i++ {
			test.Logger.Log(qlog.LogTest, qlog.TestReqId, 3,
				"Filler %s", "12345678901234567890")
		}

		// At this point the qlog file should have wrapped around several
		// times. When the test harness goes to parse it, it will panic if
		// the offset isn't properly adjusted with respect to the file size
		// with a "bounds out of range" failure in readBack().

		// Processing all these logs takes too long. Instead return an error
		// an expect this test to fail. This moves the log parsing outside of
		// the timed portion of the test.
		test.Log("ERROR: Test fill complete")
	})
}
*/
