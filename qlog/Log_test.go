// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlog

// Test the logging subsystem

import "io/ioutil"
import "reflect"
import "strconv"
import "strings"
import "testing"
import "github.com/aristanetworks/quantumfs/utils"

func TestLogSet(t *testing.T) {
	qlog := NewQlogTiny()
	// let's redirect the log writer in qfs
	var logs string
	qlog.SetWriter(utils.IoPipe(&logs))

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

func TestLoadLevels(t *testing.T) {
	qlog := NewQlogTiny()

	qlog.SetLogLevels("Daemon/*")
	if qlog.LogLevels != 0x1111F {
		t.Fatalf("Wildcard log levels incorrectly set: %x != %x", 0x1111f,
			qlog.LogLevels)
	}

	// test out of order, combo setting, and general bitmask
	qlog.SetLogLevels("Daemon/1,WorkspaceDb/*,Datastore|10")
	if qlog.LogLevels != 0x11FA3 {
		t.Fatalf("Out of order, combo setting, or general bitmask broken %x",
			qlog.LogLevels)
	}

	// test misspelling ignores misspelt entry. Ensure case insensitivity
	qlog.SetLogLevels("DaeMAN/1,WORKSPACEDB/*,Datastored|10")
	if qlog.LogLevels != 0x11F11 {
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

	if !IsLogFnPair("---In Mux::ReleaseDirEnter Fh: %d\n",
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
