// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

// Test the logging subsystem

import (
	"io/ioutil"
	"reflect"
	"strconv"
	"testing"

	"github.com/aristanetworks/quantumfs/qlog"
)

func TestPatternSmall(t *testing.T) {
	dataA := make([]qlog.LogOutput, 8, 8)
	dataA[0].Format = "C-in\n"
	dataA[1].Format = "A-in\n"
	dataA[2].Format = "A-in\n"
	dataA[3].Format = "A-in\n"
	dataA[4].Format = "A-out\n"
	dataA[5].Format = "A-out\n"
	dataA[6].Format = "A-out\n"
	dataA[7].Format = "C-out\n"

	pattern := make([]qlog.LogOutput, 2)
	pattern[0].Format = "A-in\n"
	pattern[1].Format = "A-out\n"

	if patternMatches(pattern, []bool{}, dataA[:7]) {
		t.Fatalf("Unwildcarded sequence allows wildcards at beginning")
	}

	if patternMatches(pattern, []bool{}, dataA[1:]) {
		t.Fatalf("Unwildcarded sequence allows wildcards at end")
	}

	if patternMatches(pattern, []bool{}, dataA) {
		t.Fatalf("Unwildcarded sequence somehow matches when substring")
	}

	if patternMatches(pattern, []bool{}, dataA[1:7]) {
		t.Fatalf("Unwildcarded sequence allowing internal wildcards")
	}

	if !patternMatches(pattern, []bool{}, dataA[3:5]) {
		t.Fatalf("Regular matching not working")
	}

	if patternMatches(pattern, []bool{}, dataA[3:6]) {
		t.Fatalf("Similar string longer data matches against short pattern")
	}

	if patternMatches(pattern, []bool{}, dataA[2:5]) {
		t.Fatalf("Similar string matches against short pattern with prefix")
	}
}

func TestRealWild(t *testing.T) {
	data := make([]qlog.LogOutput, 12, 12)
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

	if !patternMatches(data, wildcards, data) {
		t.Fatalf("Sequence doesn't match self with wildcards")
	}

	if !patternMatches(data, []bool{}, data) {
		t.Fatalf("Sequence doesn't match self")
	}
}

func TestReal(t *testing.T) {
	data := make([]qlog.LogOutput, 16, 16)
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

	if !patternMatches(data, []bool{}, data) {
		t.Fatalf("Sequence doesn't match self")
	}

	if !patternMatches(data, wildcards, data) {
		t.Fatalf("Sequence doesn't match self with wildcards")
	}
}

func TestPatternSubSeq(t *testing.T) {
	short := make([]qlog.LogOutput, 3, 3)
	short[0].Format = "A-in\n"
	short[1].Format = "Bline\n"
	short[2].Format = "A-out\n"
	wildcards := make([]bool, 5, 5)
	wildcards[1] = true
	wildcards[2] = true
	wildcards[3] = true

	long := make([]qlog.LogOutput, 5, 5)
	long[0].Format = "A-in\n"
	long[1].Format = "C-in\n"
	long[2].Format = "Dline\n"
	long[3].Format = "C-out\n"
	long[4].Format = "A-out\n"

	if !patternMatches(short, wildcards[:2], long) {
		t.Fatalf("Wildcard of shorter pattern doesn't match longer data")
	}

	if patternMatches(short, []bool{}, long) {
		t.Fatalf("Wildcard matched too much")
	}

	if !patternMatches(long, wildcards, short) {
		t.Fatalf("Unable to match shorter data against longer pattern")
	}

	if !patternMatches(long, wildcards, long) {
		t.Fatalf("Unable to match against self with wildcards")
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
			var newLog qlog.LogOutput
			newLog.Subsystem = qlog.LogDaemon
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

