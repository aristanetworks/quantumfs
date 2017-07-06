// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

import (
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
)

func runReader(qlogFile string,
	extractors []StatExtractorConfig) *processlocal.Memdb {

	db := processlocal.NewMemdb("").(*processlocal.Memdb)
	agg := AggregateLogs(qlog.ReadOnly, qlogFile, db, extractors)

	agg.requestEndAfter = time.Millisecond * 100

	return db
}

func (test *testHelper) runExtractorTest(qlogHandle *qlog.Qlog,
	cfg *StatExtractorConfig, check func(*processlocal.Memdb)) {

	// Setup an extractor
	extractors := []StatExtractorConfig{}
	if cfg != nil {
		extractors = append(extractors, *cfg)
	}

	// Run the reader
	memdb := runReader(test.CachePath+"/ramfs/qlog", extractors)

	test.WaitFor("statistic to register", func() bool {
		if len(memdb.Data) == 0 {
			return false
		}

		if len(memdb.Data[0].Fields) == 0 {
			return false
		}

		// Check if we're too early
		for _, v := range memdb.Data[0].Fields {
			if v.Name == "samples" && v.Data == 0 {
				return false
			}
		}

		// Data should be present now
		check(memdb)

		return true
	})
}

func TestMatches(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.Logger

		// Artificially insert matching logs.
		// The average of 10,000 and 30,000 should be 20,000
		duration1 := int64(10000)
		duration2 := int64(30000)
		qlogHandle.Log_(time.Unix(0, 20000), qlog.LogTest, 12345, 2,
			qlog.FnEnterStr+"TestMatch")
		qlogHandle.Log_(time.Unix(0, 20000+duration1), qlog.LogTest, 12345,
			3, qlog.FnExitStr+"TestMatch")

		qlogHandle.Log_(time.Unix(0, 50000), qlog.LogTest, 12346, 3,
			qlog.FnEnterStr+"TestMatch")
		qlogHandle.Log_(time.Unix(0, 50000+duration2), qlog.LogTest, 12346,
			3, qlog.FnExitStr+"TestMatch")

		// Add in some close, but not actually matching logs
		qlogHandle.Log(qlog.LogTest, 12345, 2, qlog.FnEnterStr+"TestMatchZ")
		qlogHandle.Log(qlog.LogTest, 12345, 3, qlog.FnExitStr+"TestMtch")
		qlogHandle.Log(qlog.LogTest, 12345, 3, "TestMatch")
		qlogHandle.Log(qlog.LogTest, 12347, 3, qlog.FnExitStr+"TestMatch")

		checkedAvg := false
		checkedSamples := false
		checker := func(memdb *processlocal.Memdb) {
			test.Assert(len(memdb.Data[0].Fields) == 7,
				"%d fields produced from one matching log",
				len(memdb.Data[0].Fields))

			for _, v := range memdb.Data[0].Fields {
				if v.Name == "average_ns" {
					test.Assert(v.Data == uint64(duration1+
						duration2)/2, "incorrect delta %d",
						v.Data)
					checkedAvg = true
				} else if v.Name == "samples" {
					test.Assert(v.Data == 2,
						"incorrect samples %d", v.Data)
					checkedSamples = true
				}
			}
		}

		test.runExtractorTest(qlogHandle, NewStatExtractorConfig(
			NewExtPairStats(qlog.FnEnterStr+"TestMatch\n",
				qlog.FnExitStr+"TestMatch\n", true, "TestMatch"),
			(300*time.Millisecond)), checker)

		test.Assert(checkedAvg, "test not checking average")
		test.Assert(checkedSamples, "test not checking samples")
	})
}

func TestPercentiles(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.Logger

		// Artificially insert matching with sensible percentiles
		base := int64(200000)
		// Reverse the order to ensure we test that sorting is working
		for i := int64(100); i >= 0; i-- {
			qlogHandle.Log_(time.Unix(0, base), qlog.LogTest,
				uint64(base), 2, qlog.FnEnterStr+"TestMatch")
			qlogHandle.Log_(time.Unix(0, base+i), qlog.LogTest,
				uint64(base), 2, qlog.FnExitStr+"TestMatch")
			base += int64(i)
		}

		checked := make([]bool, 8)
		checker := func(memdb *processlocal.Memdb) {
			test.Assert(len(memdb.Data[0].Fields) == 7,
				"%d fields produced from one matching log",
				len(memdb.Data[0].Fields))

			for _, v := range memdb.Data[0].Fields {
				if v.Name == "average_ns" {
					test.Assert(v.Data == 50,
						"incorrect delta %d", v.Data)
					checked[0] = true
				} else if v.Name == "maximum_ns" {
					test.Assert(v.Data == 100,
						"incorrect delta %d", v.Data)
					checked[1] = true
				} else if v.Name == "samples" {
					test.Assert(v.Data == 101,
						"incorrect samples %d", v.Data)
					checked[2] = true
				} else if v.Name == "50pct_ns" {
					test.Assert(v.Data == 50,
						"50th percentile is %d", v.Data)
					checked[3] = true
				} else if v.Name == "90pct_ns" {
					test.Assert(v.Data == 90,
						"90th percentile is %d", v.Data)
					checked[4] = true
				} else if v.Name == "95pct_ns" {
					test.Assert(v.Data == 95,
						"95th percentile is %d", v.Data)
					checked[5] = true
				} else if v.Name == "99pct_ns" {
					test.Assert(v.Data == 99,
						"99th percentile is %d", v.Data)
					checked[6] = true
				}
			}

			for k, v := range memdb.Data[0].Tags {
				if k == "version" {
					test.Assert(v == "noVersion",
						"version field not correct: %s", v)
					checked[7] = true
				}
			}
		}

		test.runExtractorTest(qlogHandle, NewStatExtractorConfig(
			NewExtPairStats(qlog.FnEnterStr+"TestMatch\n",
				qlog.FnExitStr+"TestMatch\n", true, "TestMatch"),
			(300*time.Millisecond)), checker)

		for i := 0; i < 7; i++ {
			test.Assert(checked[i], "test not checking field %d", i)
		}
	})
}

func TestPointCount(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.Logger

		for i := int64(0); i < 123; i++ {
			qlogHandle.Log_(time.Unix(0, i), qlog.LogTest,
				uint64(i), 2, "TestLog")
		}

		checked := false
		checker := func(memdb *processlocal.Memdb) {
			test.Assert(len(memdb.Data[0].Fields) == 1,
				"%d fields produced from one matching log",
				len(memdb.Data[0].Fields))

			for _, v := range memdb.Data[0].Fields {
				if v.Name == "samples" {
					test.Assert(v.Data == 123,
						"incorrect samples %d", v.Data)
					checked = true
				}
			}
		}

		test.runExtractorTest(qlogHandle, NewStatExtractorConfig(
			NewExtPointStats("TestLog\n", "TestLog Name Tag"),
			(300*time.Millisecond)), checker)

		test.Assert(checked, "test not checking anything")
	})
}

func TestIndentation(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.Logger

		// Artificially insert matching logs at different scopes
		durationReal := int64(10000)
		qlogHandle.Log_(time.Unix(0, 20000), qlog.LogTest, 12345, 2,
			qlog.FnEnterStr+"TestMatch")

		// Go up in scope and place a matching string there
		qlogHandle.Log_(time.Unix(0, 20100), qlog.LogTest, 12345, 2,
			qlog.FnEnterStr+"Other Function")
		qlogHandle.Log_(time.Unix(0, 20200), qlog.LogTest,
			12345, 3, qlog.FnEnterStr+"Mismatching funcIn")
		qlogHandle.Log_(time.Unix(0, 20300), qlog.LogTest,
			12345, 3, qlog.FnExitStr+"TestMatch")
		qlogHandle.Log_(time.Unix(0, 20400), qlog.LogTest, 12345, 2,
			qlog.FnExitStr+"Other Function")

		qlogHandle.Log_(time.Unix(0, 20000+durationReal), qlog.LogTest,
			12345, 3, qlog.FnExitStr+"TestMatch")

		checkedAvg := false
		checkedSamples := false
		checker := func(memdb *processlocal.Memdb) {
			test.Assert(len(memdb.Data[0].Fields) == 7,
				"%d fields produced from one matching log",
				len(memdb.Data[0].Fields))

			for _, v := range memdb.Data[0].Fields {
				if v.Name == "average_ns" {
					test.Assert(v.Data == uint64(durationReal),
						"incorrect delta %d", v.Data)
					checkedAvg = true
				} else if v.Name == "samples" {
					test.Assert(v.Data == 1,
						"incorrect samples %d", v.Data)
					checkedSamples = true
				}
			}
		}

		test.runExtractorTest(qlogHandle, NewStatExtractorConfig(
			NewExtPairStats(qlog.FnEnterStr+"TestMatch\n",
				qlog.FnExitStr+"TestMatch\n", true, "TestMatch"),
			(300*time.Millisecond)), checker)

		test.Assert(checkedAvg, "test not checking average")
		test.Assert(checkedSamples, "test not checking samples")
	})
}

func TestErrorCounting(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.ShouldFailLogscan = true
		qlogHandle := test.Logger

		// Artificially insert some error logs
		for i := int64(0); i < 123; i++ {
			qlogHandle.Log_(time.Unix(i, 20000+i), qlog.LogTest,
				uint64(i), 2, "ERROR: TestMatch")
		}

		checked := false
		checker := func(memdb *processlocal.Memdb) {
			for _, v := range memdb.Data[0].Fields {
				if v.Name == "SystemErrors" {
					test.Assert(v.Data == 123,
						"incorrect count %d", v.Data)
					checked = true
				}
			}
		}

		test.runExtractorTest(qlogHandle, nil, checker)
		test.Assert(checked, "test not checking count")

	})
}
