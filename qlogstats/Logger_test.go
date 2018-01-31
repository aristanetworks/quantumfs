// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

import (
	"fmt"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
)

func runReader(qlogFile string,
	extractors []StatExtractor) *processlocal.Memdb {

	db := processlocal.NewMemdb("").(*processlocal.Memdb)
	AggregateLogs(qlog.ReadOnly, qlogFile, db, extractors, 100*time.Millisecond)

	return db
}

func (test *testHelper) runExtractorTest(qlogHandle *qlog.Qlog,
	cfg StatExtractor, check func(*processlocal.Memdb)) {

	// Setup an extractor
	extractors := []StatExtractor{}
	if cfg != nil {
		extractors = append(extractors, cfg)
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
		data, exists := memdb.Data[0].Fields["samples"]
		if !exists || data.(int64) == 0 {
			return false
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

			if data, ok := memdb.Data[0].Fields["average_ns"]; ok {
				test.Assert(data.(int64) == int64(duration1+
					duration2)/2, "incorrect delta %d", data)
				checkedAvg = true
			}

			if data, ok := memdb.Data[0].Fields["samples"]; ok {
				test.Assert(data.(int64) == 2,
					"incorrect samples %d", data)
				checkedSamples = true
			}
		}

		test.runExtractorTest(qlogHandle,
			NewExtPairStats(qlog.FnEnterStr+"TestMatch",
				qlog.FnExitStr+"TestMatch", "TestMatch"), checker)

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

			if data, ok := memdb.Data[0].Fields["average_ns"]; ok {
				test.Assert(data.(int64) == 50,
					"incorrect delta %d", data)
				checked[0] = true
			}

			if data, ok := memdb.Data[0].Fields["maximum_ns"]; ok {
				test.Assert(data.(int64) == 100,
					"incorrect delta %d", data)
				checked[1] = true
			}

			if data, ok := memdb.Data[0].Fields["samples"]; ok {
				test.Assert(data.(int64) == 101,
					"incorrect samples %d", data)
				checked[2] = true
			}

			if data, ok := memdb.Data[0].Fields["50pct_ns"]; ok {
				test.Assert(data.(int64) == 50,
					"50th percentile is %d", data)
				checked[3] = true
			}

			if data, ok := memdb.Data[0].Fields["90pct_ns"]; ok {
				test.Assert(data.(int64) == 90,
					"90th percentile is %d", data)
				checked[4] = true
			}

			if data, ok := memdb.Data[0].Fields["95pct_ns"]; ok {
				test.Assert(data.(int64) == 95,
					"95th percentile is %d", data)
				checked[5] = true
			}

			if data, ok := memdb.Data[0].Fields["99pct_ns"]; ok {
				test.Assert(data.(int64) == 99,
					"99th percentile is %d", data)
				checked[6] = true
			}

			for k, v := range memdb.Data[0].Tags {
				if k == "version" {
					test.Assert(v == "noVersion",
						"version field not correct: %s", v)
					checked[7] = true
				}
			}
		}

		test.runExtractorTest(qlogHandle,
			NewExtPairStats(qlog.FnEnterStr+"TestMatch",
				qlog.FnExitStr+"TestMatch", "TestMatch"), checker)

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

			if data, ok := memdb.Data[0].Fields["samples"]; ok {
				test.Assert(data.(int64) == 123,
					"incorrect samples %d", data)
				checked = true
			}
		}

		test.runExtractorTest(qlogHandle,
			NewExtPointStats("TestLog", "TestLog Name Tag"), checker)

		test.Assert(checked, "test not checking anything")
	})
}

func TestPartialFormatMatch(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.Logger

		for i := int64(0); i < 123; i++ {
			qlogHandle.Log_(time.Unix(i, 20000+i), qlog.LogTest,
				uint64(i), 2, "ER_OR: TestMatch")
		}

		checked := false
		checker := func(memdb *processlocal.Memdb) {
			if data, ok := memdb.Data[0].Fields["samples"]; ok {
				test.Assert(data.(int64) == 123,
					"incorrect count %d", data)
				checked = true
			}
		}

		test.runExtractorTest(qlogHandle,
			NewExtPointStatsPartialFormat("ER_OR: ", "SystemErrors"),
			checker)
		test.Assert(checked, "test not checking count, %d")
	})
}

func TestPairStatsGC(t *testing.T) {
	runTest(t, func(test *testHelper) {
		statExtractor := NewExtPairStats("Start match", "Stop match",
			"Testlatency")
		ext := statExtractor.(*extPairStats)

		c := ext.Chan()

		msg := &qlog.LogOutput{
			Subsystem: qlog.LogTest,
			ReqId:     1,
			T:         1,
			Format:    "Start match\n",
			Args:      []interface{}{},
		}
		c <- &MessageCommand{
			log: msg,
		}
		test.WaitFor("Request 1 to be started", func() bool {
			return len(ext.requests) == 1
		})

		c <- &GcCommand{}
		test.Assert(len(ext.requests) == 1, "Request 1 deleted early")
		msg = &qlog.LogOutput{
			Subsystem: qlog.LogTest,
			ReqId:     2,
			T:         2,
			Format:    "Start match\n",
			Args:      []interface{}{},
		}
		c <- &MessageCommand{
			log: msg,
		}
		test.WaitFor("Request 2 to be started", func() bool {
			return len(ext.requests) == 2
		})

		c <- &GcCommand{}
		test.Assert(len(ext.requests) == 2, "Request 2 deleted early")

		msg = &qlog.LogOutput{
			Subsystem: qlog.LogTest,
			ReqId:     2,
			T:         3,
			Format:    "Stop match\n",
			Args:      []interface{}{},
		}
		c <- &MessageCommand{
			log: msg,
		}

		test.WaitFor("Request 2 to be deleted", func() bool {
			return len(ext.requests) == 1
		})
		_, exists := ext.requests[1]
		test.Assert(exists, "Request 1 deleted early")

		c <- &GcCommand{}
		test.WaitFor("Request 1 to age out", func() bool {
			return len(ext.requests) == 0
		})
	})
}

func TestExtLogDataExtractor(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.Logger

		testFmt := "DATA POINT %d"

		extractor := NewHistogramFirstParamInt(testFmt, "test_ext", 0, 99,
			10, false)

		// Setup a histogram where each bucket i has 2*i items in it
		dataPoints := int64(0)
		for i := 0; i < 10; i++ {
			for j := 0; j < 2*i; j++ {
				qlogHandle.Log_(time.Unix(100, 100), qlog.LogTest,
					uint64(i), 2, testFmt, (i*10)+1)
				dataPoints++
			}
		}

		// Add a couple points out of bounds
		qlogHandle.Log_(time.Unix(100, 100), qlog.LogTest,
			uint64(101), 2, testFmt, -1)
		qlogHandle.Log_(time.Unix(100, 100), qlog.LogTest,
			uint64(102), 2, testFmt, 1000000)
		dataPoints += 2

		bucketsChecked := 0
		checker := func(memdb *processlocal.Memdb) {
			test.Assert(len(memdb.Data[0].Fields) > 0,
				"Empty database entry")

			// check the fields
			for key, data := range memdb.Data[0].Fields {
				switch key {
				case "samples":
					test.Assert(data.(int64) == dataPoints,
						"incorrect count %d", data)
				case "BeforeHistogram":
					test.Assert(data.(int64) == 1,
						"incorrect BeforeHistogram %d", data)
				case "PastHistogram":
					test.Assert(data.(int64) == 1,
						"incorrect PastHistogram %d", data)
				case "errors":
					test.Assert(data.(int64) == 0,
						"parse errors found")
				default:
					min, _ := histoTagToInts(test, key)
					expected := 2 * (min / 10)
					test.Assert(data.(int64) == expected,
						"incorrect histogram bucket %d %d",
						data, expected)
					bucketsChecked++
				}
			}
		}

		test.runExtractorTest(qlogHandle, extractor, checker)
		test.Assert(bucketsChecked == 10, "Buckets not checked")
	})
}

func histoTagToInts(test *testHelper, tag string) (int64, int64) {
	var bucketMin, bucketMax int64
	num, err := fmt.Sscanf(tag, "%d-%d", &bucketMin, &bucketMax)
	test.Assert(err == nil, "Err %s, Str %s", err, tag)
	test.Assert(num == 2, "Incorrect number of ints in bucket tag")
	return bucketMin, bucketMax
}
