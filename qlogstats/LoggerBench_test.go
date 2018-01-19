// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

// Test the qlogger performance

import (
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	"github.com/aristanetworks/quantumfs/utils"
)

func BenchmarkEndToEnd(test *testing.B) {
	fmt := "This is a test format string, %d %f %s"

	dir := utils.TmpDir()
	qlog_, err := qlog.NewQlog(dir)
	utils.AssertNoErr(err)

	extractors := make([]StatExtractor, 0)
	extractors = append(extractors, NewExtPointStats(fmt, "test"))

	db, err := thirdparty_backends.ConnectTimeSeriesDB("memdb", "")
	utils.AssertNoErr(err)

	go AggregateLogs(qlog.ReadThenTail, dir+"/qlog", db, extractors,
		30*time.Millisecond)

	for i := 0; i < test.N; i++ {
		qlog_.Log(qlog.LogTest, 12345, 1, fmt, 54321, 123.45,
			"This is an extra string parameter")
	}
}
