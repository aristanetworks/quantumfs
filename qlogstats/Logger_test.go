// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

import (
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/processlocal"
)

func runReader(qlogFile string,
	extractors []StatExtractorConfig) *processlocal.Memdb {

	db := processlocal.NewMemdb()
	AggregateLogs(qlogFile, db, extractors)

	return db
}

func TestMatches(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.Logger

		// Artificially insert matching logs
		qlogHandle.Log(qlog.LogTest, 12345, 2, qlog.FnEnterStr+"TestMatch")
		qlogHandle.Log(qlog.LogTest, 12346, 3, qlog.FnExitStr+"TestMatch")

		// Setup an extractor
		extractors := make([]StatExtractorConfig, 0)
		extractors = append(extractors, NewStatExtractorConfig(
			NewExtPairStats(qlog.FnEnterStr+"TestMatch",
				qlog.FnExitStr+"TestMatch", true, "TestMatch"),
			(5*time.Second)))

		// Run the reader
		memdb := runReader(test.CachePath+"/ramfs/qlog", extractors)

		test.WaitFor("statistic to register", func () bool {

		})
	})
}
