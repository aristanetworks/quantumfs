// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

import (
	"testing"

	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/testutils"
)

func runReader(qlogFile string, extractors []qlogstats.StatExtractorConfig,
	fxn func(qlog.LogOutput)) processlocal.Memdb {

	reader := qlog.NewReader(qlogFile)

	db := processlocal.NewMemdb()

	logger := qlogstats.NewLoggerDb(db, extractors)
	reader.ProcessLogs(qlog.ReadOnly, func(v qlog.LogOutput) {
		logger.ProcessLog(v)
	})

	return db
}

func TestMatches(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.qfs.c.Ctx.Qlog

		// Artificially insert matching logs
		qlogHandle.Log(LogTest, 12345, 2, qlog.FnEnterStr+"TestMatch")
		qlogHandle.Log(LogTest, 12346, 3, qlog.FnExitStr+"TestMatch")

		// Setup an extractor
		extractors := make([]qlogstats.StatExtractorConfig, 0)
		extractors = append(extractors, qlogstats.NewStatExtractorConfig(
			qlogstats.NewExtPairStats(qlog.FnEnterStr+"TestMatch",
				qlog.FnExitStr+"TestMatch", true),
			(5*time.Second)))

		// Run the reader
		memdb, reader := runReader(test.CachePath+"/ramfs/qlog", extractors)

	})
}
