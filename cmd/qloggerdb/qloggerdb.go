// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aristanetworks/quantumfs/qlogstats"
	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
)

func init() {
	flag.Usage = func() {
		fmt.Printf("Usage: %s <qlogPath>\n", os.Args[0])
	}
}

func main() {
	if len(os.Args) < 2 {
		flag.Usage()
		return
	}
	reader := qlog.NewReader(os.Args[1])

	db := processlocal.NewMemdb()
	extractors := make([]qlogstats.StatExtHook, 0)

	// sample extractor
	extractors = append(extractors, qlogstats.NewStatExtHook(
		qlogstats.NewExtPairStats(qlog.FnEnterStr+"Mux::GetAttr",
			qlog.FnExitStr+"Out-- Mux::GetAttr", true),
		(5*time.Second)))

	logger := qlogstats.NewLoggerDb(db, extractors)

	reader.ProcessLogs(qlog.ReadThenTail, func(v qlog.LogOutput) {
		logger.ProcessLog(v)
	})
}
