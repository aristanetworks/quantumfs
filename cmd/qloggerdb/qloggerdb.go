// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/aristanetworks/quantumfs/loggerdb"
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
	extractors := make([]qloggerdb.StatExtractor, 0)

	// sample extractor
	extractors = append(extractors, qloggerdb.NewExtPairAvg(db,
		"---In Mux::GetAttr", "Out-- Mux::GetAttr"))

	logger := qloggerdb.NewLoggerDb(db, extractors)

	reader.ProcessLogs(true, func(v qlog.LogOutput) {
		logger.ProcessLog(v)
	})
}
