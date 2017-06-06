// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import "flag"
import "fmt"
import "os"

import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/loggerdb"

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

	db := qloggerdb.NewMemdb()
	extractors := make([]qloggerdb.StatExtractor, 0)

	// sample extractor
	extractors = append(extractors, qloggerdb.NewExtPairStats(db,
		"---In Mux::GetAttr", "Out-- Mux::GetAttr", true))

	logger := qloggerdb.NewLoggerDb(db, extractors)

	reader.ProcessLogs(true, func(v qlog.LogOutput) {
		logger.ProcessLog(v)
	})
}
