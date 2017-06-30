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

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/qlogstats"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
)

var database string
var databaseConf string

func init() {
	flag.StringVar(&database, "db", "memdb",
		"Name of database to use (memdb, influxdb)")
	flag.StringVar(&databaseConf, "dbConf", "", "Options to pass to database")

	flag.Usage = func() {
		fmt.Printf("Usage: %s [flags] <qlogPath>\n\n", os.Args[0])
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
}

func loadTimeSeriesDB() quantumfs.TimeSeriesDB {
	tsdb, err := thirdparty_backends.ConnectTimeSeriesDB(database, databaseConf)
	if err != nil {
		fmt.Printf("TimeSeriesDB load failed\n")
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	return tsdb
}

func main() {
	flag.Parse()
	if len(os.Args) < 2 {
		flag.Usage()
		return
	}

	lastParam := os.Args[len(os.Args)-1]
	if lastParam[0] == '-' {
		fmt.Printf("Last parameter must be qlog file.\n")
		return
	}

	extractors := make([]qlogstats.StatExtractorConfig, 0)
	db := loadTimeSeriesDB()

	// sample extractor
	extractors = append(extractors, qlogstats.NewStatExtractorConfig(
		qlogstats.NewExtPairStats(qlog.FnEnterStr+"Mux::GetAttr Inode %d\n",
			qlog.FnExitStr+"Mux::GetAttr\n", true, "Mux::GetAttr"),
		(5*time.Second)))

	qlogstats.AggregateLogs(qlog.ReadThenTail, lastParam, db, extractors)
}
