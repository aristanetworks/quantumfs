// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/qlogstats"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
)

var useInfluxDB bool
var influxDBHostname string
var influxDBPort int
var influxDBProtocol string
var influxDBDatabase string

func init() {
	flag.BoolVar(&useInfluxDB, "influxdb", false, "Use InfluxDB")
	flag.StringVar(&influxDBHostname, "influxHostname", "",
		"Change InfluxDB hostname from the default")
	flag.IntVar(&influxDBPort, "influxPort", -1,
		"Change InfluxDB port from the default")
	flag.StringVar(&influxDBProtocol, "influxProtocol", "",
		"Change InfluxDB protocol from the default")
	flag.StringVar(&influxDBDatabase, "influxDatabase", "",
		"Change InfluxDB database from the default")

	flag.Usage = func() {
		fmt.Printf("Usage: %s <qlogPath>\n\n", os.Args[0])
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
}

func loadTimeSeriesDB() quantumfs.TimeSeriesDB {

	if !useInfluxDB {
		return processlocal.NewMemdb()
	}

	tsdbFlags := make(map[string]string)
	if influxDBHostname != "" {
		tsdbFlags["hostname"] = influxDBHostname
	}
	if influxDBPort >= 0 {
		tsdbFlags["port"] = strconv.Itoa(influxDBPort)
	}
	if influxDBProtocol != "" {
		tsdbFlags["protocol"] = influxDBProtocol
	}
	if influxDBDatabase != "" {
		tsdbFlags["database"] = influxDBDatabase
	}

	cfgString, err := json.Marshal(tsdbFlags)
	if err != nil {
		panic(err)
	}

	tsdb, err := thirdparty_backends.ConnectTimeSeriesDB("influxlib",
		string(cfgString))
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

	extractors := make([]qlogstats.StatExtractorConfig, 0)
	db := loadTimeSeriesDB()

	// sample extractor
	extractors = append(extractors, qlogstats.NewStatExtractorConfig(
		qlogstats.NewExtPairStats(qlog.FnEnterStr+"Mux::GetAttr",
			qlog.FnExitStr+"Mux::GetAttr", true, "Mux::GetAttr"),
		(5*time.Second)))

	qlogstats.AggregateLogs(qlog.ReadThenTail, os.Args[1], db, extractors)
}
