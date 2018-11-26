// +build influxlib

// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package thirdparty_backends

// Please see FEATURES in makefile.mk for details on how to toggle this feature.

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/aristanetworks/influxlib/go"
	"github.com/aristanetworks/quantumfs"
)

func init() {
	registerTimeSeriesDB("influxdb", newInfluxDB)
}

type influxlibAdapter struct {
	connector *influxlib.InfluxDBConnection
}

func newInfluxDB(config string) quantumfs.TimeSeriesDB {
	cfg := *influxlib.DefaultConfig()

	flags := flag.NewFlagSet("influxdb", flag.ExitOnError)
	flags.StringVar(&cfg.Hostname, "hostname", cfg.Hostname, "InfluxDB Hostname")
	flags.StringVar(&cfg.Protocol, "protocol", cfg.Protocol, "InfluxDB Procotol")
	flags.StringVar(&cfg.Database, "database", cfg.Database, "InfluxDB Database")

	port := int(cfg.Port)
	flags.IntVar(&port, "port", port, "InfluxDB Port")

	flags.Parse(strings.Split(config, " "))
	cfg.Port = uint16(port)

	dbConn, err := influxlib.Connect(&cfg)
	if err != nil {
		panic(err)
	}

	return &influxlibAdapter{
		connector: dbConn,
	}
}

func (inf *influxlibAdapter) Store(measurement string, tags []quantumfs.Tag,
	fields []quantumfs.Field, t time.Time) {

	// InfluxDB automatically adds a timestamp field

	// Lookup the hostname for each Store to allow it to change underneath
	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	tags = append(tags, quantumfs.NewTag("host", host))

	tagMap := make(map[string]string)
	for _, v := range tags {
		tagMap[v.Name] = v.Data
	}

	fieldMap := make(map[string]interface{})
	for _, v := range fields {
		fieldMap[v.Name] = v.Data
	}

	err = inf.connector.WritePointTimed(measurement, tagMap, fieldMap, t)
	if err != nil {
		panic(err)
	}
}
