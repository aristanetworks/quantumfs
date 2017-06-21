// +build !skip_backends

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package thirdparty_backends

// To avoid compiling in support for ether datastore
// change "!skip_backends" in first line with "ignore"

import (
	"encoding/json"
	"os"
	"strconv"

	"github.com/aristanetworks/influxlib/go"
	"github.com/aristanetworks/quantumfs"
)

func init() {
	registerTimeSeriesDB("influxlib", newInfluxDB)
}

type influxlibAdapter struct {
	connector *influxlib.InfluxDBConnection
	hostname  string
}

func newInfluxDB(config string) quantumfs.TimeSeriesDB {
	cfg := *influxlib.DefaultConfig()

	flags := make(map[string]string)
	json.Unmarshal([]byte(config), flags)

	if hostname, exists := flags["hostname"]; exists {
		cfg.Hostname = hostname
	}
	if port_, exists := flags["port"]; exists {
		port, err := strconv.Atoi(port_)
		if err != nil {
			panic(err)
		}
		cfg.Port = uint16(port)
	}
	if protocol, exists := flags["protocol"]; exists {
		cfg.Protocol = protocol
	}
	if database, exists := flags["database"]; exists {
		cfg.Database = database
	}

	dbConn, err := influxlib.Connect(&cfg)
	if err != nil {
		panic(err)
	}

	host, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	return &influxlibAdapter{
		connector: dbConn,
		hostname:  host,
	}
}

func (inf *influxlibAdapter) Store(tags []quantumfs.Tag, fields []quantumfs.Field) {
	// InfluxDB automatically adds a timestamp field
	tags = append(tags, quantumfs.NewTag("server", inf.hostname))

	tagMap := make(map[string]string)
	for _, v := range tags {
		tagMap[v.Name] = v.Data
	}

	fieldMap := make(map[string]interface{})
	for _, v := range fields {
		fieldMap[v.Name] = v.Data
	}

	err := inf.connector.WritePoint("quantumfs", tagMap, fieldMap)
	if err != nil {
		panic(err)
	}
}
