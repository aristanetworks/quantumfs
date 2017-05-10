// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"fmt"
	"os"

	influxlib "github.com/aristanetworks/influxlib/go"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
)

// Ctx maintains context for the walker daemon.
type Ctx struct {
	Influx   *influxlib.InfluxDBConnection
	qctx     *quantumfs.Ctx
	wsdb     quantumfs.WorkspaceDB
	confFile string
}

func getWalkerDaemonContext(influxServer string, influxPort int,
	influxDBName string, config string, logdir string) *Ctx {

	// Connect to InfluxDB
	var influx *influxlib.InfluxDBConnection
	var err error
	if influxServer == "" {
		influx, err = influxlib.Connect(nil)
		if err != nil {
			fmt.Printf("Unable to connect to default influxDB err:%v\n", err)
			os.Exit(1)
		}
	} else {
		influxConfig := influxlib.DefaultConfig()
		influxConfig.Hostname = influxServer
		influxConfig.Port = influxPort
		influxConfig.Database = influxDBName

		influx, err = influxlib.Connect(influxConfig)
		if err != nil {
			fmt.Printf("Unable to connect to influxDB at addr %v:%v and db:%v err:%v\n",
				influxServer, influxPort, influxDBName, err)
			os.Exit(1)
		}
	}

	// Connect to ether.cql WorkSpaceDB
	db, err := thirdparty_backends.ConnectWorkspaceDB("ether.cql", config)
	if err != nil {
		fmt.Printf("Connection to workspaceDB failed err: %v\n", err)
		os.Exit(exitBadConfig)
	}

	return &Ctx{
		Influx:   influx,
		qctx:     newCtx(logdir),
		wsdb:     db,
		confFile: config,
	}
}

func newCtx(logdir string) *quantumfs.Ctx {
	log := qlog.NewQlogTiny()
	if logdir != "" {
		log = qlog.NewQlog(logdir)
	}

	c := &quantumfs.Ctx{
		Qlog:      log,
		RequestId: 1,
	}
	log.SetLogLevels("Tool/*")
	return c
}
