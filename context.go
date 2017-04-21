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

func getWalkerDaemonContext(serverIP string, config string,
	logdir string) *Ctx {

	// Connect to InfluxDB
	var influx *influxlib.InfluxDBConnection
	var err error
	if serverIP == "" {
		influx, err = influxlib.Connect()
		if err != nil {
			fmt.Printf("Unable to connect to influxDB\n")
			os.Exit(1)
		}
	} else {
		influx, err = influxlib.ConnectToHost(serverIP)
		if err != nil {
			fmt.Printf("Unable to connect to influxDB at IP:%v\n", serverIP)
			os.Exit(1)
		}
		influx.UseDatabase("mydb")
	}

	// Connect to ether.cql WorkSpaceDB
	db, err := thirdparty_backends.ConnectWorkspaceDB("ether.cql", config)
	if err != nil {
		fmt.Printf("Connection to workspaceDB failed\n")
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
	return c
}
