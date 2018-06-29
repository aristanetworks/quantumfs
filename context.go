// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"fmt"
	"os"
	"sync/atomic"

	"golang.org/x/net/context"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/ether/cql"
	influxlib "github.com/aristanetworks/influxlib/go"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	qubitutils "github.com/aristanetworks/qubit/tools/utils"
)

var requestID uint64

// Ctx maintains context for the walker daemon.
type Ctx struct {
	context.Context
	Influx      *influxlib.InfluxDBConnection
	qctx        *quantumfs.Ctx
	wsdb        quantumfs.WorkspaceDB
	ds          quantumfs.DataStore
	cqlds       blobstore.BlobStore
	ttlCfg      *qubitutils.TTLConfig
	etherConfig string
	wsdbConfig  string
	numSuccess  uint32
	numError    uint32
	numWalkers  int
	iteration   uint
	keyspace    string
	aliveCount  int64
}

func getWalkerDaemonContext(influxServer string, influxPort uint16,
	influxDBName string, etherCfgFile string, wsdbCfgStr string,
	logdir string, numwalkers int) *Ctx {

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
			os.Exit(exitBadConfig)
		}
	}

	// Connect to ether backed quantumfs DataStore
	quantumfsDS, err := thirdparty_backends.ConnectDatastore("ether.cql", etherCfgFile)
	if err != nil {
		fmt.Printf("Connection to DataStore failed")
		os.Exit(exitBadConfig)
	}

	// Extract blobstore from quantumfs DataStore
	// since we specifically use ether.cql datastore, it must implement
	// the following interfaces
	b, ok := quantumfsDS.(*thirdparty_backends.EtherBlobStoreTranslator)
	if !ok {
		fmt.Printf("Found unsupported datastore adapter\n")
		os.Exit(exitBadConfig)
	}
	cqlDS := b.Blobstore
	b.ApplyTTLPolicy = false

	c, isa := cqlDS.(cql.CqlStore)
	if !isa {
		fmt.Printf("Found unsupported datastore\n")
		os.Exit(exitBadConfig)
	}
	keyspace := c.Keyspace()

	// if a wsdbCfgStr was provided use that to connect to 'grpc' backend.
	var quantumfsWSDB quantumfs.WorkspaceDB
	if wsdbCfgStr != "" {
		quantumfsWSDB, err = thirdparty_backends.ConnectWorkspaceDB("grpc", wsdbCfgStr)
		if err != nil {
			fmt.Printf("Connection to workspaceDB failed err: %v\n", err)
			os.Exit(exitBadConfig)
		}

	} else {
		quantumfsWSDB, err = thirdparty_backends.ConnectWorkspaceDB("ether.cql", etherCfgFile)
		if err != nil {
			fmt.Printf("Connection to workspaceDB failed err: %v\n", err)
			os.Exit(exitBadConfig)
		}
	}

	// Load TTL Config values
	ttlConfig, err := qubitutils.LoadTTLConfig(etherCfgFile)
	if err != nil {
		fmt.Printf("Failed to load TTL: %s\n", err.Error())
		os.Exit(exitBadConfig)
	}

	log, err := qlog.NewQlog(logdir)
	if err != nil {
		fmt.Printf("Error in initializing NewQlog: %v\n", err)
		os.Exit(1)
	}

	id := atomic.AddUint64(&requestID, 1)
	return &Ctx{
		Influx:      influx,
		qctx:        newQCtx(log, id),
		wsdb:        quantumfsWSDB,
		ds:          quantumfsDS,
		cqlds:       cqlDS,
		ttlCfg:      ttlConfig,
		etherConfig: etherCfgFile,
		wsdbConfig:  wsdbCfgStr,
		numWalkers:  numwalkers,
		keyspace:    keyspace,
	}
}

func newQCtx(log *qlog.Qlog, id uint64) *quantumfs.Ctx {
	c := &quantumfs.Ctx{
		Qlog:      log,
		RequestId: id,
	}
	log.SetLogLevels("Tool/*")
	return c
}

func (c *Ctx) newRequestID() *Ctx {

	id := atomic.AddUint64(&requestID, 1)
	// inherit all the fields from base context
	// so any new information in context is setup
	// in one place.
	newCtx := *c
	// override any info specific to the new context
	newCtx.qctx = newQCtx(c.qctx.Qlog, id)

	return &newCtx
}
