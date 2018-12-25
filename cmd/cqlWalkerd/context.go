// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	influxlib "github.com/aristanetworks/influxlib/go"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends"
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/qlog"
	qubitutils "github.com/aristanetworks/quantumfs/utils/qutils"
	walkerutils "github.com/aristanetworks/quantumfs/utils/qutils2"
)

var requestID uint64

// Ctx maintains context for the walker daemon.
type Ctx struct {
	context.Context
	name            string
	host            string
	influx          *influxlib.InfluxDBConnection
	qctx            *quantumfs.Ctx
	wsdb            quantumfs.WorkspaceDB
	cqlws           cql.WorkspaceDB
	ds              quantumfs.DataStore
	cqlds           cql.BlobStore
	ttlCfg          *qubitutils.TTLConfig
	etherConfig     string
	wsdbConfig      string
	numSuccess      uint32
	numError        uint32
	numWalkers      int
	iteration       uint
	keyspace        string
	aliveCount      int64
	skipMap         *walkerutils.SkipMap
	wsNameMatcher   func(s string) bool
	wsLastWriteTime func(c *Ctx, ts, ns, ws string) (time.Time, error)
	lwDuration      time.Duration
}

func wsLastWriteTime(c *Ctx, ts, ns, ws string) (time.Time, error) {
	return c.cqlws.WorkspaceLastWriteTime(cql.DefaultCtx, ts, ns, ws)
}

func getWalkerDaemonContext(name string, influxServer string, influxPort uint16,
	influxDBName string, etherCfgFile string, wsdbCfgStr string,
	logdir string, numwalkers int,
	matcher func(s string) bool,
	lastWriteDuration string) *Ctx {

	// Connect to InfluxDB
	var influx *influxlib.InfluxDBConnection
	var err error
	if influxServer == "" {
		influx, err = influxlib.Connect(nil)
		if err != nil {
			fmt.Printf("Unable to connect to default influxDB err:%v\n",
				err)
			os.Exit(exitMiscError)
		}
	} else {
		influxConfig := influxlib.DefaultConfig()
		influxConfig.Hostname = influxServer
		influxConfig.Port = influxPort
		influxConfig.Database = influxDBName

		influx, err = influxlib.Connect(influxConfig)
		if err != nil {
			fmt.Printf("Unable to connect to influxDB at addr "+
				"%v:%v and db:%v err:%v\n",
				influxServer, influxPort, influxDBName, err)
			os.Exit(exitBadConfig)
		}
	}

	lwDuration := time.Duration(0)
	if lastWriteDuration != "" {
		lwDuration, err = time.ParseDuration(lastWriteDuration)
		if err != nil {
			fmt.Printf("Invalid duration specified. Error: %s\n", err)
			fmt.Println("Use Golang duration string format.")
			os.Exit(exitBadConfig)
		}
	}

	// Connect to ether backed quantumfs DataStore
	quantumfsDS, err := backends.ConnectDatastore("ether.cql", etherCfgFile)
	if err != nil {
		fmt.Printf("Connection to DataStore failed")
		os.Exit(exitBadConfig)
	}

	// Extract blobstore from quantumfs DataStore
	// since we specifically use ether.cql datastore, it must implement
	// the following interfaces
	b, ok := quantumfsDS.(*backends.EtherBlobStoreTranslator)
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

	etherWsdb := cql.NewUncachedWorkspaceDB(etherCfgFile)

	keyspace := c.Keyspace()

	// if a wsdbCfgStr was provided use that to connect to 'grpc' backend.
	var quantumfsWSDB quantumfs.WorkspaceDB
	if wsdbCfgStr != "" {
		quantumfsWSDB, err = backends.ConnectWorkspaceDB("grpc", wsdbCfgStr)
		if err != nil {
			fmt.Printf("Connection to workspaceDB failed err: %v\n", err)
			os.Exit(exitBadConfig)
		}

	} else {
		quantumfsWSDB, err = backends.ConnectWorkspaceDB("ether.cql",
			etherCfgFile)
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
		os.Exit(exitMiscError)
	}

	// short hostname, not FQDN
	host, err := os.Hostname()
	if err != nil {
		fmt.Printf("Failed to find hostname: %s\n", err.Error())
		os.Exit(exitMiscError)
	}

	id := atomic.AddUint64(&requestID, 1)
	return &Ctx{
		name:            name,
		host:            host,
		influx:          influx,
		qctx:            newQCtx(log, id),
		wsdb:            quantumfsWSDB,
		cqlws:           etherWsdb,
		ds:              quantumfsDS,
		cqlds:           cqlDS,
		ttlCfg:          ttlConfig,
		etherConfig:     etherCfgFile,
		wsdbConfig:      wsdbCfgStr,
		numWalkers:      numwalkers,
		keyspace:        keyspace,
		wsNameMatcher:   matcher,
		lwDuration:      lwDuration,
		wsLastWriteTime: wsLastWriteTime,
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

func (c *Ctx) WriteStatPoint(measurement string,
	tags map[string]string, fields map[string]interface{}) error {

	if c.influx == nil {
		return nil
	}
	return c.influx.WritePoint(measurement, tags, fields)

}
