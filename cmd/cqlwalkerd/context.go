// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/net/context"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends"
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/cmd/cqlwalker/utils"
	"github.com/aristanetworks/quantumfs/qlog"
)

var requestID uint64

// Ctx maintains context for the walker daemon.
type Ctx struct {
	context.Context
	name            string
	host            string
	tsdb            quantumfs.TimeSeriesDB
	qctx            *quantumfs.Ctx
	wsdb            quantumfs.WorkspaceDB
	cqlws           cql.WorkspaceDB
	ds              quantumfs.DataStore
	cqlds           cql.BlobStore
	ttlCfg          *utils.TTLConfig
	cqlConfig       string
	wsdbConfig      string
	numSuccess      uint32
	numError        uint32
	numWalkers      int
	iteration       uint
	keyspace        string
	aliveCount      int64
	skipMap         *utils.SkipMap
	wsNameMatcher   func(s string) bool
	wsLastWriteTime func(c *Ctx, ts, ns, ws string) (time.Time, error)
	lwDuration      time.Duration
}

func wsLastWriteTime(c *Ctx, ts, ns, ws string) (time.Time, error) {
	return c.cqlws.WorkspaceLastWriteTime(cql.DefaultCtx, ts, ns, ws)
}

func loadTimeSeriesDB(db, dbConf string) quantumfs.TimeSeriesDB {
	tsdb, err := backends.ConnectTimeSeriesDB(db, dbConf)
	if err != nil {
		fmt.Printf("TimeSeriesDB load failed\n")
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	return tsdb
}

func getWalkerDaemonContext(name string, timeSeriesDB string,
	timeSeriesDBConf string,
	cqlCfgFile string, wsdbCfgStr string,
	logdir string, numwalkers int,
	matcher func(s string) bool,
	lastWriteDuration string) *Ctx {

	var err error

	tsdb := loadTimeSeriesDB(timeSeriesDB, timeSeriesDBConf)
	lwDuration := time.Duration(0)
	if lastWriteDuration != "" {
		lwDuration, err = time.ParseDuration(lastWriteDuration)
		if err != nil {
			fmt.Printf("Invalid duration specified. Error: %s\n", err)
			fmt.Println("Use Golang duration string format.")
			os.Exit(exitBadConfig)
		}
	}

	// Connect to cql backed quantumfs DataStore
	quantumfsDS, err := backends.ConnectDatastore("cql", cqlCfgFile)
	if err != nil {
		fmt.Printf("Connection to DataStore failed")
		os.Exit(exitBadConfig)
	}

	// Extract blobstore from quantumfs DataStore
	// since we specifically use cql datastore, it must implement
	// the following interfaces
	b, ok := quantumfsDS.(*backends.CqlBlobStoreTranslator)
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

	cqlWsdb := cql.NewUncachedWorkspaceDB(cqlCfgFile)

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
		quantumfsWSDB, err = backends.ConnectWorkspaceDB("cql",
			cqlCfgFile)
		if err != nil {
			fmt.Printf("Connection to workspaceDB failed err: %v\n", err)
			os.Exit(exitBadConfig)
		}
	}

	// Load TTL Config values
	ttlConfig, err := utils.LoadTTLConfig(cqlCfgFile)
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
		tsdb:            tsdb,
		qctx:            newQCtx(log, id),
		wsdb:            quantumfsWSDB,
		cqlws:           cqlWsdb,
		ds:              quantumfsDS,
		cqlds:           cqlDS,
		ttlCfg:          ttlConfig,
		cqlConfig:       cqlCfgFile,
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
	tags []quantumfs.Tag, fields []quantumfs.Field) {

	if c.tsdb == nil {
		return
	}
	c.tsdb.Store(measurement, tags, fields, time.Now())
}
