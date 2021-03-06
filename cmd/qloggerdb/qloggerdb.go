// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends"
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/qlogstats"
	"github.com/aristanetworks/quantumfs/utils"
)

var database string
var databaseConf string

func init() {
	flag.StringVar(&database, "db", "processlocal",
		"Name of database to use (processlocal, influxlib)")
	flag.StringVar(&databaseConf, "dbConf", "", "Options to pass to database")

	flag.Usage = func() {
		fmt.Printf("Usage: %s [flags] <qlogPath>\n\n", os.Args[0])
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
}

func loadTimeSeriesDB() quantumfs.TimeSeriesDB {
	tsdb, err := backends.ConnectTimeSeriesDB(database, databaseConf)
	if err != nil {
		fmt.Printf("TimeSeriesDB load failed\n")
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	return tsdb
}

func newQfsExtPair(common string,
	startPostfix string) qlogstats.StatExtractor {

	start := qlog.FnEnterStr + common
	if startPostfix != "" {
		start += " " + startPostfix
	}

	return qlogstats.NewExtPairStats(
		start,
		qlog.FnExitStr+common, common)
}

var extractors []qlogstats.StatExtractor

func init() {
	quantumFS := []qlogstats.StatExtractor{
		// Critical system errors
		qlogstats.NewExtPointStatsPartialFormat("ERROR: ", "SystemErrors"),

		// Cache hits and misses
		qlogstats.NewExtPointStats(daemon.CacheHitLog, "readcache_hit"),
		qlogstats.NewExtPointStats(daemon.CacheMissLog, "readcache_miss"),

		// FUSE Requests
		newQfsExtPair(daemon.LookupLog, daemon.InodeNameLog),
		newQfsExtPair(daemon.ForgetLog, ""),
		newQfsExtPair(daemon.GetAttrLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.SetAttrLog, daemon.SetAttrArgLog),
		newQfsExtPair(daemon.MknodLog, daemon.InodeNameLog),
		newQfsExtPair(daemon.MkdirLog, daemon.InodeNameLog),
		newQfsExtPair(daemon.UnlinkLog, daemon.InodeNameLog),
		newQfsExtPair(daemon.RmdirLog, daemon.InodeNameLog),
		newQfsExtPair(daemon.RenameLog, daemon.RenameDebugLog),
		newQfsExtPair(daemon.LinkLog, daemon.LinkDebugLog),
		newQfsExtPair(daemon.SymlinkLog, daemon.InodeNameLog),
		newQfsExtPair(daemon.ReadlinkLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.AccessLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.GetXAttrSizeLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.GetXAttrDataLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.ListXAttrLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.SetXAttrLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.RemoveXAttrLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.CreateLog, daemon.InodeNameLog),
		newQfsExtPair(daemon.OpenLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.ReadLog, daemon.FileHandleLog),
		newQfsExtPair(daemon.ReleaseLog, daemon.FileHandleLog),
		newQfsExtPair(daemon.WriteLog, daemon.FileHandleLog),
		newQfsExtPair(daemon.FlushLog, daemon.FlushDebugLog),
		newQfsExtPair(daemon.FsyncLog, daemon.FileHandleLog),
		newQfsExtPair(daemon.FallocateLog, ""),
		newQfsExtPair(daemon.OpenDirLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.ReadDirLog, daemon.FileOffsetLog),
		newQfsExtPair(daemon.ReadDirPlusLog, daemon.FileOffsetLog),
		newQfsExtPair(daemon.ReleaseDirLog, daemon.FileHandleLog),
		newQfsExtPair(daemon.FsyncDirLog, daemon.FileHandleLog),
		newQfsExtPair(daemon.StatFsLog, ""),

		// Top-level QuantumFS filesystem operations
		newQfsExtPair(daemon.ReleaseFileHandleLog, ""),
		newQfsExtPair(daemon.SyncWorkspaceLog, ""),
		newQfsExtPair(daemon.SyncAllLog, ""),

		// Per QuBIT-task statistics
		qlogstats.NewExtWorkspaceStats("per-Task", []string{
			daemon.LookupLog,
			daemon.GetAttrLog,
			daemon.SetAttrLog,
			daemon.MknodLog,
			daemon.MkdirLog,
			daemon.UnlinkLog,
			daemon.RmdirLog,
			daemon.RenameLog,
			daemon.LinkLog,
			daemon.SymlinkLog,
			daemon.ReadlinkLog,
			daemon.AccessLog,
			daemon.GetXAttrSizeLog,
			daemon.GetXAttrDataLog,
			daemon.ListXAttrLog,
			daemon.SetXAttrLog,
			daemon.RemoveXAttrLog,
			daemon.CreateLog,
			daemon.OpenLog,
			daemon.ReadLog,
			daemon.WriteLog,
			daemon.OpenDirLog,
			daemon.ReadDirPlusLog,
		}),
	}

	extractors = append(extractors, quantumFS...)

	cql := []qlogstats.StatExtractor{
		qlogstats.NewExtPointStats(cql.CqlTtlCacheHit,
			"cql_setcache_hit"),
		qlogstats.NewExtPointStats(cql.CqlTtlCacheMiss,
			"cql_setcache_miss"),
		qlogstats.NewExtPointStats(cql.CqlTtlCacheEvict,
			"cql_setcache_evict"),

		// Data store latency
		newQfsExtPair(cql.CqlGetLog,
			cql.KeyLog),
		newQfsExtPair(cql.CqlSetLog,
			cql.KeyLog),

		// Workspace DB latency
		newQfsExtPair(cql.CqlTypespaceLog, ""),
		newQfsExtPair(cql.CqlNamespaceLog,
			cql.CqlNamespaceDebugLog),
		newQfsExtPair(cql.CqlWorkspaceListLog,
			cql.CqlWorkspaceListDebugLog),
		newQfsExtPair(cql.CqlWorkspaceLog,
			cql.CqlWorkspaceDebugLog),
		newQfsExtPair(cql.CqlBranchLog,
			cql.CqlBranchDebugLog),
		newQfsExtPair(cql.CqlAdvanceLog,
			cql.CqlAdvanceDebugLog),

		// CQL internal statistics
		newQfsExtPair(cql.DeleteLog, cql.KeyLog),
		newQfsExtPair(cql.GetLog, cql.KeyLog),
		newQfsExtPair(cql.InsertLog, cql.KeyTTLLog),
		newQfsExtPair(cql.UpdateLog, cql.KeyLog),
		newQfsExtPair(cql.MetadataLog, cql.KeyLog),
		newQfsExtPair(cql.GoCqlGetLog, cql.KeyLog),
		newQfsExtPair(cql.GoCqlInsertLog, cql.KeyTTLLog),
		newQfsExtPair(cql.GoCqlMetadataLog, cql.KeyLog),
	}

	extractors = append(extractors, cql...)
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

	utils.ServePprof()

	db := loadTimeSeriesDB()

	qlogstats.AggregateLogs(qlog.ReadThenTail, lastParam, db, extractors,
		30*time.Second)
}
