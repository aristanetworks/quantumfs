// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/aristanetworks/ether/cql"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/grpc"
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

func newQfsExtPair(common string,
	startPostfix string) qlogstats.StatExtractor {

	return qlogstats.NewExtPairStats(
		qlog.FnEnterStr+common+" "+startPostfix,
		qlog.FnExitStr+common, common)
}

func getFirstParamInt(log *qlog.LogOutput) (int64, bool) {
	if len(log.Args) == 0 {
		return 0, false
	}

	num := log.Args[0].(int)
	return int64(num), true
}

func createExtractors() []qlogstats.StatExtractor {
	return []qlogstats.StatExtractor{
		// Critical system errors
		qlogstats.NewExtPointStatsPartialFormat("ERROR: ", "SystemErrors"),

		// Cache hits and misses
		qlogstats.NewExtPointStats(daemon.CacheHitLog, "readcache_hit"),
		qlogstats.NewExtPointStats(daemon.CacheMissLog, "readcache_miss"),

		qlogstats.NewExtPointStats(thirdparty_backends.EtherTtlCacheHit,
			"ether_setcache_hit"),
		qlogstats.NewExtPointStats(thirdparty_backends.EtherTtlCacheMiss,
			"ether_setcache_miss"),
		qlogstats.NewExtPointStats(thirdparty_backends.EtherTtlCacheEvict,
			"ether_setcache_evict"),

		// Buffer usage statistics
		qlogstats.NewExtLogDataStats(daemon.BufferReadLog, "buffer_read",
			qlogstats.NewHistoStats(0, int64(quantumfs.MaxBlockSize),
			20), getFirstParamInt),
		qlogstats.NewExtLogDataStats(daemon.BufferGetLog, "buffer_get",
			qlogstats.NewHistoStats(0, int64(quantumfs.MaxBlockSize),
			20), getFirstParamInt),

		// Data store latency
		newQfsExtPair(thirdparty_backends.EtherGetLog,
			thirdparty_backends.KeyLog),
		newQfsExtPair(thirdparty_backends.EtherSetLog,
			thirdparty_backends.KeyLog),

		// Workspace DB latency
		newQfsExtPair(thirdparty_backends.EtherTypespaceLog, ""),
		newQfsExtPair(thirdparty_backends.EtherNamespaceLog,
			thirdparty_backends.EtherNamespaceDebugLog),
		newQfsExtPair(thirdparty_backends.EtherWorkspaceListLog,
			thirdparty_backends.EtherWorkspaceListDebugLog),
		newQfsExtPair(thirdparty_backends.EtherWorkspaceLog,
			thirdparty_backends.EtherWorkspaceDebugLog),
		newQfsExtPair(thirdparty_backends.EtherBranchLog,
			thirdparty_backends.EtherBranchDebugLog),
		newQfsExtPair(thirdparty_backends.EtherAdvanceLog,
			thirdparty_backends.EtherAdvanceDebugLog),

		newQfsExtPair(grpc.NumTypespaceLog, ""),
		newQfsExtPair(grpc.TypespaceListLog, ""),
		newQfsExtPair(grpc.NumNamespacesLog, ""),
		newQfsExtPair(grpc.NamespaceListLog, ""),
		newQfsExtPair(grpc.NumWorkspacesLog, ""),
		newQfsExtPair(grpc.WorkspaceListLog, ""),
		newQfsExtPair(grpc.BranchWorkspaceLog, ""),
		newQfsExtPair(grpc.DeleteWorkspaceLog, ""),
		newQfsExtPair(grpc.FetchWorkspaceLog, grpc.FetchWorkspaceDebug),
		newQfsExtPair(grpc.AdvanceWorkspaceLog, grpc.AdvanceWorkspaceDebug),
		newQfsExtPair(grpc.WorkspaceIsImmutableLog, ""),
		newQfsExtPair(grpc.SetWorkspaceImmutableLog,
			grpc.SetWorkspaceImmutableDebug),

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

		// Ether.CQL internal statistics
		newQfsExtPair(cql.DeleteLog, cql.KeyLog),
		newQfsExtPair(cql.GetLog, cql.KeyLog),
		newQfsExtPair(cql.InsertLog, cql.KeyTTLLog),
		newQfsExtPair(cql.UpdateLog, cql.KeyLog),
		newQfsExtPair(cql.MetadataLog, cql.KeyLog),
		newQfsExtPair(cql.GoCqlGetLog, cql.KeyLog),
		newQfsExtPair(cql.GoCqlInsertLog, cql.KeyTTLLog),
		newQfsExtPair(cql.GoCqlMetadataLog, cql.KeyLog),
	}
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

	go func() {
		fmt.Println(http.ListenAndServe("localhost:6061", nil))
	}()

	db := loadTimeSeriesDB()

	extractors := createExtractors()

	qlogstats.AggregateLogs(qlog.ReadThenTail, lastParam, db, extractors,
		30*time.Second)
}
