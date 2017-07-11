// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qloggerdb is a shared memory log parser and statistics uploader for the qlog
// quantumfs subsystem.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
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

const statPeriod = 30 * time.Second

func newQfsExtPair(common string,
	startPostfix string) qlogstats.StatExtractorConfig {

	return qlogstats.NewStatExtractorConfig(
		qlogstats.NewExtPairStats(
			qlog.FnEnterStr+common+" "+startPostfix,
			qlog.FnExitStr+common,
			true, common),
		statPeriod)
}

func createExtractors() []qlogstats.StatExtractorConfig {
	return []qlogstats.StatExtractorConfig{
		qlogstats.NewStatExtractorConfig(
			qlogstats.NewExtPointStats(daemon.CacheHitLog,
				"readcache_hit"), statPeriod),
		qlogstats.NewStatExtractorConfig(
			qlogstats.NewExtPointStats(daemon.CacheMissLog,
				"readcache_miss"), statPeriod),

		newQfsExtPair(thirdparty_backends.EtherGetLog,
			thirdparty_backends.KeyLog),
		newQfsExtPair(thirdparty_backends.EtherSetLog,
			thirdparty_backends.KeyLog),
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
		newQfsExtPair(daemon.SyncAllLog, ""),
		newQfsExtPair(daemon.LookupLog, daemon.InodeNameLog),
		newQfsExtPair(daemon.ForgetLog, ""),
		newQfsExtPair(daemon.GetAttrLog, daemon.InodeOnlyLog),
		newQfsExtPair(daemon.SetAttrLog, daemon.InodeOnlyLog),
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

	db := loadTimeSeriesDB()

	extractors := createExtractors()

	qlogstats.AggregateLogs(qlog.ReadThenTail, lastParam, db, extractors)
}
