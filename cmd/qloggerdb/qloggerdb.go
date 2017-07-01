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

const statPeriod = 5 * time.Second

func newQfsExtPair(common string,
	startPostfix string) qlogstats.StatExtractorConfig {

	return qlogstats.NewStatExtractorConfig(
		qlogstats.NewExtPairStats(
			qlog.FnEnterStr+common+" "+startPostfix+"\n",
			qlog.FnExitStr+common+"\n",
			true, common),
		statPeriod)
}

func createExtractors() []qlogstats.StatExtractorConfig {
	return []qlogstats.StatExtractorConfig {
		qlogstats.NewStatExtractorConfig(
			qlogstats.NewExtPointStats("Found key in readcache",
			"cache_hit"), statPeriod),
		qlogstats.NewStatExtractorConfig(
			qlogstats.NewExtPointStats("Cache miss",
			"cache_miss"), statPeriod),

		newQfsExtPair("EtherBlobStoreTranslator::Get", "key %s"),
		newQfsExtPair("EtherBlobStoreTranslator::Set", "key %s"),
		newQfsExtPair("EtherWsdbTranslator::TypespaceList", ""),
		newQfsExtPair("EtherWsdbTranslator::NamespaceList", "typespace: %s"),
		newQfsExtPair("EtherWsdbTranslator::WorkspaceList", "%s/%s"),
		newQfsExtPair("EtherWsdbTranslator::Workspace", "%s/%s/%s"),
		newQfsExtPair("EtherWsdbTranslator::BranchWorkspace",
			"%s/%s/%s -> %s/%s/%s"),
		newQfsExtPair("EtherWsdbTranslator::AdvanceWorkspace",
			"%s/%s/%s %s -> %s"),
		newQfsExtPair("Mux::syncAll", ""),
		newQfsExtPair("Mux::Lookup", "Inode %d Name %s"),
		newQfsExtPair("Mux::Forget", ""),
		newQfsExtPair("Mux::GetAttr", "Inode %d"),
		newQfsExtPair("Mux::SetAttr", "Inode %d"),
		newQfsExtPair("Mux::Mknod", "Inode %d Name %s"),
		newQfsExtPair("Mux::Mkdir", "Inode %d Name %s"),
		newQfsExtPair("Mux::Unlink", "Inode %d Name %s"),
		newQfsExtPair("Mux::Rmdir", "Inode %d Name %s"),
		newQfsExtPair("Mux::Rename", "Inode %d newdir %d %s -> %s"),
		newQfsExtPair("Mux::Link", "inode %d to name %s in dstDir %d"),
		newQfsExtPair("Mux::Symlink", "Inode %d Name %s"),
		newQfsExtPair("Mux::Readlink", "Inode %d"),
		newQfsExtPair("Mux::Access", "Inode %d"),
		newQfsExtPair("Mux::GetXAttrSize", "Inode %d"),
		newQfsExtPair("Mux::GetXAttrData", "Inode %d"),
		newQfsExtPair("Mux::ListXAttr", "Inode %d"),
		newQfsExtPair("Mux::SetXAttr", "Inode %d"),
		newQfsExtPair("Mux::RemoveXAttr", "Inode %d"),
		newQfsExtPair("Mux::Create", "Inode %d Name %s"),
		newQfsExtPair("Mux::Open", "Inode %d"),
		newQfsExtPair("Mux::Read", "Fh: %d"),
		newQfsExtPair("Mux::Release", "Fh: %v"),
		newQfsExtPair("Mux::Write", "Fh: %d"),
		newQfsExtPair("Mux::Flush", "Fh: %v Context %d %d %d"),
		newQfsExtPair("Mux::Fsync", "Enter Fh %d"),
		newQfsExtPair("Mux::Fallocate", ""),
		newQfsExtPair("Mux::OpenDir", "Inode %d"),
		newQfsExtPair("Mux::ReadDir", "Fh: %d offset %d"),
		newQfsExtPair("Mux::ReadDirPlus", "Fh: %d offset %d"),
		newQfsExtPair("Mux::ReleaseDir", "Fh: %d"),
		newQfsExtPair("Mux::FsyncDir", "Fh: %d"),
		newQfsExtPair("Mux::StatFs", ""),
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
