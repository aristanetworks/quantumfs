// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// quantumfsd is the central daemon of the filesystem
package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	"github.com/hanwen/go-fuse/fuse"
)

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk                  = iota
	exitBadCacheSize        = iota
	exitMountFail           = iota
	exitProfileFail         = iota
	exitDataStoreInitFail   = iota
	exitWorkspaceDbInitFail = iota
)

var version string

var cacheSizeString string
var cacheTimeNsecs uint
var memLogMegabytes uint
var config daemon.QuantumFsConfig
var showMaxSizes bool
var qflag *flag.FlagSet

func init() {
	const (
		defaultCachePath        = "/dev/shm/quantumfs"
		defaultCacheSize        = "24G"
		defaultMountPath        = "/qfs"
		defaultCacheTimeSeconds = 300
		defaultCacheTimeNsecs   = 0
		defaultMemLogMegabytes  = 500
	)

	fmt.Printf("QuantumFS version %s\n", version)

	qflag = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	qflag.StringVar(&config.CachePath, "cachePath", defaultCachePath,
		"Default location of the internal cache. Should be on a ramfs or "+
			"tmpfs filesystem to avoid random 1 second lag spikes")

	qflag.StringVar(&cacheSizeString, "cacheSize", defaultCacheSize,
		"Size of the local cache, e.g. 8G or 512M")

	qflag.StringVar(&config.MountPath, "mountpath", defaultMountPath,
		"Path to mount quantumfs at")

	qflag.Uint64Var(&config.CacheTimeSeconds, "cacheTimeSeconds",
		defaultCacheTimeSeconds,
		"Number of seconds the kernel will cache response data")

	qflag.UintVar(&cacheTimeNsecs, "cacheTimeNsecs", defaultCacheTimeNsecs,
		"Number of nanoseconds the kernel will cache response data")
	qflag.UintVar(&memLogMegabytes, "memLogMegabytes", defaultMemLogMegabytes,
		"The number of MB to allocate, total, to the shared memory log.")

	qflag.StringVar(&config.DataStoreName, "datastore", "processlocal",
		"Name of the datastore to use")
	qflag.StringVar(&config.DataStoreConf, "datastoreconf", "",
		"Options to pass to datastore")

	qflag.StringVar(&config.WorkspaceDbName, "workspaceDB", "processlocal",
		"Name of the WorkspaceDB to use")
	qflag.StringVar(&config.WorkspaceDbConf, "workspaceDBconf", "",
		"Options to pass to workspaceDB")

	qflag.BoolVar(&showMaxSizes, "showMaxSizes", false,
		"Show max block counts, metadata entries and max file sizes")
}

func maxSizes() {
	fmt.Printf("%30s %d\n", "MaxBlockSize",
		quantumfs.MaxBlockSize)
	fmt.Printf("%30s %d\n", "MaxBlocksLargeFile",
		quantumfs.MaxBlocksLargeFile())
	fmt.Printf("%30s %d\n", "MaxDirectoryRecords",
		quantumfs.MaxDirectoryRecords())
	fmt.Printf("%30s %d\n", "MaxNumExtendedAttributes",
		quantumfs.MaxNumExtendedAttributes())
	fmt.Printf("%30s %d\n", "MaxBlocksMediumFile",
		quantumfs.MaxBlocksMediumFile())
	fmt.Printf("%30s %d\n", "MaxPartsVeryLargeFile",
		quantumfs.MaxPartsVeryLargeFile())

	fmt.Printf("%30s %d\n", "MaxSmallFileSize",
		quantumfs.MaxSmallFileSize())
	fmt.Printf("%30s %d\n", "MaxMediumFileSize",
		quantumfs.MaxMediumFileSize())
	fmt.Printf("%30s %d\n", "MaxLargeFileSize",
		quantumfs.MaxLargeFileSize())
	fmt.Printf("%30s %d\n", "MaxVeryLargeFileSize",
		quantumfs.MaxVeryLargeFileSize())
}

func loadDatastore() {
	ds, err := thirdparty_backends.ConnectDatastore(config.DataStoreName,
		config.DataStoreConf)
	if err != nil {
		fmt.Printf("Datastore load failed\n")
		fmt.Printf("Error: %v\n", err)
		os.Exit(exitDataStoreInitFail)
	}
	config.DurableStore = ds
}

func loadWorkspaceDB() {
	wsdb, err := thirdparty_backends.ConnectWorkspaceDB(
		config.WorkspaceDbName, config.WorkspaceDbConf)
	if err != nil {
		fmt.Printf("WorkspaceDB load failed\n")
		fmt.Printf("Error: %v\n", err)
		os.Exit(exitWorkspaceDbInitFail)
	}
	config.WorkspaceDB = wsdb
}

// Process the command arguments. Will show the command usage if no arguments are
// given since the mount point is mandatory.
//
// Exit if processing failed
func processArgs() {
	qflag.Parse(os.Args[1:])

	if showMaxSizes {
		maxSizes()
		os.Exit(0)
	}

	if cacheSize, err := bytefmt.ToBytes(cacheSizeString); err != nil {
		os.Exit(exitBadCacheSize)
	} else {
		config.CacheSize = cacheSize
	}
	config.CacheTimeNsecs = uint32(cacheTimeNsecs)
	config.MemLogBytes = uint64(memLogMegabytes) * 1024 * 1024
	config.DirtyFlushDelay = 30 * time.Second

	loadDatastore()
	loadWorkspaceDB()
}

// Reduce the amount of "unused memory" QuantumFS uses when running as a
// daemon. Doubling memory use before running GC is an excessive amount of
// memory to use in the steady state.
//
// However, much of the GC time spent before QuantumFS has filled its
// readcache is wasted. Therefore we wait until QuantumFS has mostly
// filled its readcache and then set GCPercent to a value which will
// allow approximately 1GB of garbage to accrue before collection is
// triggered.
func reduceGCPercent(cacheSize uint64) {
	for {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)

		if memStats.HeapAlloc > cacheSize {
			// Approximately 1G seems a reasonable garbage to produce
			// before running garbage collection. Once we've surpassed
			// the configured cache size for total heap used, switch to
			// the more frequent garbage collection.
			oneGPercent := (100 * (1 * 1024 * 1024 * 1024)) / cacheSize
			fmt.Printf("Changing GCPercent to %d\n", oneGPercent)
			debug.SetGCPercent(int(oneGPercent))
			return
		}

		time.Sleep(1 * time.Minute)
	}
}

func main() {
	processArgs()

	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	go reduceGCPercent(config.CacheSize)

	var mountOptions = fuse.MountOptions{
		Name: "QuantumFS",
	}

	quantumfs := daemon.NewQuantumFs(config, "QuantumFs "+version)

	if quantumfs.Mount(mountOptions) != nil {
		os.Exit(exitMountFail)
	}
	quantumfs.Serve()
}
