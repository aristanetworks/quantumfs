// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// quantumfsd is the central daemon of the filesystem
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk = iota
	exitBadCacheSize
	exitMountFail
	exitProfileFail
	exitDataStoreInitFail
	exitWorkspaceDbInitFail
	exitInitFail
	exitShutdownFail
)

var version string

var cacheSizeString string
var cacheTimeNsecs uint
var memLogMegabytes uint
var showMaxSizes bool
var configFile string

var qflag *flag.FlagSet
var config daemon.QuantumFsConfig

func init() {
	// Defaults
	config = daemon.QuantumFsConfig{
		CachePath:        "/dev/shm/quantumfs",
		CacheSize:        24 * 1024 * 1024 * 1024,
		MountPath:        "/qfs",
		DataStoreName:    "processlocal",
		DataStoreConf:    "",
		WorkspaceDbName:  "processlocal",
		WorkspaceDbConf:  "",
		CacheTimeSeconds: 3600,
		CacheTimeNsecs:   0,
		DirtyFlushDelay:  daemon.Duration{Duration: 30 * time.Second},
		MemLogBytes:      500 * 1024 * 1024,
		VerboseTracing:   true,
	}

	const (
		defaultCacheSize = "24G"
	)

	fmt.Printf("QuantumFS version %s\n", version)

	qflag = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	qflag.StringVar(&configFile, "config", "",
		"JSON file to load configuration from")

	qflag.StringVar(&config.CachePath, "cachePath", config.CachePath,
		"Location of the internal cache. Should be on a ramfs or "+
			"tmpfs filesystem to avoid random 1 second lag spikes")

	qflag.StringVar(&cacheSizeString, "cacheSize", defaultCacheSize,
		"Size of the local cache, e.g. 8G or 512M")

	qflag.StringVar(&config.MountPath, "mountpath", config.MountPath,
		"Path to mount quantumfs at")

	qflag.Uint64Var(&config.CacheTimeSeconds, "cacheTimeSeconds",
		config.CacheTimeSeconds,
		"Number of seconds the kernel will cache response data")
	qflag.UintVar(&cacheTimeNsecs, "cacheTimeNsecs", uint(config.CacheTimeNsecs),
		"Number of nanoseconds the kernel will cache response data")

	qflag.DurationVar(&config.DirtyFlushDelay.Duration, "dirtyFlushDelay",
		config.DirtyFlushDelay.Duration,
		"Number of seconds to delay flushing dirty inodes")

	qflag.UintVar(&memLogMegabytes, "memLogMegabytes",
		uint(config.MemLogBytes/(1024*1024)),
		"The number of MB to allocate, total, to the shared memory log.")

	qflag.StringVar(&config.DataStoreName, "datastore", config.DataStoreName,
		"Name of the datastore to use")
	qflag.StringVar(&config.DataStoreConf, "datastoreconf", config.DataStoreConf,
		"Options to pass to datastore")

	qflag.StringVar(&config.WorkspaceDbName, "workspaceDB",
		config.WorkspaceDbName, "Name of the WorkspaceDB to use")
	qflag.StringVar(&config.WorkspaceDbConf, "workspaceDBconf",
		config.WorkspaceDbConf, "Options to pass to workspaceDB")

	qflag.BoolVar(&showMaxSizes, "showMaxSizes", false,
		"Show max block counts, metadata entries and max file sizes")

	qflag.BoolVar(&config.VerboseTracing, "verboseTracing",
		config.VerboseTracing, "Enable verbose qlog tracing")
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

func parseConfigFile() {
	confFlag := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	confFlag.Usage = func() {} // Be silent
	confFlag.StringVar(&configFile, "config", "", "")
	confFlag.Parse(os.Args[1:])

	if configFile != "" {
		file, err := os.Open(configFile)
		if err != nil {
			fmt.Printf("Error opening configuration file: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			fmt.Printf("Error getting file size: %v\n", err)
			os.Exit(1)
		}

		conf := make([]byte, info.Size())
		err = json.Unmarshal(conf, &config)
		if err != nil {
			fmt.Printf("Error parsing config file: %v\n", err)
			os.Exit(1)
		}
	}
}

// Process the command arguments. Will show the command usage if no arguments are
// given since the mount point is mandatory.
//
// Exit if processing failed
func processArgs() {
	// First load any configuration file so the command line arguments can
	// override those values if available.
	parseConfigFile()

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
			// Approximately 5G seems a reasonable garbage to produce
			// before running garbage collection. Once we've surpassed
			// the configured cache size for total heap used, switch to
			// the more frequent garbage collection.
			gcPercent := (100 * (5 * 1024 * 1024 * 1024)) / cacheSize
			fmt.Printf("Changing GCPercent to %d\n", gcPercent)
			debug.SetGCPercent(int(gcPercent))
			return
		}

		time.Sleep(1 * time.Minute)
	}
}

func main() {
	utils.CheckForRecursiveRLock = false

	processArgs()

	utils.ServePprof()

	go reduceGCPercent(config.CacheSize)

	var mountOptions = fuse.MountOptions{
		Name: "QuantumFS",
	}

	quantumfs, err := daemon.NewQuantumFs(config, "QuantumFs "+version)

	if err != nil {
		fmt.Printf("Could not initilize quantumfs: %s\n", err.Error())
		os.Exit(exitInitFail)

	}

	if quantumfs.Mount(mountOptions) != nil {
		os.Exit(exitMountFail)
	}
	quantumfs.Serve()
	if err := quantumfs.Shutdown(); err != nil {
		os.Exit(exitShutdownFail)
	}
}
