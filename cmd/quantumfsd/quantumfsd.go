// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// quantumfsd is the central daemon of the filesystem
package main

import "flag"
import "fmt"
import "time"
import "os"
import "runtime/debug"
import "runtime/pprof"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/daemon"
import "github.com/aristanetworks/quantumfs/thirdparty_backends"

import "github.com/hanwen/go-fuse/fuse"
import "code.cloudfoundry.org/bytefmt"

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
var cpuProfileFile string
var showMaxSizes bool

func init() {
	const (
		defaultCachePath        = "/dev/shm/quantumfs"
		defaultCacheSize        = "8G"
		defaultMountPath        = "/qfs"
		defaultCacheTimeSeconds = 1
		defaultCacheTimeNsecs   = 0
		defaultMemLogMegabytes  = 100
	)

	fmt.Printf("QuantumFS version %s\n", version)

	flag.StringVar(&config.CachePath, "cachePath", defaultCachePath,
		"Default location of the internal cache. Should be on a ramfs or "+
			"tmpfs filesystem to avoid random 1 second lag spikes")

	flag.StringVar(&cacheSizeString, "cacheSize", defaultCacheSize,
		"Size of the local cache, e.g. 8G or 512M")

	flag.StringVar(&config.MountPath, "mountpath", defaultMountPath,
		"Path to mount quantumfs at")

	flag.Uint64Var(&config.CacheTimeSeconds, "cacheTimeSeconds",
		defaultCacheTimeSeconds,
		"Number of seconds the kernel will cache response data")

	flag.UintVar(&cacheTimeNsecs, "cacheTimeNsecs", defaultCacheTimeNsecs,
		"Number of nanoseconds the kernel will cache response data")
	flag.UintVar(&memLogMegabytes, "memLogMegabytes", defaultMemLogMegabytes,
		"The number of MB to allocate, total, to the shared memory log.")

	flag.StringVar(&config.DataStoreName, "datastore", "processlocal",
		"Name of the datastore to use")
	flag.StringVar(&config.DataStoreConf, "datastoreconf", "",
		"Options to pass to datastore")

	flag.StringVar(&config.WorkspaceDbName, "workspaceDB", "processlocal",
		"Name of the WorkspaceDB to use")
	flag.StringVar(&config.WorkspaceDbConf, "workspaceDBconf", "",
		"Options to pass to workspaceDB")

	flag.StringVar(&cpuProfileFile, "profilePath", "",
		"File to write CPU Profiling data to")

	flag.BoolVar(&showMaxSizes, "showMaxSizes", false,
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
	flag.Parse()

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

func main() {
	processArgs()

	if cpuProfileFile != "" {
		profileFile, err := os.Create(cpuProfileFile)
		if err != nil {
			os.Exit(exitProfileFail)
		}
		pprof.StartCPUProfile(profileFile)
		defer pprof.StopCPUProfile()
	}

	// Reduce the amount of "unused memory" QuantumFS uses when running as a
	// daemon. Doubling memory use before running GC is an excessive amount of
	// memory to use.
	//
	// If we expect quantumfsd to consume about 30G of memory legitimately, then
	// a 10% increase is about 3G. We cannot use a significantly smaller constant
	// value because when we first start QuantumFS its memory usage will be tiny,
	// and, say, 1% of 1G results in constantly running GC and not making
	// substantial forward progress.
	debug.SetGCPercent(10)

	var mountOptions = fuse.MountOptions{
		AllowOther:    true,
		MaxBackground: 1024,
		MaxWrite:      quantumfs.MaxBlockSize,
		FsName:        "QuantumFS",
		Name:          "QuantumFS",
		Options:       make([]string, 0),
	}
	mountOptions.Options = append(mountOptions.Options, "suid")
	mountOptions.Options = append(mountOptions.Options, "dev")

	quantumfs := daemon.NewQuantumFs(config)
	err := quantumfs.Serve(mountOptions)
	if err != nil {
		os.Exit(exitMountFail)
	}
}
