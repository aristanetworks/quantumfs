// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// quantumfsd is the central daemon of the filesystem
package main

import "flag"
import "fmt"
import "time"
import "os"
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

var cacheSizeString string
var cacheTimeNsecs uint
var memLogMegabytes uint
var config daemon.QuantumFsConfig
var cpuProfileFile string
var showMaxSizes bool

func init() {
	const (
		defaultCachePath        = "/var/run/quantumfs"
		defaultCacheSize        = "8G"
		defaultMountPath        = "/qfs"
		defaultCacheTimeSeconds = 1
		defaultCacheTimeNsecs   = 0
		defaultMemLogMegabytes  = 100
	)

	flag.StringVar(&config.CachePath, "cachePath", defaultCachePath,
		"Default location of the internal cache. Should be on a ramfs or "+
			"tmpfs filsystem")

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
	for _, datastore := range thirdparty_backends.Datastores {
		if datastore.Name != config.DataStoreName {
			continue
		}

		config.DurableStore = datastore.Constructor(config.DataStoreConf)
		if config.DurableStore == nil {
			fmt.Printf("Datastore Constructor failed\n")
			os.Exit(exitDataStoreInitFail)
		} else {
			break
		}
	}
	if config.DurableStore == nil {
		fmt.Printf("Failed to find datastore '%s'\n", config.DataStoreName)
		os.Exit(exitDataStoreInitFail)
	}
}

func loadWorkspaceDB() {
	for _, db := range thirdparty_backends.WorkspaceDBs {
		if db.Name != config.WorkspaceDbName {
			continue
		}

		config.WorkspaceDB = db.Constructor(config.WorkspaceDbConf)
		if config.WorkspaceDB == nil {
			fmt.Printf("WorkspaceDB Constructor failed\n")
			os.Exit(exitWorkspaceDbInitFail)
		} else {
			break
		}
	}
	if config.WorkspaceDB == nil {
		fmt.Printf("Failed to find workspaceDB '%s'\n",
			config.WorkspaceDbName)
		os.Exit(exitWorkspaceDbInitFail)
	}
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
