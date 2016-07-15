// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// quantumfsd is the central daemon of the filesystem
package main

import "flag"
import "os"
import "runtime/pprof"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/daemon"
import "github.com/aristanetworks/quantumfs/processlocal"

import "github.com/hanwen/go-fuse/fuse"
import "github.com/pivotal-golang/bytefmt"

// Various exist reasons, will be returned to the shell as an exit code
const (
	exitOk           = iota
	exitBadCacheSize = iota
	exitMountFail    = iota
	exitProfileFail  = iota
)

var cacheSizeString string
var cacheTimeNsecs uint
var config daemon.QuantumFsConfig
var cpuProfileFile string

func init() {
	const (
		defaultCachePath        = "/dev/shmem"
		defaultCacheSize        = "8G"
		defaultMountPath        = "/mnt/quantumfs"
		defaultRamFsPath	= "/var/run/quantumfs"
		defaultCacheTimeSeconds = 1
		defaultCacheTimeNsecs   = 0
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
	flag.StringVar(&config.RamFsPath, "ramfsPath", defaultRamFsPath,
		"Path to pre-existing ramfs directory")

	flag.StringVar(&cpuProfileFile, "profilePath", "",
		"File to write CPU Profiling data to")
}

// Process the command arguments. Will show the command usage if no arguments are
// given since the mount point is mandatory.
//
// Exit if processing failed
func processArgs() {
	flag.Parse()

	if cacheSize, err := bytefmt.ToBytes(cacheSizeString); err != nil {
		os.Exit(exitBadCacheSize)
	} else {
		config.CacheSize = cacheSize
	}
	config.CacheTimeNsecs = uint32(cacheTimeNsecs)

	config.WorkspaceDB = processlocal.NewWorkspaceDB()
	config.DurableStore = processlocal.NewDataStore()

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
		FsName:        "cluster",
		Name:          "quantumfs",
	}

	quantumfs := daemon.NewQuantumFs(config)
	err := quantumfs.Serve(mountOptions)
	if err != nil {
		os.Exit(exitMountFail)
	}
}
