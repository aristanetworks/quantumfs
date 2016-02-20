// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Central configuration object and handling

package main

import "os"
import "flag"

import "github.com/pivotal-golang/bytefmt"

type QuantumFsConfig struct {
	cachePath string
	cacheSize uint64
	mountPath string
}

var cacheSizeString string
var config QuantumFsConfig

func init() {
	const (
		defaultCachePath = "/dev/shmem"
		defaultCacheSize = "8G"
		defaultMountPath = "/mnt/quantumfs"
	)

	flag.StringVar(&config.cachePath, "cachePath", defaultCachePath,
		"Default location of the internal cache. Should be on a ramfs or "+
			"tmpfs filsystem")
	flag.StringVar(&cacheSizeString, "cacheSize", defaultCacheSize,
		"Size of the local cache, e.g. 8G or 512M")
	flag.StringVar(&config.mountPath, "mountpath", defaultMountPath,
		"Path to mount quantumfs at")
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
		config.cacheSize = cacheSize
	}

}
