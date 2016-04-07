// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// quantumfsd is the central daemon of the filesystem
package daemon

import "os"
import "arista.com/quantumfs"

import "github.com/hanwen/go-fuse/fuse"

func main() {
	processArgs()

	var mountOptions = fuse.MountOptions{
		AllowOther:    true,
		MaxBackground: 1024,
		MaxWrite:      quantumfs.MaxBlockSize,
		FsName:        "cluster",
		Name:          "quantumfs",
	}

	quantumfs := getInstance(config)
	server, err := fuse.NewServer(quantumfs, config.mountPath,
		&mountOptions)
	if err != nil {
		os.Exit(exitMountFail)
	}
	server.Serve()
}
