// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// quantumfsd is the central daemon of the filesystem
package main

import "os"
import "arista.com/quantumfs"

import "github.com/hanwen/go-fuse/fuse"
import "github.com/hanwen/go-fuse/fuse/nodefs"

func main() {
	processArgs()

	var mountOptions = fuse.MountOptions{
		AllowOther:    true,
		MaxBackground: 1024,
		MaxWrite:      quantumfs.MaxBlockSize,
		FsName:        "cluster",
		Name:          "quantumfs",
	}

	filesystem := NewQuantumfs()
	connector := nodefs.NewFileSystemConnector(filesystem, nil)
	server, err := fuse.NewServer(connector.RawFS(), config.mountPath,
		&mountOptions)
	if err != nil {
		os.Exit(exitMountFail)
	}
	server.Serve()
}
