// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Various constants used throughout quantumfsd
package main

// Various exist reasons, will be returned to the shell as an exit code
const (
	exitOk           = iota
	exitBadCacheSize = iota
	exitMountFail    = iota
)

// Special reserved inode numbers
const (
	_                  = iota // Invalid
	inodeIdRoot        = iota // Same as fuse.FUSE_ROOT_ID
	inodeIdApi         = iota // /api file
	inodeId_null       = iota // /_null namespace
	inodeId_nullNull   = iota // /_null/null workspace
	inodeIdReservedEnd = iota // End of the reserved range
)

// Special reserved namespace/workspace names
const (
	apiPath       = "api" // File used for the qfs api
	nullNamespace = "_null"
	nullWorkspace = "null"
)
