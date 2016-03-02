// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The datastore interface
package quantumfs

// Maximum size of a block which can be stored in a datastore
const MaxBlockSize = 1 * 1024 * 1024

// Maximum length of a filename
const MaxFilenameLength = 256

// Special reserved namespace/workspace names
const (
	ApiPath           = "api" // File used for the qfs api
	NullNamespaceName = "_null"
	NullWorkspaceName = "null"
)

// Special reserved inode numbers
const (
	_                  = iota // Invalid
	InodeIdRoot        = iota // Same as fuse.FUSE_ROOT_ID
	InodeIdApi         = iota // /api file
	InodeId_null       = iota // /_null namespace
	InodeId_nullNull   = iota // /_null/null workspace
	InodeIdReservedEnd = iota // End of the reserved range
)
