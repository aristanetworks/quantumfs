// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Various constants used throughout quantumfsd

import (
	"syscall"

	"github.com/aristanetworks/quantumfs"
	"github.com/hanwen/go-fuse/fuse"
)

// Mapping between datastore object types and the FUSE filetype
// Returns 0 if there is no valid FUSE filetype
func objectTypeToFileType(c *ctx, objectType quantumfs.ObjectType) uint32 {
	defer c.FuncIn("objectTypeToFileType", "type %d", objectType).Out()
	return _objectTypeToFileType(c, objectType)
}

// Non-tracing version. This is useful where it is called many times from a
// well-known context, such that the function tracing isn't useful.
func _objectTypeToFileType(c *ctx, objectType quantumfs.ObjectType) uint32 {
	switch objectType {
	case quantumfs.ObjectTypeSmallFile,
		quantumfs.ObjectTypeMediumFile,
		quantumfs.ObjectTypeLargeFile,
		quantumfs.ObjectTypeVeryLargeFile,
		quantumfs.ObjectTypeVCSFile,
		quantumfs.ObjectTypeBuildProduct: // Do we need recursive evaluation?
		return fuse.S_IFREG

	case quantumfs.ObjectTypeDirectory:
		return fuse.S_IFDIR

	case quantumfs.ObjectTypeSymlink:
		return fuse.S_IFLNK

	case quantumfs.ObjectTypeHardlink:
		c.elog("Hardlink must be translated into the underlying type")
		return 0

	case quantumfs.ObjectTypeSpecial:
		// Note, Special isn't really a FIFO, but it's used as a signal to
		// the caller that the specialAttrOverride() function needs to be
		// called.
		return fuse.S_IFIFO

	case quantumfs.ObjectTypeExtendedAttribute,
		quantumfs.ObjectTypeWorkspaceRoot:
		return 0

	default:
		c.elog("Unknown object type to map to fuse: %d", objectType)
		return 0
	}
}

// The block size of the filesystem in bytes
const qfsBlockSize = uint64(quantumfs.MaxBlockSize)
const statBlockSize = uint64(512)

// This number empirically derived by looking at file sizes
const initBlockSize = 8192

// The following error message is not defined in go-fuse, therefore we
// define it here instead.
const ENAMETOOLONG = fuse.Status(syscall.ENAMETOOLONG)
