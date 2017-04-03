// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Various constants used throughout quantumfsd
package daemon

import "github.com/aristanetworks/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// Mapping between datastore object types and the FUSE filetype
// Returns 0 if there is no valid FUSE filetype
func objectTypeToFileType(c *ctx, objectType quantumfs.ObjectType) uint32 {
	defer c.FuncIn("objectTypeToFileType", "type %d", objectType).out()

	switch objectType {
	case quantumfs.ObjectTypeSmallFile,
		quantumfs.ObjectTypeMediumFile,
		quantumfs.ObjectTypeLargeFile,
		quantumfs.ObjectTypeVeryLargeFile,
		quantumfs.ObjectTypeVCSFile,
		quantumfs.ObjectTypeBuildProduct: // Do we need recursive evaluation?
		return fuse.S_IFREG

	case quantumfs.ObjectTypeDirectoryEntry:
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
