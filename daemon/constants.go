// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Various constants used throughout quantumfsd
package daemon

import "fmt"

import "arista.com/quantumfs"
import "github.com/hanwen/go-fuse/fuse"

// Mapping between datastore object types and the FUSE filetype
// Returns 0 if there is no valid FUSE filetype
func objectTypeToFileType(objectType quantumfs.ObjectType) uint32 {
	switch objectType {
	case quantumfs.ObjectTypeSmallFile,
		quantumfs.ObjectTypeMediumFile,
		quantumfs.ObjectTypeLargeFile,
		quantumfs.ObjectTypeVeryLargeFile,
		quantumfs.ObjectTypeHardlink,
		quantumfs.ObjectTypeVCSFile,
		quantumfs.ObjectTypeBuildArtifact: // Do we need recursive evaluation?
		return fuse.S_IFREG

	case quantumfs.ObjectTypeDirectoryEntry:
		return fuse.S_IFDIR

	case quantumfs.ObjectTypeSymlink:
		return fuse.S_IFLNK

	case quantumfs.ObjectTypeExtendedAttribute,
		quantumfs.ObjectTypeWorkspaceRoot:
		return 0

	default:
		fmt.Println("Unknown object type to map to fuse:", objectType)
		return 0
	}
}

// The block size of the filesystem in bytes
const qfsBlockSize = 4096
