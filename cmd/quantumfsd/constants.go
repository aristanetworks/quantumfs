// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Various constants used throughout quantumfsd
package main

// Various exist reasons, will be returned to the shell as an exit code
const (
	exitOk           = iota
	exitBadCacheSize = iota
)
