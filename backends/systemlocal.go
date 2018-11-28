// +build !skip_backends

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package backends

// To avoid compiling in support for the systemlocal backend
// change "!skip_backends" in first line to "ignore"
//
import "github.com/aristanetworks/quantumfs/backends/systemlocal"

func init() {
	registerDatastore("systemlocal", systemlocal.NewDataStore)
	registerWorkspaceDB("systemlocal", systemlocal.NewWorkspaceDB)
}
