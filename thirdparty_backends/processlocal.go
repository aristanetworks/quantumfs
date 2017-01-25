// +build !skip_backends

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// To avoid compiling in support for processlocal datastore,
// change "!skip_backends" in first line with "ignore"
package thirdparty_backends

import "github.com/aristanetworks/quantumfs/processlocal"

func init() {
	registerDatastore("processlocal", processlocal.NewDataStore)
	registerWorkspaceDB("processlocal", processlocal.NewWorkspaceDB)
}
