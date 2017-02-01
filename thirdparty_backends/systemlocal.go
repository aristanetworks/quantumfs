// +build !skip_backends

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// To avoid compiling in support for the systemlocal backend
// change "!skip_backends" in first line to "ignore"
package thirdparty_backends

import "github.com/aristanetworks/quantumfs/systemlocal"

func init() {
	registerWorkspaceDB("systemlocal", systemlocal.NewWorkspaceDB)
}
