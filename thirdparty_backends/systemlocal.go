// +build !skip_backends

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package thirdparty_backends

// To avoid compiling in support for the systemlocal backend
// change "!skip_backends" in first line to "ignore"
//
import "github.com/aristanetworks/quantumfs/systemlocal"

func init() {
	registerWorkspaceDB("systemlocal", systemlocal.NewWorkspaceDB)
}
