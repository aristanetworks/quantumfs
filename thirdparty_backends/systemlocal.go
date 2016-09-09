// +build linux

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Enable this file by removing "ignore" from the first line with "linux" if you want
// to compile in support for the systemlocal subsystems.
package thirdparty_backends

import "github.com/aristanetworks/quantumfs/systemlocal"

func init() {
	registerWorkspaceDB("systemlocal", systemlocal.NewWorkspaceDB)
}
