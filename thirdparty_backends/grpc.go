// +build !skip_backends

// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package thirdparty_backends

// To avoid compiling in support for the grpc backend change "!skip_backends" in
// first line to "ignore"
//
import "github.com/aristanetworks/quantumfs/grpc"

func init() {
	registerWorkspaceDB("grpc", grpc.NewWorkspaceDB)
}
