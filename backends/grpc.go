// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package backends

import "github.com/aristanetworks/quantumfs/backends/grpc"

func init() {
	registerWorkspaceDB("grpc", grpc.NewWorkspaceDB)
}
