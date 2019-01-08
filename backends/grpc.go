// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package backends

import "github.com/aristanetworks/quantumfs/backends/grpc"

func init() {
	registerWorkspaceDB("grpc", grpc.NewWorkspaceDB)
}
