// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package main

import (
	"github.com/aristanetworks/quantumfs/backends/grpc"
	"github.com/aristanetworks/quantumfs/qlogstats"
)

func init() {
	ses := []qlogstats.StatExtractor{
		newQfsExtPair(grpc.NumTypespaceLog, ""),
		newQfsExtPair(grpc.TypespaceListLog, ""),
		newQfsExtPair(grpc.NumNamespacesLog, ""),
		newQfsExtPair(grpc.NamespaceListLog, ""),
		newQfsExtPair(grpc.NumWorkspacesLog, ""),
		newQfsExtPair(grpc.WorkspaceListLog, ""),
		newQfsExtPair(grpc.BranchWorkspaceLog, ""),
		newQfsExtPair(grpc.DeleteWorkspaceLog, ""),
		newQfsExtPair(grpc.FetchWorkspaceLog, grpc.FetchWorkspaceDebug),
		newQfsExtPair(grpc.AdvanceWorkspaceLog, grpc.AdvanceWorkspaceDebug),
		newQfsExtPair(grpc.WorkspaceIsImmutableLog, ""),
		newQfsExtPair(grpc.SetWorkspaceImmutableLog,
			grpc.SetWorkspaceImmutableDebug),
	}

	extractors = append(extractors, ses...)
}
