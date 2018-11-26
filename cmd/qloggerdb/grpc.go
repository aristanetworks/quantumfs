// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"github.com/aristanetworks/quantumfs/grpc"
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
