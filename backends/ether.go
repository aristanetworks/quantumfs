// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package backends

// Please see FEATURES in makefile.mk for details on how to toggle this feature.

import (
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/backends/filesystem"
)

func init() {
	registerDatastore("ether.filesystem", filesystem.NewEtherFilesystemStore)
	registerDatastore("ether.cql", cql.NewEtherCqlStore)
	registerWorkspaceDB("ether.cql", cql.NewEtherWorkspaceDB)
}
