// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package backends

// Please see FEATURES in makefile.mk for details on how to toggle this feature.

import (
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/backends/filesystem"
)

func init() {
	registerDatastore("cql.filesystem", filesystem.NewCqlFilesystemStore)
	registerDatastore("cql.cql", cql.NewCqlStore)
	registerWorkspaceDB("cql.cql", cql.NewCqlWorkspaceDB)
}
