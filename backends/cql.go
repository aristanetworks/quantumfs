// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package backends

// Please see FEATURES in makefile.mk for details on how to toggle this feature.

import (
	"github.com/aristanetworks/quantumfs/backends/cql"
)

func init() {
	registerDatastore("cql.cql", cql.NewCqlStore)
	registerWorkspaceDB("cql.cql", cql.NewCqlWorkspaceDB)
}

func RegisterTestCqlBackend() {
	registerDatastore("cql.filesystem", cql.NewCqlFilesystemStore)
}
