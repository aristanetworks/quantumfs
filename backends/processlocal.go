// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package backends

import "github.com/aristanetworks/quantumfs/backends/processlocal"

func init() {
	registerDatastore("processlocal", processlocal.NewDataStore)
	registerWorkspaceDB("processlocal", processlocal.NewWorkspaceDB)
	registerTimeSeriesDB("processlocal", processlocal.NewMemdb)
}
