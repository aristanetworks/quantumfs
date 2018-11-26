// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package backends

import "github.com/aristanetworks/quantumfs/backends/processlocal"

func init() {
	registerDatastore("processlocal", processlocal.NewDataStore)
	registerWorkspaceDB("processlocal", processlocal.NewWorkspaceDB)
	registerTimeSeriesDB("processlocal", processlocal.NewMemdb)
}
