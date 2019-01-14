// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Supplement the golang typesystem and confirm that certain concrete types supply
// particular interfaces where there isn't already a strong dependency in the code.

import "testing"

// Confirm all the types which must be an inodeHolder really are
func TestInodeHolderTypes(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		assertIsInodeHolder := func(i interface{}) {
			_, ok := i.(inodeHolder)
			test.Assert(ok, "Type assertion failed")
		}

		assertIsInodeHolder(&Directory{})
		assertIsInodeHolder(&WorkspaceRoot{})
		assertIsInodeHolder(&TypespaceList{})
		assertIsInodeHolder(&NamespaceList{})
		assertIsInodeHolder(&WorkspaceList{})
	})
}
