// Copyright (c) 2018 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package systemlocal

import (
	"testing"

	"github.com/aristanetworks/quantumfs"
)

func TestObjectPath(t *testing.T) {
	runTest(t, func(test *testHelper) {
		hash := [quantumfs.HashSize]byte{
			0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9,
			0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0xb0, 0xb1, 0xb2, 0xb3}
		key := quantumfs.NewObjectKey(quantumfs.KeyTypeData, hash)

		path := objectPath("root/", key)

		test.Assert(path ==
			"root/a0/a1/05a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3",
			"Path is incorrect: %s", path)

	})
}
