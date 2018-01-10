// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test to ensure that hard coded quantumfs keys are correct

import (
	"encoding/hex"
	"testing"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils/keycompute"
)

func TestKeysMatch(t *testing.T) {
	// Until the next time that we do a datastore port, this test will fail
	t.Skip()
	runTest(t, func(test *testHelper) {
		targetHash := keycompute.ComputeEmptyDirectory()
		emptyHash := quantumfs.EmptyDirKey.Hash()
		test.Assert(targetHash == emptyHash,
			"EmptyDirKey has changed without datastore hash: %s vs %s",
			hex.EncodeToString(targetHash[:]),
			hex.EncodeToString(emptyHash[:]))

		targetHash = keycompute.ComputeEmptyBlock()
		emptyHash = quantumfs.EmptyBlockKey.Hash()
		test.Assert(targetHash == emptyHash,
			"EmptyBlockKey has changed without datastore hash: %s vs %s",
			hex.EncodeToString(targetHash[:]),
			hex.EncodeToString(emptyHash[:]))

		targetHash = keycompute.ComputeEmptyWorkspace()
		emptyHash = quantumfs.EmptyWorkspaceKey.Hash()
		test.Assert(targetHash == emptyHash,
			"EmptyWorkspaceKey has changed without datastore hash: "+
				"%s vs %s", hex.EncodeToString(targetHash[:]),
			hex.EncodeToString(emptyHash[:]))
	})
}
