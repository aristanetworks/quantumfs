// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package daemon

// Test the Low memory mode marker file

import (
	"bytes"
	"io"
	"os"
	"syscall"
	"testing"

	"github.com/aristanetworks/quantumfs"
)

func lowMemPath(test *testHelper) string {
	return test.NewWorkspace() + "/" + quantumfs.LowMemFileName
}

func TestLowMemExist(t *testing.T) {
	runTest(t, func(test *testHelper) {
		lowmemName := lowMemPath(test)

		var stat syscall.Stat_t
		test.AssertErr(syscall.Stat(lowmemName, &stat))

		test.qfs.inLowMemoryMode = true

		test.WaitFor("lowmem file to appear", func() bool {
			var stat syscall.Stat_t
			err := syscall.Stat(lowmemName, &stat)
			return err == nil
		})
	})
}

func TestLowMemRead(t *testing.T) {
	runTest(t, func(test *testHelper) {
		lowmemName := lowMemPath(test)
		test.qfs.inLowMemoryMode = true
		buf := make([]byte, 1024)

		test.WaitFor("lowmem file to appear", func() bool {
			var stat syscall.Stat_t
			err := syscall.Stat(lowmemName, &stat)
			return err == nil
		})

		file, err := os.Open(lowmemName)
		test.AssertNoErr(err)
		defer file.Close()

		// Read from beginning
		numRead, err := file.ReadAt(buf, 0)
		test.Assert(err == io.EOF, "Unexpected error: %s", err.Error())
		test.Assert(numRead == len(lowMemDescription),
			"Wrong number of bytes read %d vs %d", numRead,
			len(lowMemDescription))
		test.Assert(bytes.Equal(buf[:numRead], lowMemDescription),
			"Read bytes incorrect %s", buf)

		// Read starting part way through
		offset := 10
		numRead, err = file.ReadAt(buf, int64(offset))
		test.Assert(err == io.EOF, "Unexpected error: %s", err.Error())
		test.Assert(numRead == len(lowMemDescription)-offset,
			"Wrong number of bytes read %d vs %d", numRead,
			len(lowMemDescription)-offset)
		test.Assert(bytes.Equal(buf[:numRead], lowMemDescription[offset:]),
			"Read bytes incorrect")

		// Partial read in the middle
		start_offset := 10
		end_offset := len(lowMemDescription) - 10
		buf = buf[:end_offset-start_offset]
		numRead, err = file.ReadAt(buf, int64(start_offset))
		test.AssertNoErr(err)
		test.Assert(numRead == len(lowMemDescription)-20,
			"Wrong number of bytes read %d vs %d", numRead,
			len(lowMemDescription)-20)
		test.Assert(bytes.Equal(buf[:numRead],
			lowMemDescription[start_offset:end_offset]),
			"Read bytes incorrect")
	})
}
