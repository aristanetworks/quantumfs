// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package grpc

// Tests of the helper functions

import (
	"testing"
)

func TestMaybeAddPort(t *testing.T) {
	t.Parallel()

	run := func(in string, out string) {
		result := maybeAddPort(in)

		if result != out {
			t.Fatalf("%s -> %s got %s", in, out, result)
		}
	}

	run("1.2.3.4:1234", "1.2.3.4:1234")
	run("1.2.3.4", "1.2.3.4:2222")
	run("hostname:1234", "hostname:1234")
	run("hostname", "hostname:2222")
	run("[1:2:3::4]:1234", "[1:2:3::4]:1234")
	run("[1:2:3::4]", "[1:2:3::4]:2222")
}
