// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

// These tests do not fully stub out QuantumFS because that would cause a circular
// import and so only outputs the values as a manual testing aid.

import "fmt"
import "testing"

func TestMountPath(t *testing.T) {
	path := findApiPathMount()

	fmt.Println("TestMountPath:", path)
}

func TestMountEnvironment(t *testing.T) {
	path := findApiPathEnvironment()

	fmt.Println("TestMountEnvironment:", path)
}
