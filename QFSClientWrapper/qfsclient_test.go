// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclient

import "testing"

func TestDevelopment(t *testing.T) {
	api, err := GetApi()
	if api.handle == 1000 || err != nil {
		t.Fatalf("blah")
	}
}
