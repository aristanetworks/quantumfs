// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import "testing"

func TestFailFastWalkPanicString(t *testing.T) {
	runTest(t, doPanicStringTest(false))
}

// TestFailFastWalkPanicErr verifies that walk aborts when
// panic(err) is generated from walkFunc .
func TestFailFastWalkPanicErr(t *testing.T) {
	runTest(t, doPanicErrTest(false))
}

// TestFailFastWalkLibraryPanicErr verifies that panic in walker
// goroutine aborts the walk.
func TestFailFastWalkLibraryPanicErr(t *testing.T) {
	runTest(t, doWalkLibraryPanicErrTest(false))
}

// TestFailFastWalkErr tests that Walk aborts when
// walkFunc returns an error.
func TestFailFastWalkErr(t *testing.T) {
	runTest(t, doWalkErrTest(false))
}

// TestFailFastHLGetErr tests that Walk aborts
// when data store get of hardlink fails.
func TestFailFastHLGetErr(t *testing.T) {
	runTest(t, doHLGetErrTest(false))
}

// TestFailFastDEGetErr tests that Walk aborts
// when data store get of directory entry fails.
func TestFailFastDEGetErr(t *testing.T) {
	runTest(t, doDEGetErrTest(false))
}

// TestFailFastEAGetErr tests that Walk aborts
// when data store get of xattr block fails.
func TestFailFastEAGetErr(t *testing.T) {
	runTest(t, doEAGetErrTest(false))
}

// TestFailFastEAAttrGetErr tests that Walk aborts
// when data store get of xattr attr block fails.
func TestFailFastEAAttrGetErr(t *testing.T) {
	runTest(t, doEAAttrGetErrTest(false))
}

// TestFailFastMultiBlockGetErr tests that Walk aborts
// when data store get of multiblock fails.
func TestFailFastMultiBlockGetErr(t *testing.T) {
	runTest(t, doMultiBlockGetErrTest(false))
}

// TestFailFastVLFileGetFirstErr tests that Walk aborts
// when get of first metdata block fails.
func TestFailFastVLFileGetFirstErr(t *testing.T) {
	runTest(t, doVLFileGetFirstErrTest(false))
}

// TestFailFastVLFileGetNextErr tests that Walk aborts
// when get of second level multiblock metadata
// block fails.
func TestFailFastVLFileGetNextErr(t *testing.T) {
	runTest(t, doVLFileGetNextErrTest(false))
}
