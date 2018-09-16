// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import (
	"testing"
)

// This file contains tests which test walker's
// error handling aspects.

// TestWalkPanicString verifies that panic(string) generated from
// walkFunc is available as error from Walk.
func TestWalkPanicString(t *testing.T) {
	runTest(t, doPanicStringTest(false))
}

// TestWalkPanicErr verifies that panic(err) generated from
// walkFunc is available as error from Walk.
func TestWalkPanicErr(t *testing.T) {
	runTest(t, doPanicErrTest(false))
}

// TestWalkErr tests if the error returned from
// walkFunc is returned from Walk.
func TestWalkErr(t *testing.T) {
	runTest(t, doWalkErrTest(false))
}

// TestHLGetErr tests that Walk aborts
// when data store get of hardlink fails.
func TestHLGetErr(t *testing.T) {
	runTest(t, doHLGetErrTest(false))
}

// TestDEGetErr tests that Walk aborts
// when data store get of directory entry fails.
func TestDEGetErr(t *testing.T) {
	runTest(t, doDEGetErrTest(false))
}

// TestEAGetErr tests that Walk aborts
// when data store get of xattr block fails.
func TestEAGetErr(t *testing.T) {
	runTest(t, doEAGetErrTest(false))
}

// TestEAAttrGetErr tests that Walk aborts
// when data store get of xattr attr block fails.
func TestEAAttrGetErr(t *testing.T) {
	runTest(t, doEAAttrGetErrTest(false))
}

// TestMultiBlockGetErr tests that Walk aborts
// when data store get of multiblock fails.
func TestMultiBlockGetErr(t *testing.T) {
	runTest(t, doMultiBlockGetErrTest(false))
}

// TestVLFileGetFirstErr tests that Walk aborts
// when get of first metdata block fails.
func TestVLFileGetFirstErr(t *testing.T) {
	runTest(t, doVLFileGetFirstErrTest(false))
}

// TestVLFileGetNextErr tests that Walk aborts
// when get of second level multiblock metadata
// block fails.
func TestVLFileGetNextErr(t *testing.T) {
	runTest(t, doVLFileGetNextErrTest(false))
}
