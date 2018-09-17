// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package walker

import (
	"testing"
)

// This file contains tests which verify walker's
// best-effort behaviour.

// TestBestEffortWalkPanicString verifies that walk continues when
// panic(string) is generated from walkFunc.
func TestBestEffortWalkPanicString(t *testing.T) {
	runTest(t, doPanicStringTest(true))
}

// TestBestEffortWalkPanicErr verifies that walk continues when
// panic(err) is generated from walkFunc .
func TestBestEffortWalkPanicErr(t *testing.T) {
	runTest(t, doPanicErrTest(true))
}

// TestBestEffortWalkErr tests that Walk continues when
// walkFunc returns an error.
func TestBestEffortWalkErr(t *testing.T) {
	runTest(t, doWalkErrTest(true))
}

// TestBestEffortHLGetErr tests that Walk continues
// when data store get of hardlink fails.
func TestBestEffortHLGetErr(t *testing.T) {
	runTest(t, doHLGetErrTest(true))
}

// TestBestEffortDEGetErr tests that Walk continues
// when data store get of directory entry fails.
func TestBestEffortDEGetErr(t *testing.T) {
	runTest(t, doDEGetErrTest(true))
}

// TestBestEffortEAGetErr tests that Walk continues
// when data store get of xattr block fails.
func TestBestEffortEAGetErr(t *testing.T) {
	runTest(t, doEAGetErrTest(true))
}

// TestBestEffortEAAttrGetErr tests that Walk continues
// when data store get of xattr attr block fails.
func TestBestEffortEAAttrGetErr(t *testing.T) {
	runTest(t, doEAAttrGetErrTest(true))
}

// TestBestEffortMultiBlockGetErr tests that Walk continues
// when data store get of multiblock fails.
func TestBestEffortMultiBlockGetErr(t *testing.T) {
	runTest(t, doMultiBlockGetErrTest(true))
}

// TestBestEffortVLFileGetFirstErr tests that Walk continues
// when get of first metdata block fails.
func TestBestEffortVLFileGetFirstErr(t *testing.T) {
	runTest(t, doVLFileGetFirstErrTest(true))
}

// TestBestEffortVLFileGetNextErr tests that Walk continues
// when get of second level multiblock metadata
// block fails.
func TestBestEffortVLFileGetNextErr(t *testing.T) {
	runTest(t, doVLFileGetNextErrTest(true))
}
