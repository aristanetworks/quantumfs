// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// this is a library of tests which are common to both
// cached and uncached versions of workspace db API
// implementation

package cql

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
)

type wsdbCommonUnitTest struct {
	req      *require.Assertions
	wsdb     quantumfs.WorkspaceDB
	mockSess *MockSession
}

func (s *wsdbCommonUnitTest) TestEmptyDB() {

	nsRows := mockDbRows{{"_null"}}
	nsIter := new(MockIter)
	nsVals := []interface{}(nil)

	wsRows := mockDbRows{{"null"}}
	wsIter := new(MockIter)
	wsVals := []interface{}{"_null"}

	mockWsdbCacheNamespaceFetch(s.mockSess, nsRows, nsVals,
		nsIter, nil)
	mockWsdbCacheWorkspaceFetch(s.mockSess, wsRows, wsVals,
		wsIter, nil)

	// cached APIs
	nsCount := s.wsdb.NumNamespaces(nil)
	s.req.Equal(1, nsCount, "Empty DB has incorrect count of namespaces")

	nsIter.Reset()
	wsIter.Reset()

	nsList := s.wsdb.NamespaceList(nil)
	s.req.Equal([]string{"_null"}, nsList,
		"Empty DB has incorrect list of namespaces")

	nsIter.Reset()
	wsIter.Reset()

	wsCount := s.wsdb.NumWorkspaces(nil, "_null")
	s.req.Equal(1, wsCount, "Empty DB has incorrect count of workspaces")

	nsIter.Reset()
	wsIter.Reset()

	wsList := s.wsdb.WorkspaceList(nil, "_null")
	s.req.Equal([]string{"null"}, wsList,
		"Empty DB has incorrect list of workspaces")

	// basic uncached APIs
	mockDbNamespaceGet(s.mockSess, "_null", nil)

	exists := s.wsdb.NamespaceExists(nil, "_null")
	s.req.True(exists, "Expected namespace doesn't exist in empty DB")

	mockWsdbKeyGet(s.mockSess, "_null", "null",
		quantumfs.EmptyWorkspaceKey.Value(), nil)

	exists = s.wsdb.WorkspaceExists(nil, "_null", "null")
	s.req.True(exists, "Failed to find expected workspace in empty DB")

	key := s.wsdb.Workspace(nil, "_null", "null")
	s.req.True(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
		"null workspace isn't empty")

}

func (s *wsdbCommonUnitTest) TestBranching() {

	// test branching from invalid namespace or workspace
	mockWsdbKeyGet(s.mockSess, "notthere", "a",
		nil, gocql.ErrNotFound)

	err := s.wsdb.BranchWorkspace(nil, "notthere", "a", "somewhere", "else")
	s.req.Error(err, "Succeeded branching invalid namespace")
	s.req.IsType(&quantumfs.WorkspaceDbErr{}, err, "Invalid error type %T", err)

	// test branching from namespace and workspace in empty DB
	mockWsdbKeyGet(s.mockSess, "_null", "null",
		quantumfs.EmptyWorkspaceKey.Value(), nil)
	mockWsdbKeyGet(s.mockSess, "test", "a",
		nil, gocql.ErrNotFound)

	mockWsdbKeyPut(s.mockSess, "test", "a",
		quantumfs.EmptyWorkspaceKey.Value(), nil)

	err = s.wsdb.BranchWorkspace(nil, "_null", "null", "test", "a")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	// test branching to an existing workspace
	mockWsdbKeyGet(s.mockSess, "_null", "null",
		quantumfs.EmptyWorkspaceKey.Value(), nil)
	// mockDbKeyGets within a test use same session and
	// hence the expected mock Calls are ordered.
	// using "test" and "a" will cause previous mocked action
	// to occur instead of current. Hence using "test1" and "a1"
	mockWsdbKeyGet(s.mockSess, "test1", "a1",
		quantumfs.EmptyWorkspaceKey.Value(), nil)

	err = s.wsdb.BranchWorkspace(nil, "_null", "null", "test1", "a1")
	s.req.Error(err, "Succeeded branching to existing workspace")

	// test branching from non-null workspace
	mockWsdbKeyGet(s.mockSess, "test2", "a2",
		quantumfs.EmptyWorkspaceKey.Value(), nil)
	mockWsdbKeyGet(s.mockSess, "test3", "b3",
		nil, gocql.ErrNotFound)
	mockWsdbKeyPut(s.mockSess, "test3", "b3",
		quantumfs.EmptyWorkspaceKey.Value(), nil)

	err = s.wsdb.BranchWorkspace(nil, "test2", "a2", "test3", "b3")
	s.req.NoError(err, "Error rebranching workspace: %v", err)
}

func (s *wsdbCommonUnitTest) TestAdvanceOk() {

	// test successful advance of a branched workspace
	mockWsdbKeyGet(s.mockSess, "test", "a",
		quantumfs.EmptyWorkspaceKey.Value(), nil)
	mockWsdbKeyPut(s.mockSess, "test", "a",
		quantumfs.EmptyDirKey.Value(), nil)

	newRootID, err := s.wsdb.AdvanceWorkspace(nil, "test", "a",
		quantumfs.EmptyWorkspaceKey, quantumfs.EmptyDirKey)

	s.req.NoError(err, "Error when advancing root: %v", err)
	s.req.True(newRootID.IsEqualTo(quantumfs.EmptyDirKey),
		"New root doesn't match")
}

func (s *wsdbCommonUnitTest) TestAdvanceOutOfDate() {

	// test advance of a branched workspace whose current rootID
	// has changed

	mockWsdbKeyGet(s.mockSess, "test", "a",
		quantumfs.EmptyWorkspaceKey.Value(), nil)

	_, err := s.wsdb.AdvanceWorkspace(nil, "test", "a",
		quantumfs.EmptyDirKey, quantumfs.EmptyDirKey)

	s.req.Error(err, "Succeeded advancing out-of-date workspace")
	s.req.IsType(&quantumfs.WorkspaceDbErr{},
		err, "Invalid error type %T", err)
}

func (s *wsdbCommonUnitTest) TestAdvanceNotExist() {
	// test advance of a non-existant workspace

	mockWsdbKeyGet(s.mockSess, "test", "a",
		nil, gocql.ErrNotFound)

	_, err := s.wsdb.AdvanceWorkspace(nil, "test", "a",
		quantumfs.EmptyWorkspaceKey, quantumfs.EmptyDirKey)

	s.req.Error(err, "Succeeded advancing non-existant workspace")
	s.req.IsType(&quantumfs.WorkspaceDbErr{},
		err, "Invalid error type %T", err)
}

func (s *wsdbCommonUnitTest) TestNamespaceNotExist() {

	mockDbNamespaceGet(s.mockSess, "doesntexist", gocql.ErrNotFound)

	exists := s.wsdb.NamespaceExists(nil, "doesntexist")
	s.req.False(exists, "Unexpected namespace exists in empty DB")
}
