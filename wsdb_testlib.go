// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// this is a library of tests which are common to both
// cached and uncached versions of workspace db API
// implementation

package cql

import (
	"bytes"

	"github.com/aristanetworks/ether/qubit/wsdb"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
)

type wsdbCommonUnitTest struct {
	req      *require.Assertions
	wsdb     wsdb.WorkspaceDB
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
	nsCount, err1 := s.wsdb.NumNamespaces()
	s.req.NoError(err1, "NumNamespace failed: %s", err1)
	s.req.Equal(1, nsCount, "Empty DB has incorrect count of namespaces")

	nsIter.Reset()
	wsIter.Reset()

	nsList, err2 := s.wsdb.NamespaceList()
	s.req.NoError(err2, "NamespaceList failed: %s", err2)
	s.req.Equal([]string{"_null"}, nsList,
		"Empty DB has incorrect list of namespaces")

	nsIter.Reset()
	wsIter.Reset()

	wsCount, err3 := s.wsdb.NumWorkspaces("_null")
	s.req.NoError(err3, "NumWorkspaces failed: %s", err3)
	s.req.Equal(1, wsCount, "Empty DB has incorrect count of workspaces")

	nsIter.Reset()
	wsIter.Reset()

	wsList, err4 := s.wsdb.WorkspaceList("_null")
	s.req.NoError(err4, "WorkspaceList failed: %s", err4)
	s.req.Equal([]string{"null"}, wsList,
		"Empty DB has incorrect list of workspaces")

	// basic uncached APIs
	mockDbNamespaceGet(s.mockSess, "_null", nil)

	exists, err5 := s.wsdb.NamespaceExists("_null")
	s.req.NoError(err5, "NamespaceExists failed: %s", err5)
	s.req.True(exists, "Expected namespace doesn't exist in empty DB")

	mockWsdbKeyGet(s.mockSess, "_null", "null", []byte(nil), nil)

	exists, err6 := s.wsdb.WorkspaceExists("_null", "null")
	s.req.NoError(err6, "WorkspaceExists failed: %s", err6)
	s.req.True(exists, "Failed to find expected workspace in empty DB")

	key, err7 := s.wsdb.Workspace("_null", "null")
	s.req.NoError(err7, "Workspace failed: %s", err7)
	s.req.True(bytes.Equal(key, []byte(nil)), "null workspace isn't empty")

}

func (s *wsdbCommonUnitTest) TestBranching() {

	// test branching from invalid namespace or workspace
	mockWsdbKeyGet(s.mockSess, "notthere", "a",
		nil, gocql.ErrNotFound)

	err := s.wsdb.BranchWorkspace("notthere", "a", "somewhere", "else")
	s.req.Error(err, "Succeeded branching invalid namespace")
	s.req.IsType(&wsdb.Error{}, err, "Invalid error type %T", err)

	// test branching from namespace and workspace in empty DB
	mockWsdbKeyGet(s.mockSess, "_null", "null", []byte(nil), nil)
	mockWsdbKeyGet(s.mockSess, "test", "a", nil, gocql.ErrNotFound)

	mockWsdbKeyPut(s.mockSess, "test", "a", []byte(nil), nil)

	err = s.wsdb.BranchWorkspace("_null", "null", "test", "a")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	// test branching to an existing workspace
	mockWsdbKeyGet(s.mockSess, "_null", "null", []byte(nil), nil)
	// mockDbKeyGets within a test use same session and
	// hence the expected mock Calls are ordered.
	// using "test" and "a" will cause previous mocked action
	// to occur instead of current. Hence using "test1" and "a1"
	mockWsdbKeyGet(s.mockSess, "test1", "a1", []byte(nil), nil)

	err = s.wsdb.BranchWorkspace("_null", "null", "test1", "a1")
	s.req.Error(err, "Succeeded branching to existing workspace")

	// test branching from non-null workspace
	mockWsdbKeyGet(s.mockSess, "test2", "a2", []byte(nil), nil)
	mockWsdbKeyGet(s.mockSess, "test3", "b3", nil, gocql.ErrNotFound)
	mockWsdbKeyPut(s.mockSess, "test3", "b3", []byte(nil), nil)

	err = s.wsdb.BranchWorkspace("test2", "a2", "test3", "b3")
	s.req.NoError(err, "Error rebranching workspace: %v", err)
}

func (s *wsdbCommonUnitTest) TestAdvanceOk() {

	// test successful advance of a branched workspace
	mockWsdbKeyGet(s.mockSess, "test", "a", []byte(nil), nil)
	mockWsdbKeyPut(s.mockSess, "test", "a", []byte{1, 2, 3}, nil)

	newRootID, err := s.wsdb.AdvanceWorkspace("test", "a",
		[]byte(nil), []byte{1, 2, 3})

	s.req.NoError(err, "Error when advancing root: %v", err)
	s.req.True(bytes.Equal(newRootID, []byte{1, 2, 3}),
		"New root doesn't match")
}

func (s *wsdbCommonUnitTest) TestAdvanceOutOfDate() {

	// test advance of a branched workspace whose current rootID
	// has changed

	mockWsdbKeyGet(s.mockSess, "test", "a", []byte(nil), nil)

	newKey := []byte{1, 2, 3}
	_, err := s.wsdb.AdvanceWorkspace("test", "a",
		newKey, newKey)

	s.req.Error(err, "Succeeded advancing out-of-date workspace")
	s.req.IsType(&wsdb.Error{},
		err, "Invalid error type %T", err)
}

func (s *wsdbCommonUnitTest) TestAdvanceNotExist() {
	// test advance of a non-existant workspace

	mockWsdbKeyGet(s.mockSess, "test", "a",
		nil, gocql.ErrNotFound)

	_, err := s.wsdb.AdvanceWorkspace("test", "a",
		[]byte(nil), []byte(nil))

	s.req.Error(err, "Succeeded advancing non-existant workspace")
	s.req.IsType(&wsdb.Error{},
		err, "Invalid error type %T", err)
}

func (s *wsdbCommonUnitTest) TestNamespaceNotExist() {

	mockDbNamespaceGet(s.mockSess, "doesntexist", gocql.ErrNotFound)

	exists, err := s.wsdb.NamespaceExists("doesntexist")
	s.req.NoError(err, "NamespaceExists failed: %s", err)
	s.req.False(exists, "Unexpected namespace exists in empty DB")
}
