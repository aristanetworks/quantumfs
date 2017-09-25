// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// this is a library of tests which are common to both
// cached and uncached versions of workspace db API
// implementation

package cql

import (
	"bytes"
	"time"

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

	tsRows := mockDbRows{{wsdb.NullSpaceName}}
	tsIter := new(MockIter)
	tsVals := []interface{}(nil)

	nsRows := mockDbRows{{wsdb.NullSpaceName}}
	nsIter := new(MockIter)
	nsVals := []interface{}{wsdb.NullSpaceName}

	wsRows := mockDbRows{{wsdb.NullSpaceName}}
	wsIter := new(MockIter)
	wsVals := []interface{}{wsdb.NullSpaceName, wsdb.NullSpaceName}

	mockWsdbCacheTypespaceFetch(s.mockSess, tsRows, tsVals,
		tsIter, nil)
	mockWsdbCacheNamespaceFetch(s.mockSess, nsRows, nsVals,
		nsIter, nil)
	mockWsdbCacheWorkspaceFetch(s.mockSess, wsRows, wsVals,
		wsIter, nil)

	// cached APIs
	tsCount, err1 := s.wsdb.NumTypespaces(unitTestEtherCtx)
	s.req.NoError(err1, "NumTypespaces failed: %s", err1)
	s.req.Equal(1, tsCount, "Empty DB has incorrect count of typespaces")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	tsList, err2 := s.wsdb.TypespaceList(unitTestEtherCtx)
	s.req.NoError(err2, "TypespaceList failed: %s", err2)
	s.req.Equal([]string{wsdb.NullSpaceName}, tsList,
		"Empty DB has incorrect list of typespaces")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	nsCount, err1 := s.wsdb.NumNamespaces(unitTestEtherCtx, wsdb.NullSpaceName)
	s.req.NoError(err1, "NumNamespace failed: %s", err1)
	s.req.Equal(1, nsCount, "Empty DB has incorrect count of namespaces")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	nsList, err2 := s.wsdb.NamespaceList(unitTestEtherCtx, wsdb.NullSpaceName)
	s.req.NoError(err2, "NamespaceList failed: %s", err2)
	s.req.Equal([]string{wsdb.NullSpaceName}, nsList,
		"Empty DB has incorrect list of namespaces")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	wsCount, err3 := s.wsdb.NumWorkspaces(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	s.req.NoError(err3, "NumWorkspaces failed: %s", err3)
	s.req.Equal(1, wsCount, "Empty DB has incorrect count of workspaces")

	tsIter.Reset()
	nsIter.Reset()
	wsIter.Reset()

	wsList, err4 := s.wsdb.WorkspaceList(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	s.req.NoError(err4, "WorkspaceList failed: %s", err4)
	s.req.Equal([]string{wsdb.NullSpaceName}, wsList,
		"Empty DB has incorrect list of workspaces")

	// basic uncached APIs
	mockDbTypespaceGet(s.mockSess, wsdb.NullSpaceName, nil)

	exists, err5 := s.wsdb.TypespaceExists(unitTestEtherCtx, wsdb.NullSpaceName)
	s.req.NoError(err5, "TypespaceExists failed: %s", err5)
	s.req.True(exists, "Expected typespace doesn't exist in empty DB")

	mockDbNamespaceGet(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName, nil)

	exists, err6 := s.wsdb.NamespaceExists(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	s.req.NoError(err6, "NamespaceExists failed: %s", err6)
	s.req.True(exists, "Expected namespace doesn't exist in empty DB")

	mockWsdbKeyGet(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte(nil), nil)

	exists, err7 := s.wsdb.WorkspaceExists(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName)
	s.req.NoError(err7, "WorkspaceExists failed: %s", err7)
	s.req.True(exists, "Failed to find expected workspace in empty DB")

	key, err8 := s.wsdb.Workspace(unitTestEtherCtx, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName)
	s.req.NoError(err8, "Workspace failed: %s", err8)
	s.req.True(bytes.Equal(key, []byte(nil)), wsdb.NullSpaceName+
		" workspace isn't empty")

}

func (s *wsdbCommonUnitTest) TestBranching() {

	// test branching from invalid namespace or workspace
	mockWsdbKeyGet(s.mockSess, "notype", "notthere", "a",
		nil, gocql.ErrNotFound)

	err := s.wsdb.BranchWorkspace(unitTestEtherCtx, "notype", "notthere", "a",
		"sometype", "somewhere", "else")
	s.req.Error(err, "Succeeded branching invalid namespace")
	s.req.IsType(&wsdb.Error{}, err, "Invalid error type %T", err)

	// test branching from namespace and workspace in empty DB
	mockBranchWorkspace(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "some", "test", "a", []byte(nil),
		gocql.ErrNotFound)

	err = s.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "some", "test", "a")
	s.req.NoError(err, "Error branching "+wsdb.NullSpaceName+" workspace: %v",
		err)

	// test branching to an existing workspace
	mockWsdbKeyGet(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte(nil), nil)
	// mockDbKeyGets within a test use same session and
	// hence the expected mock Calls are ordered.
	// using "test" and "a" will cause previous mocked action
	// to occur instead of current. Hence using "test1" and "a1"
	mockWsdbKeyGet(s.mockSess, "some1", "test1", "a1", []byte(nil), nil)

	err = s.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "some1", "test1", "a1")
	s.req.Error(err, "Succeeded branching to existing workspace")

	// test branching from non-null workspace
	mockBranchWorkspace(s.mockSess, "some2", "test2", "a2",
		"some3", "test3", "b3", []byte(nil), gocql.ErrNotFound)

	err = s.wsdb.BranchWorkspace(unitTestEtherCtx, "some2", "test2", "a2",
		"some3", "test3", "b3")
	s.req.NoError(err, "Error rebranching workspace: %v", err)
}

func (s *wsdbCommonUnitTest) TestAdvanceOk() {

	// test successful advance of a branched workspace
	mockWsdbKeyGet(s.mockSess, "some", "test", "a", []byte(nil), nil)
	mockWsdbKeyPut(s.mockSess, "some", "test", "a", []byte{1, 2, 3}, nil)

	newRootID, err := s.wsdb.AdvanceWorkspace(unitTestEtherCtx, "some", "test", "a",
		[]byte(nil), []byte{1, 2, 3})

	s.req.NoError(err, "Error when advancing root: %v", err)
	s.req.True(bytes.Equal(newRootID, []byte{1, 2, 3}),
		"New root doesn't match")
}

func (s *wsdbCommonUnitTest) TestAdvanceOutOfDate() {

	// test advance of a branched workspace whose current rootID
	// has changed

	mockWsdbKeyGet(s.mockSess, "some", "test", "a", []byte(nil), nil)

	newKey := []byte{1, 2, 3}
	_, err := s.wsdb.AdvanceWorkspace(unitTestEtherCtx, "some", "test", "a",
		newKey, newKey)

	s.req.Error(err, "Succeeded advancing out-of-date workspace")
	s.req.IsType(&wsdb.Error{},
		err, "Invalid error type %T", err)
}

func (s *wsdbCommonUnitTest) TestAdvanceNotExist() {
	// test advance of a non-existant workspace

	mockWsdbKeyGet(s.mockSess, "some", "test", "a",
		nil, gocql.ErrNotFound)

	_, err := s.wsdb.AdvanceWorkspace(unitTestEtherCtx, "some", "test", "a",
		[]byte(nil), []byte(nil))

	s.req.Error(err, "Succeeded advancing non-existant workspace")
	s.req.IsType(&wsdb.Error{},
		err, "Invalid error type %T", err)
}

func (s *wsdbCommonUnitTest) TestNamespaceNotExist() {

	mockDbNamespaceGet(s.mockSess, "some", "doesntexist", gocql.ErrNotFound)

	exists, err := s.wsdb.NamespaceExists(unitTestEtherCtx, "some", "doesntexist")
	s.req.NoError(err, "NamespaceExists failed: %s", err)
	s.req.False(exists, "Unexpected namespace exists in empty DB")
}

func (s *wsdbCommonUnitTest) TestTypespaceNotExist() {

	mockDbTypespaceGet(s.mockSess, "doesntexist", gocql.ErrNotFound)

	exists, err := s.wsdb.TypespaceExists(unitTestEtherCtx, "doesntexist")
	s.req.NoError(err, "TypespaceExists failed: %s", err)
	s.req.False(exists, "Unexpected typespace exists in empty DB")
}

func (s *wsdbCommonUnitTest) TestLockedBranchWorkspace() {

	err := s.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, "ns1", "ws1")
	s.req.Error(err, "Succeeded in branching to "+wsdb.NullSpaceName+"/ns1/ws1")

	err = s.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName)
	s.req.Error(err, "Succeeded in branching to the null workspace")

	mockBranchWorkspace(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", wsdb.NullSpaceName, "ws1", []byte(nil),
		gocql.ErrNotFound)
	err = s.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", wsdb.NullSpaceName, "ws1")
	s.req.NoError(err, "Failed in branching to ts1/"+wsdb.NullSpaceName+"/ws1")

	mockBranchWorkspace(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", "ns1", wsdb.NullSpaceName, []byte(nil),
		gocql.ErrNotFound)
	err = s.wsdb.BranchWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", "ns1", wsdb.NullSpaceName)
	s.req.NoError(err, "Failed in branching to ts1/ns1/"+wsdb.NullSpaceName)
}

func (s *wsdbCommonUnitTest) TestLockedAdvanceWorkspace() {

	_, err := s.wsdb.AdvanceWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, "ns1", "ws1",
		[]byte{1, 2, 3}, []byte{4, 5, 6})
	s.req.Error(err, "Succeeded in advancing "+wsdb.NullSpaceName+"/ns1/ws1")

	_, err = s.wsdb.AdvanceWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3}, []byte{4, 5, 6})
	s.req.Error(err, "Succeeded in advancing the null workspace")

	mockWsdbKeyGet(s.mockSess, "ts1", wsdb.NullSpaceName, "ws1", []byte{1, 2,
		3}, nil)
	mockWsdbKeyPut(s.mockSess, "ts1", wsdb.NullSpaceName, "ws1", []byte{4, 5,
		6}, nil)
	_, err = s.wsdb.AdvanceWorkspace(unitTestEtherCtx, "ts1", wsdb.NullSpaceName, "ws1",
		[]byte{1, 2, 3}, []byte{4, 5, 6})
	s.req.NoError(err, "Failed in advancing ts1/"+wsdb.NullSpaceName+"/ws1")

	mockWsdbKeyGet(s.mockSess, "ts1", "ns1", wsdb.NullSpaceName, []byte{1, 2,
		3}, nil)
	mockWsdbKeyPut(s.mockSess, "ts1", "ns1", wsdb.NullSpaceName, []byte{4, 5,
		6}, nil)
	_, err = s.wsdb.AdvanceWorkspace(unitTestEtherCtx, "ts1", "ns1", wsdb.NullSpaceName,
		[]byte{1, 2, 3}, []byte{4, 5, 6})
	s.req.NoError(err, "Failed in advancing ts1/ns1/"+wsdb.NullSpaceName)
}

// verifies write once property of the null workspace
func (s *wsdbCommonUnitTest) TestInitialAdvanceWorkspace() {
	mockWsdbKeyGet(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, nil, nil)
	mockWsdbKeyPut(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3}, nil)
	_, err := s.wsdb.AdvanceWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, nil, []byte{1, 2, 3})
	s.req.NoError(err, "Failed in initial advance of the null workspace")

	_, err = s.wsdb.AdvanceWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3}, []byte{4, 5, 6})
	s.req.Error(err,
		"Succeeded in advancing null workspace after initial set")
}

func (s *wsdbCommonUnitTest) TestDeleteNullTypespace() {
	err := s.wsdb.DeleteWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, "ns1", "ws1")
	s.req.Error(err, "Succeeded in deleting null workspace")
}

func (s *wsdbCommonUnitTest) TestDeleteWorkspaceOK() {
	mockWsdbKeyDel(s.mockSess, "ts1", "ns1", "ws1", nil)
	err := s.wsdb.DeleteWorkspace(unitTestEtherCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Failed in deleting ts1/ns1/ws1 workspace")
}

func (s *wsdbCommonUnitTest) TestWorkspaceLastWriteTime() {
	microSecs := int64(4237423784)
	mockWsdbWorkspaceLastWriteTime(s.mockSess, "ts1", "ns1", "ws1", microSecs, nil)
	ts, err := s.wsdb.WorkspaceLastWriteTime(unitTestEtherCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Failed in getting last write time for ts1/ns1/ws1 workspace")
	s.req.True(
		ts.Equal(time.Unix(microSecs/int64(time.Second/time.Microsecond), 0).UTC()),
		"Expected and received time stamp mismatch")
}

func (s *wsdbCommonUnitTest) TestCreateWorkspaceNoKey() {
	mockWsdbKeyGet(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, nil, gocql.ErrNotFound)
	mockWsdbKeyPut(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3}, nil)
	err := s.wsdb.CreateWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3})
	s.req.NoError(err, "Failed in creating workspace")
}

func (s *wsdbCommonUnitTest) TestCreateWorkspaceDiffKey() {
	mockWsdbKeyGet(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{4, 5, 6}, nil)
	mockWsdbKeyPut(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3}, nil)

	err := s.wsdb.CreateWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3})
	s.req.Error(err,
		"Succeeded in creating workspace even though different key exists")
}

func (s *wsdbCommonUnitTest) TestCreateWorkspaceSameKey() {
	mockWsdbKeyGet(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3}, nil)
	mockWsdbKeyPut(s.mockSess, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3}, nil)

	err := s.wsdb.CreateWorkspace(unitTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, []byte{1, 2, 3})
	s.req.NoError(err, "Failed in creating workspace")
}
