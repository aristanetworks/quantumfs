// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// this is a library of tests which are common to both
// cached and uncached versions of workspace db API
// implementation

package cql

import (
	"bytes"

	"github.com/aristanetworks/ether/qubit/wsdb"
	"github.com/stretchr/testify/require"
)

type wsdbCommonIntegTest struct {
	req *require.Assertions
	db  wsdb.WorkspaceDB
}

func (s *wsdbCommonIntegTest) TestIntegEmptyDB() {

	nsCount, err1 := s.db.NumNamespaces()
	s.req.NoError(err1, "NumNamespaces failed %s", err1)
	s.req.Equal(1, nsCount, "Empty DB has incorrect count of namespaces")

	nsList, err2 := s.db.NamespaceList()
	s.req.NoError(err2, "NamespaceList failed %s", err2)
	s.req.Equal([]string{"_null"}, nsList,
		"Empty DB has incorrect list of namespaces")

	wsCount, err3 := s.db.NumWorkspaces("_null")
	s.req.NoError(err3, "NumWorkspaces failed %s", err3)
	s.req.Equal(1, wsCount, "Empty DB has incorrect count of workspaces")

	wsList, err4 := s.db.WorkspaceList("_null")
	s.req.NoError(err4, "WorkspaceList failed %s", err4)
	s.req.Equal([]string{"null"}, wsList,
		"Empty DB has incorrect list of workspaces")
}

func (s *wsdbCommonIntegTest) TestIntegBranching() {

	err := s.db.BranchWorkspace("_null", "null", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	nsCount, err1 := s.db.NumNamespaces()
	s.req.NoError(err1, "NumNamespaces failed %s", err1)
	s.req.Equal(2, nsCount,
		"Incorrect count of namespaces after branching")

	nsList, err2 := s.db.NamespaceList()
	s.req.NoError(err2, "NamespaceList failed %s", err2)
	s.req.Contains(nsList, "_null",
		"Expected namespace _null not found")
	s.req.Contains(nsList, "ns1",
		"Expected namespace ns1 not found")

	wsCount, err3 := s.db.NumWorkspaces("ns1")
	s.req.NoError(err3, "NumWorkspaces failed %s", err3)
	s.req.Equal(1, wsCount,
		"Incorrect count of workspaces after branching")

	wsList, err4 := s.db.WorkspaceList("ns1")
	s.req.NoError(err4, "WorkspaceList failed %s", err4)
	s.req.Contains(wsList, "ws1",
		"Expected workspace null not found")
}

func (s *wsdbCommonIntegTest) TestIntegAdvanceOk() {

	emptyKey := []byte(nil)
	newKey := []byte{1, 2, 3}

	e := s.db.BranchWorkspace("_null", "null", "ns1", "ws1")
	s.req.NoError(e, "Error branching null workspace: %v", e)

	key, err1 := s.db.Workspace("ns1", "ws1")
	s.req.NoError(err1, "Workspace failed %s", err1)
	s.req.True(bytes.Equal(key, emptyKey), "Current RootID isn't empty")

	newRootID, err := s.db.AdvanceWorkspace("ns1", "ws1", emptyKey, newKey)
	s.req.NoError(err, "Error in advancing workspace EmptyDirKey: %v", err)
	s.req.True(bytes.Equal(newRootID, newKey), "New RootID isn't EmptyDirKey")

	newRootID, err = s.db.AdvanceWorkspace("ns1", "ws1", newKey, emptyKey)
	s.req.NoError(err, "Error in advancing workspace to EmptyWorkspaceKey: %v", err)
	s.req.True(bytes.Equal(newRootID, emptyKey),
		"New RootID isn't EmptyWorkspaceKey")
}
