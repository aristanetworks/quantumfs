// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// this is a library of tests which are common to both
// cached and uncached versions of workspace db API
// implementation

package cql

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/stretchr/testify/require"
)

type wsdbCommonIntegTest struct {
	req *require.Assertions
	db  quantumfs.WorkspaceDB
}

func (s *wsdbCommonIntegTest) TestIntegEmptyDB() {

	nsCount := s.db.NumNamespaces(nil)
	s.req.Equal(1, nsCount, "Empty DB has incorrect count of namespaces")

	nsList := s.db.NamespaceList(nil)
	s.req.Equal([]string{"_null"}, nsList,
		"Empty DB has incorrect list of namespaces")

	wsCount := s.db.NumWorkspaces(nil, "_null")
	s.req.Equal(1, wsCount, "Empty DB has incorrect count of workspaces")

	wsList := s.db.WorkspaceList(nil, "_null")
	s.req.Equal([]string{"null"}, wsList,
		"Empty DB has incorrect list of workspaces")
}

func (s *wsdbCommonIntegTest) TestIntegBranching() {

	err := s.db.BranchWorkspace(nil,
		"_null", "null", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	nsCount := s.db.NumNamespaces(nil)
	s.req.Equal(2, nsCount,
		"Incorrect count of namespaces after branching")

	nsList := s.db.NamespaceList(nil)
	s.req.Contains(nsList, "_null",
		"Expected namespace _null not found")
	s.req.Contains(nsList, "ns1",
		"Expected namespace ns1 not found")

	wsCount := s.db.NumWorkspaces(nil, "ns1")
	s.req.Equal(1, wsCount,
		"Incorrect count of workspaces after branching")

	wsList := s.db.WorkspaceList(nil, "ns1")
	s.req.Contains(wsList, "ws1",
		"Expected workspace null not found")
}

func (s *wsdbCommonIntegTest) TestIntegAdvanceOk() {

	e := s.db.BranchWorkspace(nil,
		"_null", "null", "ns1", "ws1")
	s.req.NoError(e, "Error branching null workspace: %v", e)

	key := s.db.Workspace(nil, "ns1", "ws1")
	s.req.True(key.IsEqualTo(quantumfs.EmptyWorkspaceKey),
		"Current RootID isn't EmptyWorkspaceKey")

	newRootID, err := s.db.AdvanceWorkspace(nil, "ns1", "ws1",
		quantumfs.EmptyWorkspaceKey, quantumfs.EmptyDirKey)
	s.req.NoError(err, "Error in advancing workspace EmptyDirKey: %v", err)
	s.req.True(newRootID.IsEqualTo(quantumfs.EmptyDirKey),
		"New RootID isn't EmptyDirKey")

	newRootID, err = s.db.AdvanceWorkspace(nil, "ns1", "ws1",
		quantumfs.EmptyDirKey, quantumfs.EmptyWorkspaceKey)
	s.req.NoError(err, "Error in advancing workspace to EmptyWorkspaceKey: %v", err)
	s.req.True(newRootID.IsEqualTo(quantumfs.EmptyWorkspaceKey),
		"New RootID isn't EmptyWorkspaceKey")
}
