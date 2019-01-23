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
	"github.com/stretchr/testify/require"
)

type wsdbCommonIntegTest struct {
	req *require.Assertions
	db  wsdb.WorkspaceDB
}

func (s *wsdbCommonIntegTest) TestIntegEmptyDB() {

	tsCount, err1 := s.db.NumTypespaces(integTestEtherCtx)
	s.req.NoError(err1, "NumTypespaces failed %s", err1)
	s.req.Equal(1, tsCount, "Empty DB has incorrect count of typespaces")

	tsList, err2 := s.db.TypespaceList(integTestEtherCtx)
	s.req.NoError(err2, "TypespaceList failed %s", err2)
	s.req.Equal([]string{wsdb.NullSpaceName}, tsList,
		"Empty DB has incorrect list of typespaces")

	nsCount, err2 := s.db.NumNamespaces(integTestEtherCtx, wsdb.NullSpaceName)
	s.req.NoError(err2, "NumNamespaces failed %s", err2)
	s.req.Equal(1, nsCount, "Empty DB has incorrect count of namespaces")

	nsList, err4 := s.db.NamespaceList(integTestEtherCtx, wsdb.NullSpaceName)
	s.req.NoError(err4, "NamespaceList failed %s", err4)
	s.req.Equal([]string{wsdb.NullSpaceName}, nsList,
		"Empty DB has incorrect list of namespaces")

	wsCount, err5 := s.db.NumWorkspaces(integTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName)
	s.req.NoError(err5, "NumWorkspaces failed %s", err5)
	s.req.Equal(1, wsCount, "Empty DB has incorrect count of workspaces")

	wsList, err6 := s.db.WorkspaceList(integTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName)
	s.req.NoError(err6, "WorkspaceList failed %s", err6)
	s.req.Equal([]string{wsdb.NullSpaceName}, wsList,
		"Empty DB has incorrect list of workspaces")
}

func (s *wsdbCommonIntegTest) TestIntegBranching() {

	err := s.db.BranchWorkspace(integTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	tsCount, err1 := s.db.NumTypespaces(integTestEtherCtx)
	s.req.NoError(err1, "NumTypespaces failed %s", err1)
	s.req.Equal(2, tsCount,
		"Incorrect count of typespaces after branching")

	tsList, err2 := s.db.TypespaceList(integTestEtherCtx)
	s.req.NoError(err2, "TypespaceList failed %s", err2)
	s.req.Contains(tsList, wsdb.NullSpaceName,
		"Expected null typespace not found")
	s.req.Contains(tsList, "ts1",
		"Expected typespace ts1 not found")

	nsCount, err3 := s.db.NumNamespaces(integTestEtherCtx, "ts1")
	s.req.NoError(err3, "NumNamespaces failed %s", err3)
	s.req.Equal(1, nsCount,
		"Incorrect count of namespaces after branching")

	nsList, err4 := s.db.NamespaceList(integTestEtherCtx, "ts1")
	s.req.NoError(err4, "NamespaceList failed %s", err4)
	s.req.Contains(nsList, "ns1",
		"Expected namespace ns1 not found")

	wsCount, err5 := s.db.NumWorkspaces(integTestEtherCtx, "ts1", "ns1")
	s.req.NoError(err5, "NumWorkspaces failed %s", err5)
	s.req.Equal(1, wsCount,
		"Incorrect count of workspaces after branching")

	wsList, err6 := s.db.WorkspaceList(integTestEtherCtx, "ts1", "ns1")
	s.req.NoError(err6, "WorkspaceList failed %s", err6)
	s.req.Contains(wsList, "ws1",
		"Expected workspace ws1 not found")
}

func (s *wsdbCommonIntegTest) TestIntegAdvanceOk() {

	emptyKey := []byte(nil)
	newKey := []byte{1, 2, 3}

	e := s.db.BranchWorkspace(integTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", "ns1", "ws1")
	s.req.NoError(e, "Error branching null workspace: %v", e)

	key, nonce, err1 := s.db.Workspace(integTestEtherCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err1, "Workspace failed %s", err1)
	s.req.True(bytes.Equal(key, emptyKey), "Current RootID isn't empty")

	newRootID, err := s.db.AdvanceWorkspace(integTestEtherCtx, "ts1", "ns1", "ws1", nonce, emptyKey,
		newKey)
	s.req.NoError(err, "Error in advancing workspace EmptyDirKey: %v", err)
	s.req.True(bytes.Equal(newRootID, newKey), "New RootID isn't EmptyDirKey")

	newRootID, err = s.db.AdvanceWorkspace(integTestEtherCtx, "ts1", "ns1", "ws1", nonce, newKey, emptyKey)
	s.req.NoError(err, "Error in advancing workspace to EmptyWorkspaceKey: %v",
		err)
	s.req.True(bytes.Equal(newRootID, emptyKey),
		"New RootID isn't EmptyWorkspaceKey")
}

func (s *wsdbCommonIntegTest) TestIntegDeleteNullTypespace() {
	err := s.db.DeleteWorkspace(integTestEtherCtx, wsdb.NullSpaceName, "ns1", "ws1")
	s.req.Error(err, "Succeeded in deleting null workspace")
}

func (s *wsdbCommonIntegTest) TestIntegDeleteWorkspaceOK() {
	err := s.db.DeleteWorkspace(integTestEtherCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Failed in deleting ts1/ns1/ws1 workspace")
}

func (s *wsdbCommonIntegTest) TestIntegWorkspaceLastWriteTime() {
	currentTime := time.Now().UTC()
	e := s.db.BranchWorkspace(integTestEtherCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName, "ts1", "ns1", "ws1")
	s.req.NoError(e, "Error branching null workspace: %v", e)
	ts, err := s.db.WorkspaceLastWriteTime(integTestEtherCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Failed in getting last write time for ts1/ns1/ws1 workspace")

	// Check if the 2 time stamps are close to each other.
	s.req.True(ts.Unix()-currentTime.Unix() < 5,
		"Time stamp is off by more than 5 seconds")
}
