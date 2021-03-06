// Copyright (c) 2016 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// this is a library of tests which are common to both
// cached and uncached versions of workspace db API
// implementation

package cql

import (
	"bytes"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/stretchr/testify/require"
)

type wsdbCommonIntegTest struct {
	req *require.Assertions
	db  WorkspaceDB
}

func (s *wsdbCommonIntegTest) TestIntegEmptyDB() {

	tsCount, err1 := s.db.NumTypespaces(integTestCqlCtx)
	s.req.NoError(err1, "NumTypespaces failed %s", err1)
	s.req.Equal(1, tsCount, "Empty DB has incorrect count of typespaces")

	tsList, err2 := s.db.TypespaceList(integTestCqlCtx)
	s.req.NoError(err2, "TypespaceList failed %s", err2)
	s.req.Equal([]string{quantumfs.NullSpaceName}, tsList,
		"Empty DB has incorrect list of typespaces")

	nsCount, err2 := s.db.NumNamespaces(integTestCqlCtx, quantumfs.NullSpaceName)
	s.req.NoError(err2, "NumNamespaces failed %s", err2)
	s.req.Equal(1, nsCount, "Empty DB has incorrect count of namespaces")

	nsList, err4 := s.db.NamespaceList(integTestCqlCtx, quantumfs.NullSpaceName)
	s.req.NoError(err4, "NamespaceList failed %s", err4)
	s.req.Equal([]string{quantumfs.NullSpaceName}, nsList,
		"Empty DB has incorrect list of namespaces")

	wsCount, err5 := s.db.NumWorkspaces(integTestCqlCtx, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName)
	s.req.NoError(err5, "NumWorkspaces failed %s", err5)
	s.req.Equal(1, wsCount, "Empty DB has incorrect count of workspaces")

	wsList, err6 := s.db.WorkspaceList(integTestCqlCtx, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName)
	s.req.NoError(err6, "WorkspaceList failed %s", err6)
	s.req.Equal(1, len(wsList),
		"Empty DB has incorrect number of workspaces")
	s.req.Contains(wsList, quantumfs.NullSpaceName,
		"Empty DB has incorrect list of workspaces")
	s.req.Equal(wsList[quantumfs.NullSpaceName], quantumfs.WorkspaceNonceInvalid,
		"Incorect Nonce value for _/_/_")
}

func (s *wsdbCommonIntegTest) TestIntegBranching() {

	nonceNull, nonceWS1, err := s.db.BranchWorkspace(integTestCqlCtx,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)
	s.req.NotEqual(nonceNull, nonceWS1,
		"Same nonce for src and dst after branching workspace")

	tsCount, err1 := s.db.NumTypespaces(integTestCqlCtx)
	s.req.NoError(err1, "NumTypespaces failed %s", err1)
	s.req.Equal(2, tsCount,
		"Incorrect count of typespaces after branching")

	tsList, err2 := s.db.TypespaceList(integTestCqlCtx)
	s.req.NoError(err2, "TypespaceList failed %s", err2)
	s.req.Contains(tsList, quantumfs.NullSpaceName,
		"Expected null typespace not found")
	s.req.Contains(tsList, "ts1",
		"Expected typespace ts1 not found")

	nsCount, err3 := s.db.NumNamespaces(integTestCqlCtx, "ts1")
	s.req.NoError(err3, "NumNamespaces failed %s", err3)
	s.req.Equal(1, nsCount,
		"Incorrect count of namespaces after branching")

	nsList, err4 := s.db.NamespaceList(integTestCqlCtx, "ts1")
	s.req.NoError(err4, "NamespaceList failed %s", err4)
	s.req.Contains(nsList, "ns1",
		"Expected namespace ns1 not found")

	wsCount, err5 := s.db.NumWorkspaces(integTestCqlCtx, "ts1", "ns1")
	s.req.NoError(err5, "NumWorkspaces failed %s", err5)
	s.req.Equal(1, wsCount,
		"Incorrect count of workspaces after branching")

	wsList, err6 := s.db.WorkspaceList(integTestCqlCtx, "ts1", "ns1")
	s.req.NoError(err6, "WorkspaceList failed %s", err6)
	s.req.Contains(wsList, "ws1",
		"Expected workspace ws1 not found")
	s.req.Equal(nonceWS1, wsList["ws1"],
		"Mismatch nonce for workspace ws1")
}

func (s *wsdbCommonIntegTest) TestIntegAdvanceOk() {

	emptyKey := []byte(nil)
	newKey := []byte{1, 2, 3}

	_, nonce, e := s.db.BranchWorkspace(integTestCqlCtx, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		"ts1", "ns1", "ws1")
	s.req.NoError(e, "Error branching null workspace: %v", e)

	key, nonceBefore, err1 := s.db.Workspace(integTestCqlCtx,
		"ts1", "ns1", "ws1")
	s.req.NoError(err1, "Workspace failed %s", err1)
	s.req.True(bytes.Equal(key, emptyKey), "Current RootID isn't empty")

	newRootID, _, err := s.db.AdvanceWorkspace(integTestCqlCtx, "ts1", "ns1",
		"ws1", nonce, emptyKey, newKey)
	s.req.NoError(err, "Error in advancing workspace EmptyDirKey: %v", err)
	s.req.True(bytes.Equal(newRootID, newKey), "New RootID isn't EmptyDirKey")

	key, nonceAfter, err1 := s.db.Workspace(integTestCqlCtx, "ts1", "ns1",
		"ws1")
	s.req.NoError(err1, "Workspace failed %s", err1)
	s.req.True(bytes.Equal(key, newRootID), "Current RootID isn't empty")

	s.req.Equal(nonceBefore, nonceAfter,
		"nonce changed after advance for ts1/ns1/ws1")

	newRootID, _, err = s.db.AdvanceWorkspace(integTestCqlCtx, "ts1",
		"ns1", "ws1", nonce, newKey, emptyKey)
	s.req.NoError(err, "Error in advancing workspace to EmptyWorkspaceKey: %v",
		err)
	s.req.True(bytes.Equal(newRootID, emptyKey),
		"New RootID isn't EmptyWorkspaceKey")
}

func (s *wsdbCommonIntegTest) TestIntegDeleteNullTypespace() {
	err := s.db.DeleteWorkspace(integTestCqlCtx, quantumfs.NullSpaceName,
		"ns1", "ws1")
	s.req.Error(err, "Succeeded in deleting null workspace")
}

func (s *wsdbCommonIntegTest) TestIntegDeleteWorkspaceOK() {
	err := s.db.DeleteWorkspace(integTestCqlCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Failed in deleting ts1/ns1/ws1 workspace")
}

func (s *wsdbCommonIntegTest) TestIntegWorkspaceLastWriteTime() {
	currentTime := time.Now().UTC()
	_, _, e := s.db.BranchWorkspace(integTestCqlCtx, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		"ts1", "ns1", "ws1")
	s.req.NoError(e, "Error branching null workspace: %v", e)
	ts, err := s.db.WorkspaceLastWriteTime(integTestCqlCtx,
		"ts1", "ns1", "ws1")
	s.req.NoError(err,
		"Failed in getting last write time for ts1/ns1/ws1 workspace")

	// Check if the 2 time stamps are close to each other.
	s.req.True(ts.Unix()-currentTime.Unix() < 5,
		"Time stamp is off by more than 5 seconds")
}

func (s *wsdbCommonIntegTest) TestIntegWorkspaceReCreateWithNewNonce() {

	_, nonceBefore, err := s.db.BranchWorkspace(integTestCqlCtx,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	err = s.db.DeleteWorkspace(integTestCqlCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Failed in deleting ts1/ns1/ws1 workspace")

	_, nonceAfter, err := s.db.BranchWorkspace(integTestCqlCtx,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	s.req.NotEqual(nonceBefore, nonceAfter,
		"Same nonce for workspace ts1/ns1/ws1 even after delete")
}

// This test ensures that the map returning from WorkspaceList
// Has the correct nonces when the map has multiple keys.
func (s *wsdbCommonIntegTest) TestIntegWorkspaceNonce() {

	_, nonceWS1, err := s.db.BranchWorkspace(integTestCqlCtx,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	_, nonceWS2, err := s.db.BranchWorkspace(integTestCqlCtx,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, "ts1", "ns1", "ws2")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	wsList, err6 := s.db.WorkspaceList(integTestCqlCtx, "ts1", "ns1")
	s.req.NoError(err6, "WorkspaceList failed %s", err6)

	s.req.Contains(wsList, "ws1",
		"Expected workspace ws1 not found")
	s.req.Equal(nonceWS1, wsList["ws1"],
		"Mismatch nonce for workspace ws1")

	s.req.Contains(wsList, "ws2",
		"Expected workspace ws2 not found")
	s.req.Equal(nonceWS2, wsList["ws2"],
		"Mismatch nonce for workspace ws2")
}

func (s *wsdbCommonIntegTest) TestIntegSetWorkspaceImmutable() {
	_, _, err := s.db.BranchWorkspace(integTestCqlCtx, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		"ts1", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	err = s.db.SetWorkspaceImmutable(integTestCqlCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Error in SetWorkspaceImmtable for ts1/ns1/ws1: %v", err)
}

func (s *wsdbCommonIntegTest) TestIntegSetWorkspaceImmutableError() {
	err := s.db.SetWorkspaceImmutable(integTestCqlCtx, "ts1", "ns1", "ws1")
	s.req.Error(err, "Success in SetWorkspaceImmtable for ts1/ns1/ws1")
}

func (s *wsdbCommonIntegTest) TestIntegWorkspaceIsImmutable() {
	_, _, err := s.db.BranchWorkspace(integTestCqlCtx, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		"ts1", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	immutable, err := s.db.WorkspaceIsImmutable(integTestCqlCtx,
		"ts1", "ns1", "ws1")
	s.req.NoError(err, "Success in WorkspaceIsImmutable for ts1/ns1/ws1")
	s.req.Equal(false, immutable, "immutable should be false for ts1/ns1/ws1")

	err = s.db.SetWorkspaceImmutable(integTestCqlCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Error in SetWorkspaceImmtable for ts1/ns1/ws1: %v", err)

	immutable, err = s.db.WorkspaceIsImmutable(integTestCqlCtx,
		"ts1", "ns1", "ws1")
	s.req.NoError(err, "Success in WorkspaceIsImmutable for ts1/ns1/ws1")
	s.req.Equal(true, immutable, "immutable should be true for ts1/ns1/ws1")
}

func (s *wsdbCommonIntegTest) TestIntegWorkspaceIsImmutableError() {
	_, err := s.db.WorkspaceIsImmutable(integTestCqlCtx, "ts1", "ns1", "ws1")
	s.req.Error(err, "Success in WorkspaceIsImmutable for ts1/ns1/ws1")
}

func (s *wsdbCommonIntegTest) TestIntegDeleteImmutableSet() {
	_, _, err := s.db.BranchWorkspace(integTestCqlCtx, quantumfs.NullSpaceName,
		quantumfs.NullSpaceName, quantumfs.NullSpaceName,
		"ts1", "ns1", "ws1")
	s.req.NoError(err, "Error branching null workspace: %v", err)

	err = s.db.SetWorkspaceImmutable(integTestCqlCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Error in SetWorkspaceImmtable for ts1/ns1/ws1: %v", err)

	err = s.db.DeleteWorkspace(integTestCqlCtx, "ts1", "ns1", "ws1")
	s.req.NoError(err, "Failed in deleting ts1/ns1/ws1 workspace")
}
