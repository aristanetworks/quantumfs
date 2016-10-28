// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import "github.com/aristanetworks/quantumfs"

type WorkspaceDB struct {
	store *cqlStore
}

func NewWorkspaceDB(confName string) quantumfs.WorkspaceDB {

	// currently initCqlStore panics upon error
	initCqlStore(confName)

	wsdb := &WorkspaceDB{
		store: &globalCqlStore,
	}

	return wsdb
}

func (wsdb *WorkspaceDB) NumNamespaces(c *quantumfs.Ctx) int {
	return 0
}

func (wsdb *WorkspaceDB) NamespaceList(c *quantumfs.Ctx) []string {
	return nil
}

func (wsdb *WorkspaceDB) NumWorkspaces(c *quantumfs.Ctx, namespace string) int {
	return 0
}

func (wsdb *WorkspaceDB) WorkspaceList(c *quantumfs.Ctx, namespace string) []string {
	return nil
}

func (wsdb *WorkspaceDB) NamespaceExists(c *quantumfs.Ctx, namespace string) bool {
	return false
}

func (wsdb *WorkspaceDB) WorkspaceExists(c *quantumfs.Ctx, namespace string,
	workspace string) bool {
	return false
}

func (wsdb *WorkspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcNamespace string,
	srcWorkspace string, dstNamespace string, dstWorkspace string) error {
	return nil
}

func (wsdb *WorkspaceDB) Workspace(c *quantumfs.Ctx, namespace string,
	workspace string) quantumfs.ObjectKey {
	// TBD: This API should have a way to pass back invalid key or error
	empty := make([]byte, 0)
	return quantumfs.NewObjectKeyFromBytes(empty)
}

func (wsdb *WorkspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, namespace string,
	workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	empty := make([]byte, 0)
	return quantumfs.NewObjectKeyFromBytes(empty), nil
}
