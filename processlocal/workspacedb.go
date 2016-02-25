// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "sync"

import "arista.com/quantumfs"

func NewWorkspaceDB() quantumfs.WorkspaceDB {
	wsdb := &WorkspaceDB{
		cache: make(map[string]map[string]uint64),
	}
	return wsdb
}

// WorkspaceDB is a process local quantumfs.WorkspaceDB
type WorkspaceDB struct {
	cacheMutex sync.Mutex
	cache      map[string]map[string]uint64
}

func (wsdb *WorkspaceDB) NumNamespaces() int {
	wsdb.cacheMutex.Lock()
	num := len(wsdb.cache)
	wsdb.cacheMutex.Unlock()

	return num
}

func (wsdb *WorkspaceDB) NamespaceList() []string {
	wsdb.cacheMutex.Lock()
	namespaces := make([]string, 0, len(wsdb.cache))

	for name, _ := range wsdb.cache {
		namespaces = append(namespaces, name)
	}

	wsdb.cacheMutex.Unlock()

	return namespaces
}

func (wsdb *WorkspaceDB) WorkspaceList(namespace string) []string {
	wsdb.cacheMutex.Lock()
	workspaces := make([]string, 0, len(wsdb.cache[namespace]))

	for name, _ := range wsdb.cache {
		workspaces = append(workspaces, name)
	}

	wsdb.cacheMutex.Unlock()

	return workspaces
}
