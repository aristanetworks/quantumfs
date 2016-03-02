// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package processlocal

import "fmt"
import "sync"

import "arista.com/quantumfs"

func NewWorkspaceDB() quantumfs.WorkspaceDB {
	wsdb := &WorkspaceDB{
		cache: make(map[string]map[string]uint64),
	}

	wsdb.cache["travisb"] = make(map[string]uint64)
	wsdb.cache["abuild"] = make(map[string]uint64)
	wsdb.cache["travisb"]["workspace1"] = 1
	wsdb.cache["travisb"]["workspace2"] = 2
	wsdb.cache["abuild"]["project1@1234"] = 3
	wsdb.cache["abuild"]["project1@5678"] = 4
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

func (wsdb *WorkspaceDB) NumWorkspaces(namespace string) int {
	wsdb.cacheMutex.Lock()
	num := len(wsdb.cache[namespace])
	wsdb.cacheMutex.Unlock()

	return num
}

func (wsdb *WorkspaceDB) WorkspaceList(namespace string) []string {
	wsdb.cacheMutex.Lock()
	workspaces := make([]string, 0, len(wsdb.cache[namespace]))

	for name, _ := range wsdb.cache[namespace] {
		workspaces = append(workspaces, name)
	}

	wsdb.cacheMutex.Unlock()

	return workspaces
}

func (wsdb *WorkspaceDB) NamespaceExists(namespace string) bool {
	wsdb.cacheMutex.Lock()
	_, exists := wsdb.cache[namespace]
	wsdb.cacheMutex.Unlock()

	return exists
}

// Non-lock grabbing variant of WorkspaceExists
func (wsdb *WorkspaceDB) workspaceExists(namespace string, workspace string) bool {
	_, exists := wsdb.cache[namespace]
	if exists {
		_, exists = wsdb.cache[namespace][workspace]
	}

	return exists
}

func (wsdb *WorkspaceDB) WorkspaceExists(namespace string, workspace string) bool {
	wsdb.cacheMutex.Lock()
	exists := wsdb.workspaceExists(namespace, workspace)
	wsdb.cacheMutex.Unlock()

	return exists
}

func (wsdb *WorkspaceDB) BranchWorkspace(srcNamespace string, srcWorkspace string,
	dstNamespace string, dstWorkspace string) error {

	wsdb.cacheMutex.Lock()
	defer wsdb.cacheMutex.Unlock()

	if !wsdb.workspaceExists(srcNamespace, srcWorkspace) {
		return fmt.Errorf("Source Workspace doesn't exist")
	}

	if _, exists := wsdb.cache[dstNamespace]; !exists {
		wsdb.cache[dstNamespace] = make(map[string]uint64)
	} else if _, exists := wsdb.cache[dstNamespace][dstWorkspace]; exists {
		return fmt.Errorf("Destination Workspace already exists")
	}

	wsdb.cache[dstNamespace][dstWorkspace] = wsdb.cache[srcNamespace][srcWorkspace]

	return nil
}
