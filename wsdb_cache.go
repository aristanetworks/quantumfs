// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"github.com/aristanetworks/quantumfs"
	"time"
)

// In this implementation of workspace DB API,
// some APIs use cached information and do not
// interact with underlying datastore. All APIs interact
// with the cache to maintain local coherency. This
// implementation relies on the uncached workspace DB
// API to interact with underlying datastore.

type cacheWsdb struct {
	base  quantumfs.WorkspaceDB
	cache *entityCache
}

// this wsdb implementation wraps any wsdb (base) implementation
// with entity cache
func newCacheWsdb(base quantumfs.WorkspaceDB, cfg WsDBConfig) quantumfs.WorkspaceDB {

	cwsdb := &cacheWsdb{
		base: base,
	}

	// TODO: default max cache age can be a configuration parameter?
	c := newEntityCache(2, 1*time.Second, cwsdb, wsdbFetcherImpl)

	// QFS requires an empty workspaceDB to contain null namespace
	// and _null workspace
	c.InsertEntities("_null", "null")

	cwsdb.cache = c

	return cwsdb
}

// --- workspace DB API implementation ---

func (cw *cacheWsdb) NumNamespaces(c *quantumfs.Ctx) int {
	return cw.cache.CountEntities()
}

func (cw *cacheWsdb) NamespaceList(c *quantumfs.Ctx) []string {
	return cw.cache.ListEntities()
}

func (cw *cacheWsdb) NumWorkspaces(c *quantumfs.Ctx, namespace string) int {
	return cw.cache.CountEntities(namespace)
}

func (cw *cacheWsdb) WorkspaceList(c *quantumfs.Ctx, namespace string) []string {
	return cw.cache.ListEntities(namespace)
}

func (cw *cacheWsdb) NamespaceExists(c *quantumfs.Ctx, namespace string) bool {

	exist := cw.base.NamespaceExists(c, namespace)
	if !exist {
		cw.cache.DeleteEntities(namespace)
	} else {
		cw.cache.InsertEntities(namespace)
	}

	return exist
}

func (cw *cacheWsdb) WorkspaceExists(c *quantumfs.Ctx, namespace string,
	workspace string) bool {

	exist := cw.base.WorkspaceExists(c, namespace, workspace)
	if !exist {
		cw.cache.DeleteEntities(namespace, workspace)
	} else {
		cw.cache.InsertEntities(namespace, workspace)
	}

	return exist
}

func (cw *cacheWsdb) BranchWorkspace(c *quantumfs.Ctx, srcNamespace string,
	srcWorkspace string, dstNamespace string, dstWorkspace string) error {

	if err := cw.base.BranchWorkspace(c, srcNamespace, srcWorkspace,
		dstNamespace, dstWorkspace); err != nil {
		return err
	}

	cw.cache.InsertEntities(srcNamespace, srcWorkspace)
	cw.cache.InsertEntities(dstNamespace, dstWorkspace)

	return nil
}

func (cw *cacheWsdb) Workspace(c *quantumfs.Ctx, namespace string,
	workspace string) quantumfs.ObjectKey {

	key := cw.base.Workspace(c, namespace, workspace)
	// TODO: check errors
	cw.cache.InsertEntities(namespace, workspace)
	return key
}

func (cw *cacheWsdb) AdvanceWorkspace(c *quantumfs.Ctx, namespace string,
	workspace string, currentRootID quantumfs.ObjectKey,
	newRootID quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	key, err := cw.base.AdvanceWorkspace(c, namespace,
		workspace, currentRootID, newRootID)
	if err != nil {
		return key, err
	}

	cw.cache.InsertEntities(namespace, workspace)

	return key, nil
}

// wsdbFetcherImpl implements fetcher interface in entity cache
// the returned map contains entities inserted on local node or
// insertions from other nodes in the CQL cluster
func wsdbFetcherImpl(arg interface{}, entityPath ...string) map[string]bool {

	cw, ok := arg.(*cacheWsdb)
	if !ok {
		panic("unsupported type of arg")
	}

	var list []string
	switch len(entityPath) {
	case 0:
		list = cw.base.NamespaceList(nil)
	case 1:
		list = cw.base.WorkspaceList(nil, entityPath[0])
	default:
		panic("unsupported entityPath depth")
	}

	m := make(map[string]bool)
	for _, val := range list {
		m[val] = true
	}

	return m
}
