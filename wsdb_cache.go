// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"time"

	"github.com/aristanetworks/ether/qubit/wsdb"
	"github.com/aristanetworks/ether/utils/stats"
	"github.com/aristanetworks/ether/utils/stats/inmem"
)

// In this implementation of workspace DB API,
// some APIs use cached information and do not
// interact with underlying datastore. All APIs interact
// with the cache to maintain local coherency. This
// implementation relies on the uncached workspace DB
// API to interact with underlying datastore.

type cacheWsdb struct {
	base  wsdb.WorkspaceDB
	cache *entityCache

	branchStats  stats.OpStats
	advanceStats stats.OpStats
}

// this wsdb implementation wraps any wsdb (base) implementation
// with entity cache
func newCacheWsdb(base wsdb.WorkspaceDB, cfg WsDBConfig) wsdb.WorkspaceDB {

	cwsdb := &cacheWsdb{
		base:         base,
		branchStats:  inmem.NewOpStatsInMem("branchWorkspace"),
		advanceStats: inmem.NewOpStatsInMem("advanceWorkspace"),
	}

	// TODO: default max cache age can be a configuration parameter?
	c := newEntityCache(3, 1*time.Second, cwsdb, wsdbFetcherImpl)

	// QFS requires an empty workspaceDB to contain null namespace
	// and null workspace
	c.InsertEntities(wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName)

	cwsdb.cache = c

	return cwsdb
}

// --- workspace DB API implementation ---

func (cw *cacheWsdb) NumTypespaces() (int, error) {
	return cw.cache.CountEntities(), nil
}

func (cw *cacheWsdb) TypespaceList() ([]string, error) {
	return cw.cache.ListEntities(), nil
}

func (cw *cacheWsdb) NumNamespaces(typespace string) (int, error) {
	return cw.cache.CountEntities(typespace), nil
}

func (cw *cacheWsdb) NamespaceList(
	typespace string) ([]string, error) {

	return cw.cache.ListEntities(typespace), nil
}

func (cw *cacheWsdb) NumWorkspaces(typespace,
	namespace string) (int, error) {

	return cw.cache.CountEntities(typespace, namespace), nil
}

func (cw *cacheWsdb) WorkspaceList(typespace string,
	namespace string) ([]string, error) {

	return cw.cache.ListEntities(typespace, namespace), nil
}

func (cw *cacheWsdb) TypespaceExists(
	typespace string) (bool, error) {

	exist, err := cw.base.TypespaceExists(typespace)
	if err != nil {
		return exist, err
	}

	if !exist {
		cw.cache.DeleteEntities(typespace)
	} else {
		cw.cache.InsertEntities(typespace)
	}

	return exist, nil
}

func (cw *cacheWsdb) NamespaceExists(typespace string,
	namespace string) (bool, error) {

	exist, err := cw.base.NamespaceExists(typespace, namespace)
	if err != nil {
		return exist, err
	}

	if !exist {
		cw.cache.DeleteEntities(typespace, namespace)
	} else {
		cw.cache.InsertEntities(typespace, namespace)
	}

	return exist, nil
}

func (cw *cacheWsdb) WorkspaceExists(typespace string,
	namespace string, workspace string) (bool, error) {

	exist, err := cw.base.WorkspaceExists(typespace, namespace, workspace)
	if err != nil {
		return exist, err
	}

	if !exist {
		cw.cache.DeleteEntities(typespace, namespace, workspace)
	} else {
		cw.cache.InsertEntities(typespace, namespace, workspace)
	}

	return exist, nil
}

func (cw *cacheWsdb) BranchWorkspace(srcTypespace string, srcNamespace string,
	srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	start := time.Now()
	defer func() { cw.branchStats.RecordOp(time.Since(start)) }()

	if err := cw.base.BranchWorkspace(srcTypespace, srcNamespace, srcWorkspace,
		dstTypespace, dstNamespace, dstWorkspace); err != nil {
		return err
	}

	cw.cache.InsertEntities(srcTypespace, srcNamespace, srcWorkspace)
	cw.cache.InsertEntities(dstTypespace, dstNamespace, dstWorkspace)

	return nil
}

func (cw *cacheWsdb) DeleteWorkspace(typespace string, namespace string,
	workspace string) error {

	if err := cw.base.DeleteWorkspace(typespace, namespace,
		workspace); err != nil {
		return err
	}

	cw.cache.DeleteEntities(typespace, namespace, workspace)
	return nil
}
func (cw *cacheWsdb) Workspace(typespace string, namespace string,
	workspace string) (wsdb.ObjectKey, error) {

	key, err := cw.base.Workspace(typespace, namespace, workspace)
	if err != nil {
		return wsdb.ObjectKey{}, err
	}
	cw.cache.InsertEntities(typespace, namespace, workspace)
	return key, nil
}

func (cw *cacheWsdb) AdvanceWorkspace(typespace string,
	namespace string, workspace string, currentRootID wsdb.ObjectKey,
	newRootID wsdb.ObjectKey) (wsdb.ObjectKey, error) {

	start := time.Now()
	defer func() { cw.advanceStats.RecordOp(time.Since(start)) }()

	key, err := cw.base.AdvanceWorkspace(typespace, namespace,
		workspace, currentRootID, newRootID)
	if err != nil {
		return key, err
	}

	cw.cache.InsertEntities(typespace, namespace, workspace)

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
		list, _ = cw.base.TypespaceList()
	case 1:
		list, _ = cw.base.NamespaceList(entityPath[0])
	case 2:
		list, _ = cw.base.WorkspaceList(entityPath[0], entityPath[1])
	default:
		panic("unsupported entityPath depth")
	}

	m := make(map[string]bool)
	for _, val := range list {
		m[val] = true
	}

	return m
}

func (cw *cacheWsdb) ReportAPIStats() {
	cw.branchStats.(stats.OpStatReporter).ReportOpStats()
	cw.advanceStats.(stats.OpStatReporter).ReportOpStats()
}
