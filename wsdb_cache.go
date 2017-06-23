// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"time"

	"github.com/aristanetworks/ether"
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
	ce := newEntityCache(3, 1*time.Second, cwsdb, wsdbFetcherImpl)

	// QFS requires an empty workspaceDB to contain null namespace
	// and null workspace
	// Had to pass ether.DefaultCtx for the lack of a better option.
	ce.InsertEntities(ether.DefaultCtx, wsdb.NullSpaceName, wsdb.NullSpaceName,
		wsdb.NullSpaceName)

	cwsdb.cache = ce

	return cwsdb
}

// --- workspace DB API implementation ---

func (cw *cacheWsdb) NumTypespaces(c ether.Ctx) (int, error) {
	defer c.FuncIn("cacheWsdb::NumTypespaces", "").Out()

	return cw.cache.CountEntities(c), nil
}

func (cw *cacheWsdb) TypespaceList(c ether.Ctx) ([]string, error) {
	defer c.FuncIn("cacheWsdb::TypespaceList", "").Out()

	return cw.cache.ListEntities(c), nil
}

func (cw *cacheWsdb) NumNamespaces(c ether.Ctx, typespace string) (int, error) {
	defer c.FuncIn("cacheWsdb::NumNamespaces", "%s", typespace).Out()

	return cw.cache.CountEntities(c, typespace), nil
}

func (cw *cacheWsdb) NamespaceList(c ether.Ctx,
	typespace string) ([]string, error) {

	defer c.FuncIn("cacheWsdb::NamespaceList", "%s", typespace).Out()

	return cw.cache.ListEntities(c, typespace), nil
}

func (cw *cacheWsdb) NumWorkspaces(c ether.Ctx, typespace,
	namespace string) (int, error) {
	defer c.FuncIn("cacheWsdb::NumWorkspaces", "%s/%s", typespace, namespace).Out()

	return cw.cache.CountEntities(c, typespace, namespace), nil
}

func (cw *cacheWsdb) WorkspaceList(c ether.Ctx, typespace string,
	namespace string) ([]string, error) {

	defer c.FuncIn("cacheWsdb::WorkspaceList", "%s/%s", typespace, namespace).Out()

	return cw.cache.ListEntities(c, typespace, namespace), nil
}

func (cw *cacheWsdb) TypespaceExists(c ether.Ctx,
	typespace string) (bool, error) {

	defer c.FuncIn("cacheWsdb::TypespaceExists", "%s", typespace).Out()

	exist, err := cw.base.TypespaceExists(c, typespace)
	if err != nil {
		return exist, err
	}

	if !exist {
		cw.cache.DeleteEntities(c, typespace)
	} else {
		cw.cache.InsertEntities(c, typespace)
	}

	return exist, nil
}

func (cw *cacheWsdb) NamespaceExists(c ether.Ctx, typespace string,
	namespace string) (bool, error) {

	defer c.FuncIn("cacheWsdb::NamespaceExists", "%s/%s", typespace, namespace).Out()

	exist, err := cw.base.NamespaceExists(c, typespace, namespace)
	if err != nil {
		return exist, err
	}

	if !exist {
		cw.cache.DeleteEntities(c, typespace, namespace)
	} else {
		cw.cache.InsertEntities(c, typespace, namespace)
	}

	return exist, nil
}

func (cw *cacheWsdb) WorkspaceExists(c ether.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	defer c.FuncIn("cacheWsdb::WorkspaceExists", "%s/%s/%s", typespace, namespace,
		workspace).Out()

	exist, err := cw.base.WorkspaceExists(c, typespace, namespace, workspace)
	if err != nil {
		return exist, err
	}

	if !exist {
		cw.cache.DeleteEntities(c, typespace, namespace, workspace)
	} else {
		cw.cache.InsertEntities(c, typespace, namespace, workspace)
	}

	return exist, nil
}

func (cw *cacheWsdb) BranchWorkspace(c ether.Ctx, srcTypespace string, srcNamespace string,
	srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	start := time.Now()
	defer func() { cw.branchStats.RecordOp(time.Since(start)) }()

	defer c.FuncIn("cacheWsdb::BranchWorkspace", "%s/%s/%s -> %s/%s/%s)", srcTypespace,
		srcNamespace, srcWorkspace, dstTypespace, dstNamespace, dstWorkspace).Out()

	if err := cw.base.BranchWorkspace(c, srcTypespace, srcNamespace, srcWorkspace,
		dstTypespace, dstNamespace, dstWorkspace); err != nil {
		return err
	}

	cw.cache.InsertEntities(c, srcTypespace, srcNamespace, srcWorkspace)
	cw.cache.InsertEntities(c, dstTypespace, dstNamespace, dstWorkspace)

	return nil
}

func (cw *cacheWsdb) DeleteWorkspace(c ether.Ctx, typespace string, namespace string,
	workspace string) error {

	defer c.FuncIn("cacheWsdb::DeleteWorkspace", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	if err := cw.base.DeleteWorkspace(c, typespace, namespace,
		workspace); err != nil {
		return err
	}

	cw.cache.DeleteEntities(c, typespace, namespace, workspace)
	return nil
}

func (cw *cacheWsdb) Workspace(c ether.Ctx, typespace string, namespace string,
	workspace string) (wsdb.ObjectKey, error) {

	defer c.FuncIn("cacheWsdb::Workspace", "%s/%s/%s", typespace, namespace,
		workspace).Out()

	key, err := cw.base.Workspace(c, typespace, namespace, workspace)
	if err != nil {
		return wsdb.ObjectKey{}, err
	}
	cw.cache.InsertEntities(c, typespace, namespace, workspace)
	return key, nil
}

func (cw *cacheWsdb) AdvanceWorkspace(c ether.Ctx, typespace string,
	namespace string, workspace string, currentRootID wsdb.ObjectKey,
	newRootID wsdb.ObjectKey) (wsdb.ObjectKey, error) {

	defer c.FuncIn("cacheWsdb::AdvanceWorkspace", "%s/%s/%s(%s -> %s)", typespace, namespace,
		workspace, currentRootID, newRootID).Out()

	start := time.Now()
	defer func() { cw.advanceStats.RecordOp(time.Since(start)) }()

	key, err := cw.base.AdvanceWorkspace(c, typespace, namespace,
		workspace, currentRootID, newRootID)
	if err != nil {
		return key, err
	}

	cw.cache.InsertEntities(c, typespace, namespace, workspace)

	return key, nil
}

// wsdbFetcherImpl implements fetcher interface in entity cache
// the returned map contains entities inserted on local node or
// insertions from other nodes in the CQL cluster
func wsdbFetcherImpl(c ether.Ctx, arg interface{}, entityPath ...string) map[string]bool {
	cw, ok := arg.(*cacheWsdb)
	if !ok {
		panic("unsupported type of arg")
	}

	var list []string
	switch len(entityPath) {
	case 0:
		list, _ = cw.base.TypespaceList(c)
	case 1:
		list, _ = cw.base.NamespaceList(c, entityPath[0])
	case 2:
		list, _ = cw.base.WorkspaceList(c, entityPath[0], entityPath[1])
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
