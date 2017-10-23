// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"encoding/hex"
	"fmt"
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

const defaultCacheTimeoutSecs = 1

// this wsdb implementation wraps any wsdb (base) implementation
// with entity cache
func newCacheWsdb(base wsdb.WorkspaceDB, cfg WsDBConfig) wsdb.WorkspaceDB {

	cwsdb := &cacheWsdb{
		base:         base,
		branchStats:  inmem.NewOpStatsInMem("branchWorkspace"),
		advanceStats: inmem.NewOpStatsInMem("advanceWorkspace"),
	}

	cacheTimeout := cfg.CacheTimeoutSecs
	if cacheTimeout == 0 {
		cacheTimeout = defaultCacheTimeoutSecs
	} else if cacheTimeout < 0 && cacheTimeout != DontExpireWsdbCache {
		panic(fmt.Sprintf("Unsupported CacheTimeoutSecs value: %d in wsdb configuration\n",
			cacheTimeout))
	}

	ce := newEntityCache(3, cacheTimeout, cwsdb, wsdbFetcherImpl)

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
	defer c.FuncInName("cacheWsdb::NumTypespaces").Out()

	return cw.cache.CountEntities(c)
}

func (cw *cacheWsdb) TypespaceList(c ether.Ctx) ([]string, error) {
	defer c.FuncInName("cacheWsdb::TypespaceList").Out()

	return cw.cache.ListEntities(c)
}

func (cw *cacheWsdb) NumNamespaces(c ether.Ctx, typespace string) (int, error) {
	defer c.FuncIn("cacheWsdb::NumNamespaces", "%s", typespace).Out()

	return cw.cache.CountEntities(c, typespace)
}

func (cw *cacheWsdb) NamespaceList(c ether.Ctx,
	typespace string) ([]string, error) {

	defer c.FuncIn("cacheWsdb::NamespaceList", "%s", typespace).Out()

	return cw.cache.ListEntities(c, typespace)
}

func (cw *cacheWsdb) NumWorkspaces(c ether.Ctx, typespace,
	namespace string) (int, error) {
	defer c.FuncIn("cacheWsdb::NumWorkspaces", "%s/%s", typespace, namespace).Out()

	return cw.cache.CountEntities(c, typespace, namespace)
}

func (cw *cacheWsdb) WorkspaceList(c ether.Ctx, typespace string,
	namespace string) ([]string, error) {

	defer c.FuncIn("cacheWsdb::WorkspaceList", "%s/%s", typespace, namespace).Out()

	return cw.cache.ListEntities(c, typespace, namespace)
}

func (cw *cacheWsdb) CreateWorkspace(c ether.Ctx, typespace string, namespace string,
	workspace string, wsKey wsdb.ObjectKey) error {

	keyHex := hex.EncodeToString(wsKey)
	defer c.FuncIn("cacheWsdb::CreateWorkspace", "%s/%s/%s(%s)", typespace, namespace,
		workspace, keyHex).Out()

	err := cw.base.CreateWorkspace(c, typespace, namespace, workspace, wsKey)
	if err != nil {
		return err
	}
	return nil
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

func (cw *cacheWsdb) WorkspaceLastWriteTime(c ether.Ctx, typespace string, namespace string,
	workspace string) (time.Time, error) {

	defer c.FuncIn("cacheWsdb::WorkspaceLastWriteTime", "%s/%s/%s", typespace,
		namespace, workspace).Out()

	ts, err := cw.base.WorkspaceLastWriteTime(c, typespace, namespace,
		workspace)
	if err != nil {
		return time.Time{}, err
	}

	return ts, nil
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

	currentKeyHex := hex.EncodeToString(currentRootID)
	newKeyHex := hex.EncodeToString(newRootID)

	defer c.FuncIn("cacheWsdb::AdvanceWorkspace", "%s/%s/%s(%s -> %s)", typespace, namespace,
		workspace, currentKeyHex, newKeyHex).Out()

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
func wsdbFetcherImpl(c ether.Ctx, arg interface{},
	entityPath ...string) (map[string]bool, error) {

	cw, ok := arg.(*cacheWsdb)
	if !ok {
		panic("unsupported type of arg")
	}

	var list []string
	var err error
	switch len(entityPath) {
	case 0:
		list, err = cw.base.TypespaceList(c)
	case 1:
		list, err = cw.base.NamespaceList(c, entityPath[0])
	case 2:
		list, err = cw.base.WorkspaceList(c, entityPath[0], entityPath[1])
	default:
		panic("unsupported entityPath depth")
	}

	m := make(map[string]bool)
	if err != nil {
		return m, err
	}

	for _, val := range list {
		m[val] = true
	}

	return m, nil
}

func (cw *cacheWsdb) ReportAPIStats(c ether.Ctx) {
	defer c.FuncInName("cacheWsdb::ReportAPIStats").Out()

	cw.branchStats.(stats.OpStatReporter).ReportOpStats()
	cw.advanceStats.(stats.OpStatReporter).ReportOpStats()
}
