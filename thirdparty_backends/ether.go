// +build linux

// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// Disable this file by replacing "linux" from the first line with "ignore" if you
// want to compile in support for the ether datastores. You will need to do the same
// in daemon/Ether_test.go as well.
package thirdparty_backends

import "bytes"
import "fmt"

import "github.com/aristanetworks/ether/blobstore"
import "github.com/aristanetworks/ether/cql"
import "github.com/aristanetworks/ether/filesystem"
import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/ether/qubit/wsdb"

func init() {
	registerDatastore("ether.filesystem", NewEtherFilesystemStore)
	registerDatastore("ether.cql", NewEtherCqlStore)
	registerWorkspaceDB("ether.cql", NewEtherWorkspaceDB)
}

func NewEtherFilesystemStore(path string) quantumfs.DataStore {

	blobstore, err := filesystem.NewFilesystemStore(path)
	if err != nil {
		fmt.Printf("Failed to init ether.filesystem datastore: %s\n",
			err.Error())
		return nil
	}
	translator := EtherBlobStoreTranslator{blobstore: blobstore}
	return &translator
}

func NewEtherCqlStore(path string) quantumfs.DataStore {

	blobstore, err := cql.NewCqlBlobStore(path)
	if err != nil {
		fmt.Printf("Failed to init ether.cql datastore: %s\n",
			err.Error())
		return nil
	}
	translator := EtherBlobStoreTranslator{blobstore: blobstore}
	return &translator
}

type EtherBlobStoreTranslator struct {
	blobstore blobstore.BlobStore
}

func (ebt *EtherBlobStoreTranslator) Get(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	data, _, err := ebt.blobstore.Get(key.String())

	if err != nil {
		return err
	}

	buf.Set(data, key.Type())
	return nil
}

func (ebt *EtherBlobStoreTranslator) Set(c *quantumfs.Ctx, key quantumfs.ObjectKey,
	buf quantumfs.Buffer) error {

	return ebt.blobstore.Insert(key.String(), buf.Get(), nil)
}

type EtherWsdbTranslator struct {
	wsdb wsdb.WorkspaceDB
}

// convert wsdb.Error to quantumfs.WorkspaceDbErr
func convertWsdbError(e error) error {
	wE, ok := e.(*wsdb.Error)
	if !ok {
		panic("BUG: Errors from wsdb APIs must be of *wsdb.Error type")
	}

	var errCode quantumfs.WsdbErrCode
	switch wE.Code {
	case wsdb.ErrWorkspaceExists:
		errCode = quantumfs.WSDB_WORKSPACE_EXISTS
	case wsdb.ErrWorkspaceNotFound:
		errCode = quantumfs.WSDB_WORKSPACE_NOT_FOUND
	case wsdb.ErrFatal:
		errCode = quantumfs.WSDB_FATAL_DB_ERROR
	case wsdb.ErrWorkspaceOutOfDate:
		errCode = quantumfs.WSDB_OUT_OF_DATE
	default:
		panic(fmt.Sprintf("Bug: Unsupported error %s", e.Error()))
	}

	return quantumfs.NewWorkspaceDbErr(errCode, wE.Msg)
}

func NewEtherWorkspaceDB(path string) quantumfs.WorkspaceDB {
	eWsdb := &EtherWsdbTranslator{
		wsdb: cql.NewWorkspaceDB(path),
	}

	// since generic wsdb API sets up _null/null with nil key
	key, err := eWsdb.wsdb.AdvanceWorkspace("_null", "null",
		[]byte(nil), quantumfs.EmptyWorkspaceKey.Value())
	if err != nil {
		// an existing workspaceDB will have currentRootID as
		// EmptyWorkspaceKey
		wE, _ := err.(*wsdb.Error)
		if wE.Code != wsdb.ErrWorkspaceOutOfDate ||
			!bytes.Equal(key, quantumfs.EmptyWorkspaceKey.Value()) {
			panic(fmt.Sprintf("Failed wsdb setup: %s", err.Error()))
		}
	}

	return eWsdb
}

func (w *EtherWsdbTranslator) NumNamespaces(c *quantumfs.Ctx) (int, error) {
	count, err := w.wsdb.NumNamespaces()
	if err != nil {
		return 0, convertWsdbError(err)
	}
	return count, nil
}

func (w *EtherWsdbTranslator) NamespaceList(c *quantumfs.Ctx) ([]string, error) {
	list, err := w.wsdb.NamespaceList()
	if err != nil {
		return nil, convertWsdbError(err)
	}
	return list, nil
}

func (w *EtherWsdbTranslator) NumWorkspaces(c *quantumfs.Ctx,
	namespace string) (int, error) {

	count, err := w.wsdb.NumWorkspaces(namespace)
	if err != nil {
		return 0, convertWsdbError(err)
	}
	return count, nil
}

func (w *EtherWsdbTranslator) WorkspaceList(c *quantumfs.Ctx,
	namespace string) ([]string, error) {

	list, err := w.wsdb.WorkspaceList(namespace)
	if err != nil {
		return nil, convertWsdbError(err)
	}
	return list, nil
}

func (w *EtherWsdbTranslator) NamespaceExists(c *quantumfs.Ctx,
	namespace string) (bool, error) {

	exists, err := w.wsdb.NamespaceExists(namespace)
	if err != nil {
		return exists, convertWsdbError(err)
	}
	return exists, nil
}

func (w *EtherWsdbTranslator) WorkspaceExists(c *quantumfs.Ctx, namespace string,
	workspace string) (bool, error) {

	exists, err := w.wsdb.WorkspaceExists(namespace, workspace)
	if err != nil {
		return exists, convertWsdbError(err)
	}
	return exists, nil
}

func (w *EtherWsdbTranslator) Workspace(c *quantumfs.Ctx, namespace string,
	workspace string) (quantumfs.ObjectKey, error) {

	key, err := w.wsdb.Workspace(namespace, workspace)
	if err != nil {
		return quantumfs.ObjectKey{}, convertWsdbError(err)
	}

	return quantumfs.NewObjectKeyFromBytes(key), nil
}

func (w *EtherWsdbTranslator) BranchWorkspace(c *quantumfs.Ctx, srcNamespace string,
	srcWorkspace string, dstNamespace string, dstWorkspace string) error {

	err := w.wsdb.BranchWorkspace(srcNamespace, srcWorkspace,
		dstNamespace, dstWorkspace)
	if err != nil {
		return convertWsdbError(err)
	}
	return nil
}

func (w *EtherWsdbTranslator) AdvanceWorkspace(c *quantumfs.Ctx, namespace string,
	workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	key, err := w.wsdb.AdvanceWorkspace(namespace, workspace,
		currentRootId.Value(), newRootId.Value())
	if err != nil {
		return quantumfs.ObjectKey{}, convertWsdbError(err)
	}

	return quantumfs.NewObjectKeyFromBytes(key), nil
}
