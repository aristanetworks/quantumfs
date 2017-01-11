// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package systemlocal

import "bytes"
import "fmt"
import "strings"
import "time"

import "github.com/boltdb/bolt"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"

var namespacesBucket []byte

func init() {
	namespacesBucket = []byte("Namespaces")
}

// The database underlying the system local WorkspaceDB has two levels of buckets.
//
// The first level bucket are all the namespaces. Keys into this bucket are namespace
// names and are mapped to the second level bucket.
//
// The second level of buckets are the workspaces within each namespace.
//
// This structure maps naturally to the heirarchical nature of workspace naming and
// helps us to perform namespace and workspace counts easily.

func NewWorkspaceDB(conf string) quantumfs.WorkspaceDB {
	var options *bolt.Options

	if strings.HasPrefix(conf, "/tmp") {
		// We are running inside a test, don't wait forever
		options = &bolt.Options{
			Timeout: 100 * time.Millisecond,
		}
	}

	db, err := bolt.Open(conf, 0600, options)
	if err != nil {
		panic(err.Error())
	}

	if strings.HasPrefix(conf, "/tmp") {
		// We are running inside a test, syncing can only slow us down
		db.NoSync = true
	}

	wsdb := &WorkspaceDB{
		db: db,
	}

	// Create the null workspace
	db.Update(func(tx *bolt.Tx) error {
		namespaces, err := tx.CreateBucketIfNotExists(namespacesBucket)
		if err != nil {
			panic("Unable to create Namespaces bucket")
		}

		_null, err := namespaces.CreateBucketIfNotExists([]byte("_null"))
		if err != nil {
			panic("Unable to create _null namespace")
		}

		err = _null.Put([]byte("null"), quantumfs.EmptyWorkspaceKey.Value())
		if err != nil {
			panic("Unable to reset null workspace")
		}

		return nil
	})
	return wsdb
}

// WorkspaceDB is a persistent, system local quantumfs.WorkspaceDB. It only supports
// one Quantumfs instance at a time however.
type WorkspaceDB struct {
	db *bolt.DB
}

func (wsdb *WorkspaceDB) NumNamespaces(c *quantumfs.Ctx) (int, error) {
	var num int

	wsdb.db.View(func(tx *bolt.Tx) error {
		namespaces := tx.Bucket(namespacesBucket)

		namespaces.ForEach(func(k []byte, v []byte) error {
			num++
			return nil
		})

		return nil
	})

	return num, nil
}

func (wsdb *WorkspaceDB) NamespaceList(c *quantumfs.Ctx) ([]string, error) {
	namespaceList := make([]string, 0, 100)

	wsdb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(namespacesBucket)
		namespaces := bucket.Cursor()
		for n, _ := namespaces.First(); n != nil; n, _ = namespaces.Next() {
			namespaceList = append(namespaceList, string(n))
		}

		return nil
	})

	return namespaceList, nil
}

func (wsdb *WorkspaceDB) NumWorkspaces(c *quantumfs.Ctx, namespace string) (int,
	error) {

	var num int

	wsdb.db.View(func(tx *bolt.Tx) error {
		namespaces := tx.Bucket(namespacesBucket)
		workspaces := namespaces.Bucket([]byte(namespace))
		workspaces.ForEach(func(k []byte, v []byte) error {
			num++
			return nil
		})

		return nil
	})

	return num, nil
}

func (wsdb *WorkspaceDB) WorkspaceList(c *quantumfs.Ctx, namespace string) ([]string,
	error) {

	workspaceList := make([]string, 0, 100)

	wsdb.db.View(func(tx *bolt.Tx) error {
		namespaces := tx.Bucket(namespacesBucket)
		bucket := namespaces.Bucket([]byte(namespace))
		workspaces := bucket.Cursor()
		for w, _ := workspaces.First(); w != nil; w, _ = workspaces.Next() {
			workspaceList = append(workspaceList, string(w))
		}

		return nil
	})

	return workspaceList, nil
}

func (wsdb *WorkspaceDB) NamespaceExists(c *quantumfs.Ctx, namespace string) (bool,
	error) {

	var exists bool

	wsdb.db.View(func(tx *bolt.Tx) error {
		namespaces := tx.Bucket(namespacesBucket)
		bucket := namespaces.Bucket([]byte(namespace))
		if bucket != nil {
			exists = true
		}

		return nil
	})

	return exists, nil
}

// Get the workspace key. This must be run inside a boltDB transaction
func getWorkspaceKey_(tx *bolt.Tx, namespace string, workspace string) []byte {
	namespaces := tx.Bucket(namespacesBucket)
	workspaces := namespaces.Bucket([]byte(namespace))
	if workspaces == nil {
		// The namespace doesn't exist, so the workspace cannot exist
		return nil
	}

	return workspaces.Get([]byte(workspace))
}

func (wsdb *WorkspaceDB) WorkspaceExists(c *quantumfs.Ctx, namespace string,
	workspace string) (bool, error) {

	var exists bool

	wsdb.db.View(func(tx *bolt.Tx) error {
		key := getWorkspaceKey_(tx, namespace, workspace)
		if key != nil {
			exists = true
		}

		return nil
	})

	return exists, nil
}

func (wsdb *WorkspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcNamespace string,
	srcWorkspace string, dstNamespace string, dstWorkspace string) error {

	return wsdb.db.Update(func(tx *bolt.Tx) error {
		// Get the source workspace rootID
		namespaces := tx.Bucket(namespacesBucket)
		workspaces := namespaces.Bucket([]byte(srcNamespace))
		if workspaces == nil {
			return fmt.Errorf("Source Namespace doesn't exist")
		}

		srcKey := workspaces.Get([]byte(srcWorkspace))
		if srcKey == nil {
			return fmt.Errorf("Source Workspace doesn't exist")
		}

		// Create the destination namespace if necessary
		workspaces, err := namespaces.CreateBucketIfNotExists(
			[]byte(dstNamespace))
		if err != nil {
			return err
		}

		dstKey := workspaces.Get([]byte(dstWorkspace))
		if dstKey != nil {
			return fmt.Errorf("Destination Workspace already exists")
		}

		err = workspaces.Put([]byte(dstWorkspace), srcKey)

		if c != nil {
			objectKey := quantumfs.NewObjectKeyFromBytes(srcKey)
			c.Dlog(qlog.LogWorkspaceDb,
				"Branched workspace '%s/%s' to '%s/%s' with key %s",
				srcNamespace, srcWorkspace, dstNamespace,
				dstWorkspace, objectKey.String())
		}

		return nil
	})
}

func (wsdb *WorkspaceDB) Workspace(c *quantumfs.Ctx, namespace string,
	workspace string) (quantumfs.ObjectKey, error) {

	var rootid quantumfs.ObjectKey

	wsdb.db.View(func(tx *bolt.Tx) error {
		key := getWorkspaceKey_(tx, namespace, workspace)

		if key == nil {
			panic("Got nil rootid")
		}

		rootid = quantumfs.NewObjectKeyFromBytes(key)

		return nil
	})

	return rootid, nil
}

func (wsdb *WorkspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, namespace string,
	workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	var dbRootId quantumfs.ObjectKey

	err := wsdb.db.Update(func(tx *bolt.Tx) error {
		rootId := getWorkspaceKey_(tx, namespace, workspace)
		if rootId == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"Advance failed")
		}

		if !bytes.Equal(currentRootId.Value(), rootId) {
			dbRootId = quantumfs.NewObjectKeyFromBytes(rootId)
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_OUT_OF_DATE,
				"%s vs %s Advance failed", currentRootId.String(),
				dbRootId.String())

		}

		if c != nil {
			c.Qlog.Log(qlog.LogWorkspaceDb, uint64(c.RequestId), 3,
				"WorkspaceDB::AdvanceWorkspace %s/%s %s", namespace,
				workspace, newRootId.String())
		}

		// The workspace exists and the caller has the uptodate rootid, so
		// advance the rootid in the DB.
		namespaces := tx.Bucket(namespacesBucket)
		workspaces := namespaces.Bucket([]byte(namespace))
		err := workspaces.Put([]byte(workspace), newRootId.Value())
		if err == nil {
			dbRootId = newRootId
		}
		return err
	})

	return dbRootId, err
}
