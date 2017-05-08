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

var typespacesBucket []byte
var stateTypespacesBucket []byte

func init() {
	typespacesBucket = []byte("Typespaces")
	stateTypespacesBucket = []byte("StateTypespaces")
}

// The database underlying the system local workspaceDB has three levels of buckets.
//
// The first level bucket are all the typespaces. Keys into this bucket are typespace
// names and are mapped to the second level bucket.
//
// The second level bucket are all the namespaces. Keys into this bucket are
// namespace names and are mapped to the third level bucket.
//
// The third level of buckets are the workspaces within each namespace.
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

	wsdb := &workspaceDB{
		db: db,
	}

	// Create the null workspace
	db.Update(func(tx *bolt.Tx) error {
		insertMap(tx, typespacesBucket)
		insertMap(tx, stateTypespacesBucket)

		return nil
	})
	return wsdb
}

func insertMap(tx *bolt.Tx, bucketKey []byte) {
	typespaces, err := tx.CreateBucketIfNotExists(bucketKey)
	if err != nil {
		panic("Unable to create Typespaces bucket")
	}

	_null_, err := typespaces.CreateBucketIfNotExists([]byte("" +
		quantumfs.NullSpaceName))
	if err != nil {
		panic("Unable to create null typespace")
	}

	_null, err := _null_.CreateBucketIfNotExists([]byte("" +
		quantumfs.NullSpaceName))
	if err != nil {
		panic("Unable to create null namespace")
	}

	err = _null.Put([]byte(quantumfs.NullSpaceName),
		quantumfs.EmptyWorkspaceKey.Value())
	if err != nil {
		panic("Unable to reset null workspace")
	}
}

// workspaceDB is a persistent, system local quantumfs.WorkspaceDB. It only supports
// one Quantumfs instance at a time however.
type workspaceDB struct {
	db *bolt.DB
}

func (wsdb *workspaceDB) NumTypespaces(c *quantumfs.Ctx) (int, error) {
	var num int

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NumTypespaces").Out()

	wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)

		typespaces.ForEach(func(k []byte, v []byte) error {
			num++
			return nil
		})

		return nil
	})

	return num, nil
}

func (wsdb *workspaceDB) TypespaceList(c *quantumfs.Ctx) ([]string, error) {
	typespaceList := make([]string, 0, 100)

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::TypespaceList").Out()

	wsdb.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(typespacesBucket)
		typespaces := bucket.Cursor()
		for n, _ := typespaces.First(); n != nil; n, _ = typespaces.Next() {
			typespaceList = append(typespaceList, string(n))
		}

		return nil
	})

	return typespaceList, nil
}

func (wsdb *workspaceDB) NumNamespaces(c *quantumfs.Ctx, typespace string) (int,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NumNamespaces").Out()

	var num int

	wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		namespaces.ForEach(func(k []byte, v []byte) error {
			num++
			return nil
		})

		return nil
	})

	return num, nil
}

func (wsdb *workspaceDB) NamespaceList(c *quantumfs.Ctx, typespace string) ([]string,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NamespaceList").Out()

	namespaceList := make([]string, 0, 100)

	wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		bucket := typespaces.Bucket([]byte(typespace))
		namespaces := bucket.Cursor()
		for n, _ := namespaces.First(); n != nil; n, _ = namespaces.Next() {
			namespaceList = append(namespaceList, string(n))
		}

		return nil
	})

	return namespaceList, nil
}

func (wsdb *workspaceDB) NumWorkspaces(c *quantumfs.Ctx, typespace string,
	namespace string) (int, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NumWorkspaces").Out()

	var num int

	wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		workspaces := namespaces.Bucket([]byte(namespace))
		workspaces.ForEach(func(k []byte, v []byte) error {
			num++
			return nil
		})

		return nil
	})

	return num, nil
}

func (wsdb *workspaceDB) WorkspaceList(c *quantumfs.Ctx, typespace string,
	namespace string) ([]string, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::WorkspaceList").Out()

	workspaceList := make([]string, 0, 100)

	wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		bucket := namespaces.Bucket([]byte(namespace))
		workspaces := bucket.Cursor()
		for w, _ := workspaces.First(); w != nil; w, _ = workspaces.Next() {
			workspaceList = append(workspaceList, string(w))
		}

		return nil
	})

	return workspaceList, nil
}

func (wsdb *workspaceDB) TypespaceExists(c *quantumfs.Ctx, typespace string) (bool,
	error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::TypespaceExists").Out()

	var exists bool

	wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		bucket := typespaces.Bucket([]byte(typespace))
		if bucket != nil {
			exists = true
		}

		return nil
	})

	return exists, nil
}

func (wsdb *workspaceDB) NamespaceExists(c *quantumfs.Ctx, typespace string,
	namespace string) (bool, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::NamespaceExists").Out()

	var exists bool

	wsdb.db.View(func(tx *bolt.Tx) error {
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		if namespaces == nil {
			// The typespace doesn't exist, so the namespace cannot exist
			return nil
		}

		workspaces := namespaces.Bucket([]byte(namespace))
		if workspaces != nil {
			exists = true
		}

		return nil
	})

	return exists, nil
}

// Get the workspace key. This must be run inside a boltDB transaction
func getWorkspaceContent_(tx *bolt.Tx, root []byte, typespace string,
	namespace string, workspace string) []byte {

	typespaces := tx.Bucket(root)
	namespaces := typespaces.Bucket([]byte(typespace))
	if namespaces == nil {
		// The typespace doesn't exist, so the namespace cannot exist
		return nil
	}

	workspaces := namespaces.Bucket([]byte(namespace))
	if workspaces == nil {
		// The namespace doesn't exist, so the workspace cannot exist
		return nil
	}

	return workspaces.Get([]byte(workspace))
}

func (wsdb *workspaceDB) WorkspaceExists(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::WorkspaceExists").Out()

	var exists bool

	wsdb.db.View(func(tx *bolt.Tx) error {
		key := getWorkspaceContent_(tx, typespacesBucket,
			typespace, namespace, workspace)
		if key != nil {
			exists = true
		}

		return nil
	})

	return exists, nil
}

func (wsdb *workspaceDB) BranchWorkspace(c *quantumfs.Ctx, srcTypespace string,
	srcNamespace string, srcWorkspace string, dstTypespace string,
	dstNamespace string, dstWorkspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::BranchWorkspace").Out()

	return wsdb.db.Update(func(tx *bolt.Tx) error {
		// Get the source workspace rootID
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(srcTypespace))
		if namespaces == nil {
			return fmt.Errorf("Source Typespace doesn't exist")
		}
		workspaces := namespaces.Bucket([]byte(srcNamespace))
		if workspaces == nil {
			return fmt.Errorf("Source Namespace doesn't exist")
		}

		srcKey := workspaces.Get([]byte(srcWorkspace))
		if srcKey == nil {
			return fmt.Errorf("Source Workspace doesn't exist")
		}

		// Create the destination typespace/namespace if necessary
		workspaces, err := getWorkspaces(typespaces,
			dstTypespace, dstNamespace)
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
				"Branch workspace '%s/%s/%s' to '%s/%s/%s' with %s",
				srcTypespace, srcNamespace, srcWorkspace,
				dstTypespace, dstNamespace, dstWorkspace,
				objectKey.String())
		}

		return nil
	})
}

func deleteWorkspace_(tx *bolt.Tx, root []byte, typespace string,
	namespace string, workspace string) error {

	typespaces := tx.Bucket(root)
	namespaces := typespaces.Bucket([]byte(typespace))
	if namespaces == nil {
		// No such typespace indicates no such workspace. Success.
		return nil
	}

	workspaces := namespaces.Bucket([]byte(namespace))
	if workspaces == nil {
		// No such namespace indicates no such workspace. Success.
		return nil
	}

	err := workspaces.Delete([]byte(workspace))
	if err != nil {
		return err
	}

	var count int
	workspaces.ForEach(func(k []byte, v []byte) error {
		count++
		return nil
	})

	if count == 0 {
		err = namespaces.DeleteBucket([]byte(namespace))
		if err != nil {
			return err
		}
	}

	count = 0
	namespaces.ForEach(func(k []byte, v []byte) error {
		count++
		return nil
	})

	if count == 0 {
		err = typespaces.DeleteBucket([]byte(typespace))
		if err != nil {
			return err
		}
	}

	return nil
}

func (wsdb *workspaceDB) DeleteWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::DeleteWorkspace").Out()

	return wsdb.db.Update(func(tx *bolt.Tx) error {
		err := deleteWorkspace_(tx, typespacesBucket,
			typespace, namespace, workspace)
		if err != nil {
			return err
		}

		// Remove the possible record in the state table
		return deleteWorkspace_(tx, stateTypespacesBucket,
			typespace, namespace, workspace)
	})
}

func (wsdb *workspaceDB) Workspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::Workspace").Out()

	var rootid quantumfs.ObjectKey

	err := wsdb.db.View(func(tx *bolt.Tx) error {
		key := getWorkspaceContent_(tx, typespacesBucket,
			typespace, namespace, workspace)

		if key == nil {
			return quantumfs.NewWorkspaceDbErr(
				quantumfs.WSDB_WORKSPACE_NOT_FOUND,
				"workspace does not exist")
		}

		rootid = quantumfs.NewObjectKeyFromBytes(key)

		return nil
	})

	return rootid, err
}

func (wsdb *workspaceDB) AdvanceWorkspace(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string, currentRootId quantumfs.ObjectKey,
	newRootId quantumfs.ObjectKey) (quantumfs.ObjectKey, error) {

	defer c.FuncInName(qlog.LogWorkspaceDb, "systemlocal::AdvanceWorkspace").Out()

	var dbRootId quantumfs.ObjectKey

	err := wsdb.db.Update(func(tx *bolt.Tx) error {
		rootId := getWorkspaceContent_(tx, typespacesBucket,
			typespace, namespace, workspace)
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
				"WorkspaceDB::AdvanceWorkspace %s/%s/%s %s",
				typespace, namespace, workspace, newRootId.String())
		}

		// The workspace exists and the caller has the uptodate rootid, so
		// advance the rootid in the DB.
		typespaces := tx.Bucket(typespacesBucket)
		namespaces := typespaces.Bucket([]byte(typespace))
		workspaces := namespaces.Bucket([]byte(namespace))
		err := workspaces.Put([]byte(workspace), newRootId.Value())
		if err == nil {
			dbRootId = newRootId
		}
		return err
	})

	return dbRootId, err
}

func (wsdb *workspaceDB) WorkspaceIsImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) (bool, error) {

	var exists bool
	err := wsdb.db.View(func(tx *bolt.Tx) error {
		state := getWorkspaceContent_(tx, stateTypespacesBucket,
			typespace, namespace, workspace)
		if state != nil {
			exists = true
		}
		return nil
	})

	return exists, err
}

func getWorkspaces(typespaces *bolt.Bucket, typespace string,
	namespace string) (*bolt.Bucket, error) {

	namespaces, err := typespaces.CreateBucketIfNotExists([]byte(typespace))
	if err != nil {
		return nil, err
	}

	workspaces, err := namespaces.CreateBucketIfNotExists([]byte(namespace))
	if err != nil {
		return nil, err
	}
	return workspaces, nil
}

func (wsdb *workspaceDB) SetWorkspaceImmutable(c *quantumfs.Ctx, typespace string,
	namespace string, workspace string) error {

	return wsdb.db.Update(func(tx *bolt.Tx) error {

		typespaces := tx.Bucket(stateTypespacesBucket)
		workspaces, err := getWorkspaces(typespaces, typespace, namespace)
		if err != nil {
			return err
		}

		err = workspaces.Put([]byte(workspace), []byte("T"))
		if err != nil {
			return err
		}

		return nil
	})
}
