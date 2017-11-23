// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"fmt"

	"github.com/boltdb/bolt"
)

var bucketName = []byte("onDiskMap")

// OnDiskSet mimics the a Set API
// and is backed by a boltdb
type OnDiskSet struct {
	db *bolt.DB
}

// NewOnDiskSet returns a new instance of OnDiskSet
func NewOnDiskSet(dbFileName string) (*OnDiskSet, error) {
	handle, err := bolt.Open(dbFileName, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = handle.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket(bucketName)
		if err != nil {
			return fmt.Errorf("NewOnDiskSet:: CreateBucket: %v", err)
		}
		return nil
	})
	handle.NoSync = true
	ods := &OnDiskSet{
		db: handle,
	}
	return ods, err
}

// Insert the given key  in the Set,
// return true if Key was already Present
func (ods *OnDiskSet) Insert(key string) (bool, error) {
	present := false
	err := ods.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)

		// First check if the key is present
		val := bucket.Get([]byte(key))
		if val != nil {
			present = true
			return nil
		}

		err := bucket.Put([]byte(key), []byte(nil))
		return err
	})
	return present, err
}

// Delete the given key from the Set
func (ods *OnDiskSet) Delete(key string) error {
	return ods.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		err := bucket.Delete([]byte(key))
		return err
	})
}

// IsPresent returns true if the Key is present in the map
func (ods *OnDiskSet) IsPresent(key string) bool {
	present := false
	_ = ods.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		val := bucket.Get([]byte(key))
		if val != nil {
			present = true
			return nil
		}
		return fmt.Errorf("Key not found")

	})
	return present
}

func (ods *OnDiskSet) Count() (int, error) {
	count := 0
	err := ods.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		count = bucket.Stats().KeyN
		return nil
	})
	return count, err
}

func (ods *OnDiskSet) Close() {
	defer ods.db.Close()
	ods.db.Sync()
}

func (ods *OnDiskSet) Sync() error {
	return ods.db.Sync()
}
