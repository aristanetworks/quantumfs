// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"encoding/binary"
	"fmt"

	"github.com/boltdb/bolt"
)

var bucketName = []byte("onDiskMap")

// OnDiskSet mimics the a Set API
// and is backed by a boltdb.
// Maintains count for evert element
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

// InsertElem inserts the given element in the Set,
// returns
// 	true,  if Key was already Present
// 	false, if Key was not Present
func (ods *OnDiskSet) InsertElem(key string) (bool, error) {
	present := false
	err := ods.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		count := uint64(0)
		n := 0

		// First check if the key is present
		countByteArray := bucket.Get([]byte(key))
		if countByteArray != nil {
			present = true
			count, n = binary.Uvarint(countByteArray)
			if n == 0 {
				return fmt.Errorf("Too small buffer to store count")
			} else if n < 0 {
				return fmt.Errorf("value larger than 64 bits, " +
					"overflow")
			} else {
				count++
			}
		} else {
			count = 1
		}

		countByteArray = make([]byte, binary.MaxVarintLen64)
		n = binary.PutUvarint(countByteArray, count)
		if n < 1 {
			return fmt.Errorf("less than 1 byte written to buffer for " +
				"count")
		}
		err := bucket.Put([]byte(key), countByteArray)
		return err
	})
	return present, err
}

// DeleteElem deletes the given element from the Set
func (ods *OnDiskSet) DeleteElem(key string) error {
	return ods.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		err := bucket.Delete([]byte(key))
		return err
	})
}

// CountElem returns the currentCount of the given element in the map
func (ods *OnDiskSet) CountElem(key string) (uint64, error) {
	count := uint64(0)
	n := 0
	err := ods.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(bucketName)
		countByteArray := bucket.Get([]byte(key))
		if countByteArray != nil {
			count, n = binary.Uvarint(countByteArray)
			if n == 0 {
				return fmt.Errorf("Too small bugger to store " +
					"countByteArray")
			} else if n < 0 {
				return fmt.Errorf("value larger than 64 bits, " +
					"overflow")
			} else {
				return nil
			}
		}
		return nil

	})
	return count, err
}

// TotalUniqueElems returns the total number of elements in the set.
func (ods *OnDiskSet) TotalUniqueElems() (int, error) {
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
