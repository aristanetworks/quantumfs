// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package filesystem

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"

	ether "github.com/aristanetworks/quantumfs/backends/cql"
)

func openFile(fileName string) (*os.File, int64, error) {

	f, err := os.Open(fileName)
	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

func getFile(file *os.File, fileSize int64) ([]byte, error) {

	b := make([]byte, fileSize)
	n, err := file.Read(b)
	if err != nil {
		return nil, err
	}

	if int64(n) != fileSize {
		return nil, errors.New("partial read of file")
	}
	return b, nil
}

func retrieveMetadata(filePath string) (map[string]interface{}, map[string]string, error) {

	file, size, err := openFile(filePath + ".mdata")
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	mdata, err := getFile(file, size)
	if err != nil {
		return nil, nil, err
	}
	blobstoreMetadata, metadata, err := unmarshallMetadata(mdata)
	if err != nil {
		return nil, nil, err
	}

	return blobstoreMetadata, metadata, nil
}

func unmarshallMetadata(data []byte) (map[string]interface{}, map[string]string, error) {

	var allMetadata AllMetadata
	err := json.Unmarshal(data, &allMetadata)
	if err != nil {
		return nil, nil, err
	}
	return allMetadata.BlobStoreMetadata, allMetadata.Metadata, nil
}

// Get will first retrieve the metadata file and then the data file. Since we expect
// the key to include the content hash of the value we will not perform any strong
// checks to verify that the data and metadata are from an atomic Insert. The only
// check we do is to verify if the size of the data file and the size stored in the
// blobstore metadata match. If they do not we will return ErrOperationFailed as it is
// likely a race with a delete operaration. The client can retry the operation.
func (b *fileStore) Get(c ether.Ctx, key []byte) ([]byte, map[string]string, error) {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn("fs::Get", "key: %s", keyHex).Out()

	_, filePath := getDirAndFilePath(b, key)

	// Read metadata
	blobstoreMetadata, metadata, err := retrieveMetadata(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ether.NewError(ether.ErrKeyNotFound, "key %s not found in Get", keyHex)
		}
		return nil, nil, ether.NewError(ether.ErrOperationFailed, "error in retrieving metadata in Get %s", err.Error())
	}

	dataSize := int64(blobstoreMetadata["size"].(float64))
	value := []byte(nil)
	if dataSize != 0 {
		df, ds, err := openFile(filePath + ".data")
		if err != nil {
			return nil, nil, ether.NewError(ether.ErrOperationFailed, "error in opening data file in Get %s", err.Error())
		}
		defer df.Close()

		if ds != dataSize {
			return nil, nil, ether.NewError(ether.ErrOperationFailed, "incorrect size read in Get")
		}
		value, err = getFile(df, dataSize)
		if err != nil {
			return nil, nil, ether.NewError(ether.ErrOperationFailed, "error in reading data file in Get %s", err.Error())
		}
	}
	return value, metadata, nil
}

func (b *fileStore) Metadata(c ether.Ctx, key []byte) (map[string]string, error) {
	keyHex := hex.EncodeToString(key)
	defer c.FuncIn("fs::Metadata", "key: %s", keyHex).Out()

	_, filePath := getDirAndFilePath(b, key)
	_, metadata, err := retrieveMetadata(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ether.NewError(ether.ErrKeyNotFound, "key %s not found during Metadata", keyHex)
		}
		return nil, ether.NewError(ether.ErrOperationFailed, "error in retrieving  metadata file in Metadata %s", err.Error())
	}

	return metadata, nil
}
