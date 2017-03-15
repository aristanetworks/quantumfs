// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "strings"
import "syscall"

import "github.com/aristanetworks/quantumfs"

func WriteXAttrs(path string,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	sizeofXAttrs, err := syscall.Listxattr(path, nil)
	if err != nil {
		return quantumfs.EmptyBlockKey, err
	}

	if sizeofXAttrs == 0 {
		return quantumfs.EmptyBlockKey, nil
	}

	xattrs := make([]byte, sizeofXAttrs)
	_, err = syscall.Listxattr(path, xattrs)
	if err != nil {
		return quantumfs.EmptyBlockKey, err
	}

	// attribute names are separated by null byte
	xattrNames := strings.Split(strings.Trim(string(xattrs), "\000"), "\000")

	if len(xattrNames) > quantumfs.MaxNumExtendedAttributes() {
		return quantumfs.EmptyBlockKey,
			fmt.Errorf("Max number of xattrs supported is %d, found %d on"+
				" path %s\n", quantumfs.MaxNumExtendedAttributes(), len(xattrNames),
				path)
	}

	xattrMetadata := quantumfs.NewExtendedAttributes()
	for i, xattrName := range xattrNames {
		xattrSz, err := syscall.Getxattr(path, xattrName, nil)
		if err != nil {
			return quantumfs.EmptyBlockKey, err
		}
		xattrData := make([]byte, xattrSz)
		_, err = syscall.Getxattr(path, xattrName, xattrData)
		if err != nil {
			return quantumfs.EmptyBlockKey, err
		}

		dataKey, bErr := writeBlob(xattrData, quantumfs.KeyTypeData, ds)
		if bErr != nil {
			return quantumfs.EmptyBlockKey, bErr
		}

		xattrMetadata.SetAttribute(i, xattrName, dataKey)
		xattrMetadata.SetNumAttributes(i + 1)
	}

	return writeBlob(xattrMetadata.Bytes(),
		quantumfs.KeyTypeMetadata, ds)
}
