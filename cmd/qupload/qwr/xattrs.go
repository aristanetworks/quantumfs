// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qwr

import "fmt"
import "strings"
import "sync/atomic"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/utils"

func WriteXAttrs(path string,
	ds quantumfs.DataStore) (quantumfs.ObjectKey, error) {

	sizeofXAttrs, err, _ := utils.LListXattr(path, 0)
	if err != nil {
		return quantumfs.EmptyBlockKey,
			fmt.Errorf("Write xattrs (list size) for "+
				"%q failed: %v", path, err)
	}

	if sizeofXAttrs == 0 {
		return quantumfs.EmptyBlockKey, nil
	}

	var xattrs []byte
	_, err, xattrs = utils.LListXattr(path, sizeofXAttrs)
	if err != nil {
		return quantumfs.EmptyBlockKey,
			fmt.Errorf("Write xattrs (list read) for "+
				"%q failed: %v", path, err)
	}

	// attribute names are separated by null byte
	xattrNames := strings.Split(strings.Trim(string(xattrs),
		"\000"), "\000")

	if len(xattrNames) > quantumfs.MaxNumExtendedAttributes() {
		return quantumfs.EmptyBlockKey,
			fmt.Errorf("Write xattrs failed. "+
				"Max number of xattrs supported is "+
				"%d, found %d on path %s\n",
				quantumfs.MaxNumExtendedAttributes(),
				len(xattrNames), path)
	}

	xattrMetadata := quantumfs.NewExtendedAttributes()
	for i, xattrName := range xattrNames {
		xattrSz, err, _ := utils.LGetXattr(path, xattrName, 0)
		if err != nil {
			return quantumfs.EmptyBlockKey,
				fmt.Errorf("Write xattrs (attr size) %q "+
					"for %q failed: %v", xattrName,
					path, err)
		}
		var xattrData []byte
		_, err, xattrData = utils.LGetXattr(path, xattrName, xattrSz)
		if err != nil {
			return quantumfs.EmptyBlockKey,
				fmt.Errorf("Write xattrs (attr read) %q "+
					"for %q failed: %v", xattrName,
					path, err)
		}

		dataKey, bErr := writeBlock(xattrData, quantumfs.KeyTypeData, ds)
		if bErr != nil {
			return quantumfs.EmptyBlockKey,
				fmt.Errorf("Write xattrs (block write) %q "+
					"for %q failed: %v", xattrName,
					path, err)
		}
		atomic.AddUint64(&MetadataBytesWritten, uint64(len(xattrData)))

		xattrMetadata.SetAttribute(i, xattrName, dataKey)
		xattrMetadata.SetNumAttributes(i + 1)
	}

	xKey, xerr := writeBlock(xattrMetadata.Bytes(),
		quantumfs.KeyTypeMetadata, ds)
	if xerr != nil {
		return quantumfs.EmptyBlockKey,
			fmt.Errorf("Write xattrs (metadata write) for "+
				"%q failed: %v", path, xerr)
	}
	atomic.AddUint64(&MetadataBytesWritten,
		uint64(len(xattrMetadata.Bytes())))
	return xKey, nil
}
