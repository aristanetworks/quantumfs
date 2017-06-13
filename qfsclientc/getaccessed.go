// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

/*
#cgo LDFLAGS: -lqfsclient -ljansson -lcrypto
#cgo CXXFLAGS: -std=c++11

#include <stdint.h>

const char * cGetAccessed(uint32_t apiHandle, const char * workspaceRoot,
	void *paths);
*/
import "C"

import (
	"errors"
	"unsafe"

	"github.com/aristanetworks/quantumfs"
)

// A trampoline to allow the C++ code to set path values instead of writing a C
// wrapper for std::unordered_map.
//
//export setPath
func setPath(paths unsafe.Pointer, path *C.char, value uint) {
	pathList := (*quantumfs.PathAccessList)(paths)
	pathList.Paths[C.GoString(path)] = quantumfs.PathFlags(value)
}

func (api *QfsClientApi) GetAccessed(
	workspace string) (quantumfs.PathAccessList, error) {

	paths := quantumfs.NewPathAccessList()

	err := C.GoString(C.cGetAccessed(C.uint32_t(api.handle),
		C.CString(workspace), unsafe.Pointer(&paths)))
	if err != "" {
		return quantumfs.NewPathAccessList(), errors.New(err)
	}

	return paths, nil
}
