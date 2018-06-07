// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

/*
#cgo LDFLAGS: -ljansson -lcrypto -lqfsclient -lqfs
#cgo CXXFLAGS: -std=c++11

#include <stdint.h>
#include <stddef.h>

const char * cGetApi(uint32_t *apiHandleOut);
const char * cGetApiPath(const char *path, uint32_t *apiHandleOut);
const char * cReleaseApi(uint32_t apiHandle);
const char * cGetAccessed(uint32_t apiHandle, const char * workspaceRoot,
	uint64_t pathId);
const char * cInsertInode(uint32_t apiHandle, const char *dest, const char *key,
	uint32_t permissions, uint32_t uid, uint32_t gid);
const char * cBranch(uint32_t apiHandle, const char *source, const char *dest);
const char * cDelete(uint32_t apiHandle, const char *workspace);
const char * cSetBlock(uint32_t apiHandle, const char *key, uint8_t *data,
	uint32_t len);
const char * cGetBlock(uint32_t apiHandle, const char *key, char *dataOut,
	uint32_t *lenOut);

*/
import "C"

import (
	"errors"
	"unsafe"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/utils"
)

type QfsClientApi struct {
	handle uint32
}

func checkError(errStr string) error {
	if errStr == "" {
		return nil
	}

	return errors.New(errStr)
}

func GetApi() (api QfsClientApi, err error) {
	var apiHandle uint32
	errStr := C.GoString(C.cGetApi((*C.uint32_t)(unsafe.Pointer(&apiHandle))))
	if errStr != "" {
		return QfsClientApi{}, errors.New(errStr)
	}

	return QfsClientApi{
		handle: apiHandle,
	}, nil
}

func GetApiPath(path string) (api QfsClientApi, err error) {
	var apiHandle uint32
	errStr := C.GoString(C.cGetApiPath(C.CString(path),
		(*C.uint32_t)(unsafe.Pointer(&apiHandle))))
	if errStr != "" {
		return QfsClientApi{}, errors.New(errStr)
	}

	return QfsClientApi{
		handle: apiHandle,
	}, nil
}

func ReleaseApi(api QfsClientApi) error {
	handle := C.uint32_t(api.handle)
	err := C.GoString(C.cReleaseApi(handle))

	return checkError(err)
}

// A trampoline to allow the C++ code to set path values instead of writing a C
// wrapper for std::unordered_map.
//
//export setPath
func setPath(pathId uint64, path *C.char, value uint32) {
	defer tmpListLock.Lock().Unlock()
	pathList := tmpListMap[pathId]
	pathList.Paths[C.GoString(path)] = quantumfs.PathFlags(value)
}

var tmpListLock utils.DeferableMutex
var tmpListMap map[uint64]*quantumfs.PathsAccessed

func init() {
	tmpListMap = map[uint64]*quantumfs.PathsAccessed{}
}

func (api *QfsClientApi) GetAccessed(
	workspace string) (quantumfs.PathsAccessed, error) {

	paths := quantumfs.NewPathsAccessed()
	pathId := uint64(uintptr(unsafe.Pointer(&paths)))

	func() {
		defer tmpListLock.Lock().Unlock()
		tmpListMap[pathId] = &paths
	}()

	err := C.GoString(C.cGetAccessed(C.uint32_t(api.handle),
		C.CString(workspace), C.uint64_t(pathId)))

	func() {
		defer tmpListLock.Lock().Unlock()
		delete(tmpListMap, pathId)
	}()
	if err != "" {
		return quantumfs.NewPathsAccessed(), errors.New(err)
	}

	return paths, nil
}

func (api *QfsClientApi) InsertInode(dest string, key string, permissions uint32,
	uid uint32, gid uint32) error {

	err := C.GoString(C.cInsertInode(C.uint32_t(api.handle), C.CString(dest),
		C.CString(key), C.uint32_t(permissions), C.uint32_t(uid),
		C.uint32_t(gid)))

	return checkError(err)
}

func (api *QfsClientApi) Branch(source string, dest string) error {
	err := C.GoString(C.cBranch(C.uint32_t(api.handle), C.CString(source),
		C.CString(dest)))

	return checkError(err)
}

func (api *QfsClientApi) Delete(workspace string) error {
	err := C.GoString(C.cDelete(C.uint32_t(api.handle), C.CString(workspace)))

	return checkError(err)
}

func (api *QfsClientApi) SetBlock(key string, data []byte) error {
	err := C.GoString(C.cSetBlock(C.uint32_t(api.handle), C.CString(key),
		(*C.uint8_t)(unsafe.Pointer(&data)),
		C.uint32_t(len(data))))

	return checkError(err)
}

func (api *QfsClientApi) GetBlock(key string) ([]byte, error) {
	data := make([]byte, quantumfs.MaxBlockSize)
	var dataLen uint32

	err := C.GoString(C.cGetBlock(C.uint32_t(api.handle), C.CString(key),
		(*C.char)(unsafe.Pointer(&data)),
		(*C.uint32_t)(unsafe.Pointer(&dataLen))))

	if err != "" {
		return nil, errors.New(err)
	}

	return data[:dataLen], nil
}
