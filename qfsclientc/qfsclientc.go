// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclientc

/*
#cgo LDFLAGS: -lqfsclient -ljansson -lcrypto
#cgo CXXFLAGS: -std=c++11

#include <stdint.h>
#include <stddef.h>

const char * cGetApi(uint32_t *apiHandleOut);
const char * cGetApiPath(const char *path, uint32_t *apiHandleOut);
const char * cReleaseApi(uint32_t apiHandle);
const char * cGetAccessed(uint32_t apiHandle, const char * workspaceRoot);
const char * cInsertInode(uint32_t apiHandle, const char *dest, const char *key,
	uint32_t permissions, uint32_t uid, uint32_t gid);
const char * cBranch(uint32_t apiHandle, const char *source, const char *dest);
const char * cSetBlock(uint32_t apiHandle, const char *key, uint8_t *data,
	uint32_t len);
const char * cGetBlock(uint32_t apiHandle, const char *key, char *dataOut,
	uint32_t *lenOut);

*/
import "C"

import "errors"
import "unsafe"

import "github.com/aristanetworks/quantumfs"

type QfsClientApi struct {
	handle uint32
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
	err := C.cReleaseApi(handle)
	errStr := C.GoString(err)

	if errStr != "" {
		return errors.New(errStr)
	}

	return nil
}

func (api *QfsClientApi) GetAccessed(workspaceRoot string) error {
	errStr := C.GoString(C.cGetAccessed(C.uint32_t(api.handle),
		C.CString(workspaceRoot)))

	if errStr != "" {
		return errors.New(errStr)
	}

	return nil
}

func (api *QfsClientApi) InsertInode(dest string, key string, permissions uint32,
	uid uint32, gid uint32) error {

	errStr := C.GoString(C.cInsertInode(C.uint32_t(api.handle), C.CString(dest),
		C.CString(key), C.uint32_t(permissions), C.uint32_t(uid),
		C.uint32_t(gid)))

	if errStr != "" {
		return errors.New(errStr)
	}

	return nil
}

func (api *QfsClientApi) Branch(source string, dest string) error {
	errStr := C.GoString(C.cBranch(C.uint32_t(api.handle), C.CString(source),
		C.CString(dest)))

	if errStr != "" {
		return errors.New(errStr)
	}

	return nil
}

func (api *QfsClientApi) SetBlock(key string, data []byte) error {
	errStr := C.GoString(C.cSetBlock(C.uint32_t(api.handle), C.CString(key),
		(*C.uint8_t)(unsafe.Pointer(&data)),
		C.uint32_t(len(data))))

	if errStr != "" {
		return errors.New(errStr)
	}

	return nil
}

func (api *QfsClientApi) GetBlock(key string) ([]byte, error) {
	data := make([]byte, quantumfs.MaxBlockSize)
	var dataLen uint32

	errStr := C.GoString(C.cGetBlock(C.uint32_t(api.handle), C.CString(key),
		(*C.char)(unsafe.Pointer(&data)),
		(*C.uint32_t)(unsafe.Pointer(&dataLen))))

	if errStr != "" {
		return nil, errors.New(errStr)
	}

	return data[:dataLen], nil
}
