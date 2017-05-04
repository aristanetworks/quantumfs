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

*/
import "C"

import "errors"
import "unsafe"

type QfsClientApi struct {
	handle	uint32
}

func GetApi() (api QfsClientApi, err error) {
	var apiHandle uint32
	errStr := C.GoString(C.cGetApi((*C.uint32_t)(unsafe.Pointer(&apiHandle))))
	if errStr != "" {
		return QfsClientApi{}, errors.New(errStr)
	}

	return QfsClientApi {
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

	return QfsClientApi {
		handle: apiHandle,
	}, nil
}

func ReleaseApi(api QfsClientApi) error {
	handle := C.uint32_t(api.handle)
	err := C.cReleaseApi(handle);
	errStr := C.GoString(err)

	if errStr != "" {
		return errors.New(errStr)
	}

	return nil
}
