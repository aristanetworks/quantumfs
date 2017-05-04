// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qfsclient

/*
#cgo LDFLAGS: -lqfsclient -ljansson -lcrypto
#cgo CXXFLAGS: -std=c++11

#include <stdint.h>
#include <stddef.h>

const char * cGetApi(uint32_t *apiHandleOut);

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
