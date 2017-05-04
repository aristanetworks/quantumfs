// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "../QFSClient/qfs_client.h"

#include "map"
#include <stdint.h>

static std::map<uint32_t, qfsclient::Api*> s_apiHandles;
static uint32_t s_freeHandle = 0;

extern "C" {

	const char * cGetApi(uint32_t *apiHandleOut) {
		qfsclient::Api * api = NULL;
		qfsclient::Error err = qfsclient::GetApi(&api);

		if (err.code == qfsclient::kSuccess) {
			uint32_t handle = s_freeHandle++;
			s_apiHandles[handle] = api;
		}

		return err.message.c_str();
	}

}
