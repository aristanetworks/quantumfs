// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "../QFSClient/qfs_client.h"

#include "map"
#include <stdint.h>

extern "C" {
	static std::map<uint32_t, qfsclient::Api*> s_apiHandles;
	static uint32_t s_freeHandle = 0;

	const char * errStr(qfsclient::Error err) {
		if (err.code ==  qfsclient::kSuccess) {
			return "";
		}

		return err.message.c_str();
	}

	const char * cGetApi(uint32_t *apiHandleOut) {
		qfsclient::Api * api = NULL;
		qfsclient::Error err = qfsclient::GetApi(&api);

		const char * rtn = errStr(err);
		if (rtn == "") {
			uint32_t handle = s_freeHandle++;
			s_apiHandles[handle] = api;
			*apiHandleOut = handle;
		}
		return rtn;
	}

	const char * cGetApiPath(const char *path, uint32_t *apiHandleOut) {
		qfsclient::Api * api = NULL;
		qfsclient::Error err = qfsclient::GetApi(path, &api);

		const char * rtn = errStr(err);
		if (rtn == "") {
			uint32_t handle = s_freeHandle++;
			s_apiHandles[handle] = api;
			*apiHandleOut = handle;
		}
		return rtn;
	}

	const char * cReleaseApi(uint32_t apiHandle) {
		auto search = s_apiHandles.find(apiHandle);
		if (search == s_apiHandles.end()) {
			return "Api doesn't exist.";
		}

		qfsclient::ReleaseApi(search->second);
		s_apiHandles.erase(search);
		return "";
	}
}
