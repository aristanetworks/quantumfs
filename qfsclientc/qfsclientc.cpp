// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

#include "../QFSClient/qfs_client.h"

#include <atomic>
#include <map>
#include <pthread.h>
#include <stdint.h>
#include <string.h>

extern "C" {
#include "_cgo_export.h"

	// We need a unique identifier to give out for each Api instance.
	// The map is from identifiers to Apis
	static pthread_mutex_t mapLock = PTHREAD_MUTEX_INITIALIZER;
	static std::map<uint32_t, qfsclient::Api*> s_apiHandles;
	static std::atomic<uint32_t> s_freeHandle;

	const char * errStr(qfsclient::Error err) {
		if (err.code != qfsclient::kSuccess) {
			return err.message.c_str();
		}

		return "";
	}

	const char * cGetApi(uint32_t *apiHandleOut) {
		qfsclient::Api * api = NULL;
		qfsclient::Error err = qfsclient::GetApi(&api);

		const char * rtn = errStr(err);
		if (strcmp(rtn, "") != 0) {
			return rtn;
		}

		uint32_t handle = s_freeHandle++;

		pthread_mutex_lock(&mapLock);
		s_apiHandles[handle] = api;
		pthread_mutex_unlock(&mapLock);

		*apiHandleOut = handle;

		return "";
	}

	const char * cGetApiPath(const char *path, uint32_t *apiHandleOut) {
		qfsclient::Api * api = NULL;
		qfsclient::Error err = qfsclient::GetApi(path, &api);

		const char * rtn = errStr(err);
		if (strcmp(rtn, "") != 0) {
			return rtn;
		}

		uint32_t handle = s_freeHandle++;

		pthread_mutex_lock(&mapLock);
		s_apiHandles[handle] = api;
		pthread_mutex_unlock(&mapLock);

		*apiHandleOut = handle;

		return "";
	}

	qfsclient::Api* findApi(uint32_t apiHandle) {
		pthread_mutex_lock(&mapLock);
		auto search = s_apiHandles.find(apiHandle);
		if (search == s_apiHandles.end()) {
			pthread_mutex_unlock(&mapLock);
			return NULL;
		}
		pthread_mutex_unlock(&mapLock);

		return search->second;
	}

	const char * cReleaseApi(uint32_t apiHandle) {
		auto api = findApi(apiHandle);
		if (!api) {
			return "Api doesn't exist.";
		}

		qfsclient::ReleaseApi(api);

		pthread_mutex_lock(&mapLock);
		auto search = s_apiHandles.find(apiHandle);
		if (search == s_apiHandles.end()) {
			pthread_mutex_unlock(&mapLock);
			return "Api doesn't exist.";
		}
		s_apiHandles.erase(search);
		pthread_mutex_unlock(&mapLock);

		return "";
	}

	const char * cGetAccessed(uint32_t apiHandle, const char * workspaceRoot,
		uint64_t pathId) {

		auto api = findApi(apiHandle);
		if (!api) {
			return "Api doesn't exist.";
		}

		qfsclient::PathsAccessed accessed_list;

		qfsclient::Error err = api->GetAccessed(workspaceRoot, &accessed_list);
		if (err.code != 0) {
			return errStr(err);
		}

		for (auto i = accessed_list.paths.begin();
		    i != accessed_list.paths.end();
		    ++i) {
			setPath(pathId, const_cast<char *>(i->first.c_str()),
				i->second);
		}

		return "";
	}

	const char * cInsertInode(uint32_t apiHandle, const char *dest,
		const char *key, uint32_t permissions, uint32_t uid, uint32_t gid) {

		auto api = findApi(apiHandle);
		if (!api) {
			return "Api doesn't exist.";
		}

		qfsclient::Error err = api->InsertInode(dest, key,
			permissions, uid, gid);
		return errStr(err);
	}

	const char * cBranch(uint32_t apiHandle, const char *source,
		const char *dest) {

		auto api = findApi(apiHandle);
		if (!api) {
			return "Api doesn't exist.";
		}

		qfsclient::Error err = api->Branch(source, dest);
		return errStr(err);
	}

	const char * cDelete(uint32_t apiHandle, const char *workspace) {

		auto api = findApi(apiHandle);
		if (!api) {
			return "Api doesn't exist.";
		}

		qfsclient::Error err = api->Delete(workspace);
		return errStr(err);
	}

	const char * cSetBlock(uint32_t apiHandle, const char *key, uint8_t *data,
		uint32_t len) {

		auto api = findApi(apiHandle);
		if (!api) {
			return "Api doesn't exist.";
		}

		uint8_t *convertedKey = (uint8_t*)key;
		std::vector<uint8_t> inputKey(convertedKey,
			convertedKey + strlen(key));

		std::vector<uint8_t> inputData(data, data + len);

		qfsclient::Error err = api->SetBlock(inputKey, inputData);

		return errStr(err);
	}

	const char * cGetBlock(uint32_t apiHandle, const char *key, char *dataOut,
		uint32_t *lenOut) {

		auto api = findApi(apiHandle);
		if (!api) {
			return "Api doesn't exist.";
		}

		uint8_t *convertedKey = (uint8_t*)key;
		std::vector<uint8_t> inputKey(convertedKey,
			convertedKey + strlen(key));

		std::vector<uint8_t> data;
		qfsclient::Error err = api->GetBlock(inputKey, &data);

		const char *rtn = errStr(err);
		if (strcmp(rtn, "") != 0) {
			return rtn;
		}

		memcpy(dataOut, &data[0], data.size());
		*lenOut = data.size();

		return "";
	}
}
