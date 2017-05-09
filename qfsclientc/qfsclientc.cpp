// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "../QFSClient/qfs_client.h"

#include <atomic>
#include <map>
#include <stdint.h>
#include <string.h>

extern "C" {
	// We need a unique identifier to give out for each Api instance.
	// The map is from identifiers to Apis
	static std::map<uint32_t, qfsclient::Api*> s_apiHandles;
	static std::atomic<uint32_t> s_freeHandle;

	const char * errStr(qfsclient::Error err) {
		if (err.code !=  qfsclient::kSuccess) {
			return err.message.c_str();
		}

		return "";
	}

	const char * cGetApi(uint32_t *apiHandleOut) {
		qfsclient::Api * api = NULL;
		qfsclient::Error err = qfsclient::GetApi(&api);

		const char * rtn = errStr(err);
		if (rtn != "") {
			return rtn;
		}

		uint32_t handle = s_freeHandle++;
		s_apiHandles[handle] = api;
		*apiHandleOut = handle;

		return "";
	}

	const char * cGetApiPath(const char *path, uint32_t *apiHandleOut) {
		qfsclient::Api * api = NULL;
		qfsclient::Error err = qfsclient::GetApi(path, &api);

		const char * rtn = errStr(err);
		if (rtn != "") {
			return rtn;
		}

		uint32_t handle = s_freeHandle++;
		s_apiHandles[handle] = api;
		*apiHandleOut = handle;

		return "";
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

	const char * cInsertInode(uint32_t apiHandle, const char *dest,
		const char *key, uint32_t permissions, uint32_t uid, uint32_t gid) {

		auto search = s_apiHandles.find(apiHandle);
		if (search == s_apiHandles.end()) {
			return "Api doesn't exist.";
		}

		qfsclient::Error err = search->second->InsertInode(dest, key,
			permissions, uid, gid);
		return errStr(err);
	}

	const char * cBranch(uint32_t apiHandle, const char *source,
		const char *dest) {

		auto search = s_apiHandles.find(apiHandle);
		if (search == s_apiHandles.end()) {
			return "Api doesn't exist.";
		}

		qfsclient::Error err = search->second->Branch(source, dest);
		return errStr(err);
	}

	const char * cSetBlock(uint32_t apiHandle, const char *key, uint8_t *data,
		uint32_t len) {

		auto search = s_apiHandles.find(apiHandle);
		if (search == s_apiHandles.end()) {
			return "Api doesn't exist.";
		}

		uint8_t *convertedKey = (uint8_t*)key;
		std::vector<uint8_t> inputKey(convertedKey,
			convertedKey + strlen(key));

		std::vector<uint8_t> inputData(data, data + len);

		qfsclient::Error err = search->second->SetBlock(inputKey, inputData);

		return errStr(err);
	}

	const char * cGetBlock(uint32_t apiHandle, const char *key, char *dataOut,
		uint32_t *lenOut) {

		auto search = s_apiHandles.find(apiHandle);
		if (search == s_apiHandles.end()) {
			return "Api doesn't exist.";
		}

		uint8_t *convertedKey = (uint8_t*)key;
		std::vector<uint8_t> inputKey(convertedKey,
			convertedKey + strlen(key));

		std::vector<uint8_t> data;
		qfsclient::Error err = search->second->GetBlock(inputKey, &data);

		const char *rtn = errStr(err);
		if (rtn != "") {
			return rtn;
		}

		memcpy(dataOut, &data[0], data.size());
		*lenOut = data.size();

		return "";
	}
}
