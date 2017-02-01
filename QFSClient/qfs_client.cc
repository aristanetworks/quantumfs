// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "qfs_client.h"

#include <ios>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

#include "qfs_client_util.h"

namespace qfsclient {

std::string getErrorMessage(ErrorCode code, const std::string &details) {
	switch (code) {
	case kSuccess:
		return "success";
	case kDontKnowCwd:
		return "couldn't determine current working directory";
	case kCantFindApiFile:
		return "couldn't find API file (started looking at " +
			details + ")";
	case kCantOpenApiFile:
		return "couldn't open API file " + details;
	case kApiFileNotOpen:
		return "call Api.Open() before calling API functions";
	case kApiFileSeekFail:
		return "couldn't seek within API file " + details;
	case kApiFileWriteFail:
		return "couldn't write to API file " + details;
	case kApiFileFlushFail:
		return "couldn't flush API file " + details;
	case kApiFileReadFail:
		return "couldn't read from API file " + details;
	}

	std::string result("unknown error (");
	result += code;
	result += ")";

	return result;
}

Error getError(ErrorCode code, const std::string &details = "") {
	return Error { code, getErrorMessage(code, details) };
}

Api::Api()
	: path(""),
	  api_inode_id(kInodeIdApi) {
}

Api::Api(const char *path)
	: path(path),
	  api_inode_id(kInodeIdApi) {
}

Api::~Api() {
	Close();
}

Error Api::Open() {
	if (this->path.length() == 0) {
		// Path was not passed to constructor: determine path
		Error err = this->DeterminePath();
		if (err.code != kSuccess) {
			return err;
		}
	}

	if (!this->file.is_open()) {
		this->file.open(this->path.c_str(),
				std::ios::in | std::ios::out | std::ios::binary);

		if (this->file.fail()) {
			return getError(kCantOpenApiFile, this->path);
		}
	}

	return getError(kSuccess);
}

void Api::Close() {
	if (this->file.is_open()) {
		this->file.close();
	}
}

Error Api::GetAccessed(const char *workspace_root) {
	// TODO: implement
}

Error Api::SendCommand(const CommandBuffer &command, CommandBuffer *result) {
	Error err = this->Open();
	if (err.code != kSuccess) {
		return err;
	}

	err = this->WriteCommand(command);
	if (err.code != kSuccess) {
		return err;
	}

	return this->ReadResponse(result);
}

Error Api::WriteCommand(const CommandBuffer &command) {
	if (!this->file.is_open()) {
		return getError(kApiFileNotOpen);
	}

	this->file.seekp(0);
	if (this->file.fail()) {
		return getError(kApiFileSeekFail, this->path);
	}

	this->file.write((const char *)command.data, command.size);
	if (this->file.fail()) {
		return getError(kApiFileWriteFail, this->path);
	}

	this->file.flush();
	if (this->file.fail()) {
		return getError(kApiFileFlushFail, this->path);
	}

	return getError(kSuccess);
}

Error Api::ReadResponse(CommandBuffer *command) {
	if (!this->file.is_open()) {
		return getError(kApiFileNotOpen);
	}

	this->file.seekg(0);
	if (this->file.fail()) {
		return getError(kApiFileSeekFail);
	}

	this->file.read((char *)command->data, sizeof(command->data));

	if (this->file.fail() && !(this->file.rdstate() & std::ios_base::eofbit)) {
		// any read failure *except* an EOF is a failure
		return getError(kApiFileReadFail, this->path);
	}

	command->size = this->file.gcount();

	return getError(kSuccess);
}

Error Api::DeterminePath() {
	// We use get_current_dir_name() here rather than getcwd() because it
	// allocates a buffer of the correct size for us (although we do have to
	// remember to free() that buffer). PATH_MAX isn't known at compile time
	// (and it is possible for paths to be longer than PATH_MAX anyway)
	char *cwd = get_current_dir_name();
	if (!cwd) {
		return getError(kDontKnowCwd);
	}

	std::vector<std::string> directories;
	std::string path;
	std::string currentDir(cwd);

	free(cwd);

	util::Split(currentDir, "/", &directories);

	while (true) {
		util::Join(directories, "/", &path);
		path = "/" + path + "/" + kApiPath;

		struct stat path_status;

		// lstat API_PATH at path
		if (lstat(path.c_str(), &path_status) == 0) {
			if (((S_ISREG(path_status.st_mode)) ||
			     (S_ISLNK(path_status.st_mode))) &&
			    (path_status.st_ino == this->api_inode_id)) {
				// we found an API *file* with the correct
				// inode ID: success
				this->path = path;
				return getError(kSuccess, this->path);
			}

			// Note: it's valid to have a file *or* directory called
			// 'api' that isn't the actual api file: in that case we
			// should just keep walking up the tree towards the root
		}

		if (directories.size() == 0) {
			// We got to / without finding the api file: fail
			return getError(kCantFindApiFile, currentDir);
		}

		// Remove last entry from directories and continue moving up
		// the directory tree by one level
		directories.pop_back();
	}

	return getError(kCantFindApiFile, currentDir);
}

} // namespace qfsclient

