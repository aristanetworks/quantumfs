// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "QFSClient/qfs_client_implementation.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <jansson.h>

#include <algorithm>
#include <ios>
#include <vector>

#include "./libqfs.h"
#include "QFSClient/qfs_client.h"
#include "QFSClient/qfs_client_data.h"
#include "QFSClient/qfs_client_test.h"
#include "QFSClient/qfs_client_util.h"

namespace qfsclient {

ApiContext::ApiContext() : request_json_object(NULL), response_json_object(NULL) {
}

ApiContext::~ApiContext() {
	SetRequestJsonObject(NULL);
	SetResponseJsonObject(NULL);
}

void ApiContext::SetRequestJsonObject(json_t *request_json_object) {
	if (this->request_json_object) {
		json_decref(this->request_json_object);
		this->request_json_object = NULL;
	}

	this->request_json_object = request_json_object;
}

json_t *ApiContext::GetRequestJsonObject() const {
	return request_json_object;
}

void ApiContext::SetResponseJsonObject(json_t *response_json_object) {
	if (this->response_json_object) {
		json_decref(this->response_json_object);
		this->response_json_object = NULL;
	}

	this->response_json_object = response_json_object;
}

json_t *ApiContext::GetResponseJsonObject() const {
	return response_json_object;
}

CommandBuffer::CommandBuffer() {
}

CommandBuffer::~CommandBuffer() {
}

// Remove the trailing zeros from the tail of the response
void CommandBuffer::Sanitize() {
	auto size = this->data.size();

	for (auto it = this->data.crbegin();
			it != this->data.crend();
			++it) {
		if (*it) {
			break;
		}
		// *it is a trailing zero, discard it
		--size;
	}

	this->data.resize(size);
}

// Copy the contents of the given CommandBuffer into this one
void CommandBuffer::Copy(const CommandBuffer &source) {
	this->data = source.data;
}

// Return a const pointer to the data in the buffer
const byte *CommandBuffer::Data() const {
	return this->data.data();
}

// Return the size of the data stored in the buffer
size_t CommandBuffer::Size() const {
	return this->data.size();
}

// Reset the buffer such that it will contain no data and will
// have a zero size
void CommandBuffer::Reset() {
	this->data.clear();
}

// Append a block of data to the buffer. Returns an error if the
// buffer would have to be grown too large to add this block
ErrorCode CommandBuffer::Append(const byte *data, size_t size) {
	try {
		this->data.insert(this->data.end(), data, data + size);
	}
	catch (...) {
		return kBufferTooBig;
	}

	return kSuccess;
}

// copy a string into the buffer, but without a NUL terminator. An error will
// be returned if the buffer would have to be grown too large to fit the string.
ErrorCode CommandBuffer::CopyString(const char *s) {
	this->data.clear();

	return this->Append((const byte *)s, strlen(s));
}

Error GetApi(Api **api) {
	*api = new ApiImpl();
	return util::getError(kSuccess);
}

Error GetApi(const char *path, Api **api) {
	*api = new ApiImpl(path);
	return util::getError(kSuccess);
}

void ReleaseApi(Api *api) {
	if (api != NULL) {
		delete reinterpret_cast<ApiImpl*>(api);
	}
}

ApiImpl::ApiImpl()
	: fd(-1),
	  path(""),
	  api_inode_id(kInodeIdApi),
	  test_hook(NULL) {
}

ApiImpl::ApiImpl(const char *path)
	: fd(-1),
	  path(path),
	  api_inode_id(kInodeIdApi),
	  test_hook(NULL) {
}

ApiImpl::~ApiImpl() {
	Close();
}

Error ApiImpl::Open() {
	return this->OpenCommon(false);
}

Error ApiImpl::TestOpen() {
	return this->OpenCommon(true);
}

Error ApiImpl::OpenCommon(bool inTest) {
	if (this->path.length() == 0) {
		// Path was not passed to constructor: determine path
		Error err;
		if (inTest) {
			err = this->DeterminePathInTest();
		} else {
			err = this->DeterminePath();
		}
		if (err.code != kSuccess) {
			return err;
		}
	}

	if (this->fd == -1) {
		int flags = O_RDWR | O_CLOEXEC;
#if defined(O_DIRECT)
		if (!inTest) {
			flags |= O_DIRECT;
		}
#endif

		this->fd = open(this->path.c_str(), flags);
		if (this->fd == -1) {
			return util::getError(kCantOpenApiFile, this->path);
		}
	}

	return util::getError(kSuccess);
}

void ApiImpl::Close() {
	if (this->fd != -1) {
		int err = close(this->fd);
		if (err != 0) {
			printf("Error when closing api: %d\n", errno);
		}
		this->fd = -1;
	}
}

Error ApiImpl::SendCommand(const CommandBuffer &command, CommandBuffer *response) {
	Error err = this->Open();
	if (err.code != kSuccess) {
		return err;
	}

	err = this->WriteCommand(command);
	if (err.code != kSuccess) {
		return err;
	}

	if (this->test_hook) {
		Error err = this->test_hook->PostWriteHook();
		if (err.code != kSuccess) {
			return err;
		}

		err = this->test_hook->PreReadHook(response);
		if (err.code != kSuccess) {
			return err;
		}

		return util::getError(kSuccess);
	}

	return this->ReadResponse(response);
}

Error ApiImpl::WriteCommand(const CommandBuffer &command) {
	if (this->fd == -1) {
		return util::getError(kApiFileNotOpen);
	}

	int err = lseek(this->fd, 0, SEEK_SET);
	if (err == -1) {
		return util::getError(kApiFileSeekFail, this->path);
	}

	util::AlignedMem<512> data(command.Size());
	memcpy(*data, command.Data(), command.Size());

	// We must write the whole command at once
	int written = write(this->fd, *data, command.Size());

	if (written == -1 || written != command.Size()) {
		return util::getError(kApiFileWriteFail, this->path);
	}

	return util::getError(kSuccess);
}

Error ApiImpl::ReadResponse(CommandBuffer *command) {
	ErrorCode err = kSuccess;

	if (this->fd == -1) {
		return util::getError(kApiFileNotOpen);
	}

	int errCode = lseek(this->fd, 0, SEEK_SET);
	if (errCode == -1) {
		return util::getError(kApiFileSeekFail);
	}

	// read up to 4k at a time, stopping on EOF
	command->Reset();

	util::AlignedMem<512> data(4096);

	while(true) {
		int num = read(this->fd, *data, data.Size());
		if (num == 0) {
			break;
		}

		if (num < 0) {
			// any read failure *except* an EOF is a failure
			return util::getError(kApiFileReadFail, this->path);
		}

		if (command->Size() % 512) {
			return util::getError(kApiFileReadFail, "unaligned read");
		}

		err = command->Append(reinterpret_cast<const byte *>(*data), num);

		if (err != kSuccess) {
			return util::getError(err);
		}
	}

	command->Sanitize();
	return util::getError(err);
}

Error ApiImpl::DeterminePath() {
	FindApiPath_return apiPath = FindApiPath();
	if (strlen(apiPath.r1) != 0) {
		return util::getError(kCantFindApiFile, std::string(apiPath.r1));
	}

	this->path = std::string(apiPath.r0);
}

Error ApiImpl::DeterminePathInTest() {
	// getcwd() with a NULL first parameter results in a buffer of whatever size
	// is required being allocated, which we must then free. PATH_MAX isn't
	// known at compile time (and it is possible for paths to be longer than
	// PATH_MAX anyway)
	char *cwd = getcwd(NULL, 0);
	if (!cwd) {
		return util::getError(kDontKnowCwd);
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

		if (lstat(path.c_str(), &path_status) == 0) {
			if (((S_ISREG(path_status.st_mode)) ||
			     (S_ISLNK(path_status.st_mode))) &&
			    (path_status.st_ino == this->api_inode_id)) {
				// we found an API *file* with the correct
				// inode ID: success
				this->path = path;
				return util::getError(kSuccess, this->path);
			}

			// Note: it's valid to have a file *or* directory called
			// 'api' that isn't the actual api file: in that case we
			// should just keep walking up the tree towards the root
		}

		if (directories.size() == 0) {
			// We got to / without finding the api file: fail
			return util::getError(kCantFindApiFile, currentDir);
		}

		// Remove last entry from directories and continue moving up
		// the directory tree by one level
		directories.pop_back();
	}

	return util::getError(kCantFindApiFile, currentDir);
}

Error ApiImpl::CheckWorkspaceNameValid(const char *workspace_name) {
	std::string str(workspace_name);
	std::vector<std::string> tokens;

	util::Split(str, "/", &tokens);

	// name must have *exactly* two '/' characters (in which case it will have
	// three tokens if split by '/'
	if (tokens.size() != 3) {
		return util::getError(kWorkspaceNameInvalid, workspace_name);
	}

	return util::getError(kSuccess);
}

Error ApiImpl::CheckWorkspacePathValid(const char *workspace_path) {
	std::string str(workspace_path);
	std::vector<std::string> tokens;

	util::Split(str, "/", &tokens);

	// path must have *at least* two '/' characters (in which case it will have
	// three or more tokens if split by '/'
	if (tokens.size() < 3) {
		return util::getError(kWorkspacePathInvalid, workspace_path);
	}

	return util::getError(kSuccess);
}

Error ApiImpl::CheckCommonApiResponse(const CommandBuffer &response,
				      ApiContext *context) {
	json_error_t json_error;

	// parse JSON in response into a std::unordered_map<std::string, bool>
	json_t *response_json = json_loadb((const char *)response.Data(),
					   response.Size(),
					   0,
					   &json_error);

	context->SetResponseJsonObject(response_json);

	if (response_json == NULL) {
		std::string details = util::buildJsonErrorDetails(
			json_error.text,
			(const char *)response.Data(),
			response.Size());
		return util::getError(kJsonDecodingError, details);
	}

	json_t *response_json_obj;
	// note about cleaning up response_json_obj: this isn't necessary
	// as long as we use format character 'o' below, because in this case
	// the reference count of response_json isn't increased and we don't
	// leak any references. However, format character 'O' *will* increase the
	// reference count and in that case we would need to clean up afterwards.
	int success = json_unpack_ex(response_json, &json_error, 0,
				     "o", &response_json_obj);

	if (success != 0) {
		std::string details = util::buildJsonErrorDetails(
			json_error.text,
			(const char *)response.Data(),
			response.Size());
		return util::getError(kJsonDecodingError, details);
	}

	json_t *error_json_obj = json_object_get(response_json_obj, kErrorCode);
	if (error_json_obj == NULL) {
		std::string details = util::buildJsonErrorDetails(
			kErrorCode,
			(const char *)response.Data(),
			response.Size());
		return util::getError(kMissingJsonObject, details);
	}

	json_t *message_json_obj = json_object_get(response_json_obj, kMessage);
	if (message_json_obj == NULL) {
		std::string details = util::buildJsonErrorDetails(
			kMessage, (const char *)response.Data(), response.Size());
		return util::getError(kMissingJsonObject, details);
	}

	if (!json_is_integer(error_json_obj)) {
		std::string details = util::buildJsonErrorDetails(
			"error code in response JSON is not valid",
			(const char *)response.Data(),
			response.Size());
		return util::getError(kJsonDecodingError,
				      details);
	}

	CommandError apiError = (CommandError)json_integer_value(error_json_obj);
	if (apiError != kCmdOk) {
		std::string api_error = util::getApiError(
			apiError,
			json_string_value(message_json_obj));

		std::string details = util::buildJsonErrorDetails(
			api_error, (const char *)response.Data(), response.Size());

		return util::getError(kApiError, details);
	}

	return util::getError(kSuccess);
}

Error ApiImpl::SendJson(ApiContext *context) {
	json_t *request_json = context->GetRequestJsonObject();
	// we pass these flags to json_dumps() because:
	//    JSON_COMPACT: there's no good reason for verbose JSON
	//    JSON_SORT_KEYS: so that the tests can get predictable JSON and will
	//                    be able to compare generated JSON reliably
	char *request_json_str = json_dumps(request_json,
					    JSON_COMPACT | JSON_SORT_KEYS);

	CommandBuffer command;

	command.CopyString(request_json_str);

	free(request_json_str);    // free the JSON string created by json_dumps()

	// send CommandBuffer and receive response in another one
	CommandBuffer response;
	Error err = this->SendCommand(command, &response);
	if (err.code != kSuccess) {
		 return err;
	}

	err = this->CheckCommonApiResponse(response, context);
	if (err.code != kSuccess) {
		 return err;
	}

	return util::getError(kSuccess);
}

Error ApiImpl::GetAccessed(const char *workspace_root, PathsAccessed *paths) {
	Error err = this->CheckWorkspaceNameValid(workspace_root);
	if (err.code != kSuccess) {
		return err;
	}

	// create JSON in a CommandBuffer with:
	//    CommandId = kGetAccessed and
	//    WorkspaceRoot = workspace_root
	json_error_t json_error;
	json_t *request_json = json_pack_ex(&json_error, 0,
					    kGetAccessedJSON,
					    kCommandId, kCmdGetAccessed,
					    kWorkspaceRoot, workspace_root);
	if (request_json == NULL) {
		return util::getError(kJsonEncodingError, json_error.text);
	}

	ApiContext context;
	context.SetRequestJsonObject(request_json);
	err = this->SendJson(&context);
	if (err.code != kSuccess) {
		return err;
	}

	err = this->PrepareAccessedListResponse(&context, paths);
	if (err.code != kSuccess) {
		return err;
	}

	return util::getError(kSuccess);
}

Error ApiImpl::InsertInode(const char *destination,
			   const char *key,
			   uint32_t permissions,
			   uint32_t uid,
			   uint32_t gid) {
	Error err = this->CheckWorkspacePathValid(destination);
	if (err.code != kSuccess) {
		return err;
	}

	// create JSON with:
	//    CommandId = kCmdInsertInode and
	//    DstPath = destination
	//    Key = key
	//    Uid = uid
	//    Gid = gid
	//    Permissions = permissions
	json_error_t json_error;
	json_t *request_json = json_pack_ex(&json_error, 0,
					    kInsertInodeJSON,
					    kCommandId, kCmdInsertInode,
					    kDstPath, destination,
					    kKey, key,
					    kUid, uid,
					    kGid, gid,
					    kPermissions, permissions);
	if (request_json == NULL) {
		return util::getError(kJsonEncodingError, json_error.text);
	}

	ApiContext context;
	context.SetRequestJsonObject(request_json);

	err = this->SendJson(&context);
	if (err.code != kSuccess) {
		return err;
	}

	return util::getError(kSuccess);
}

Error ApiImpl::Branch(const char *source, const char *destination) {
	Error err = this->CheckWorkspaceNameValid(source);
	if (err.code != kSuccess) {
		return err;
	}

	err = this->CheckWorkspaceNameValid(destination);
	if (err.code != kSuccess) {
		return err;
	}

	// create JSON with:
	//    CommandId = kCmdBranchRequest and
	//    Src = source
	//    Dst = destination
	json_error_t json_error;
	json_t *request_json = json_pack_ex(&json_error, 0,
					    kBranchJSON,
					    kCommandId, kCmdBranchRequest,
					    kSource, source,
					    kDestination, destination);
	if (request_json == NULL) {
		return util::getError(kJsonEncodingError, json_error.text);
	}

	ApiContext context;
	context.SetRequestJsonObject(request_json);

	err = this->SendJson(&context);
	if (err.code != kSuccess) {
		return err;
	}

	return util::getError(kSuccess);
}

Error ApiImpl::Delete(const char *workspace) {
	Error err = this->CheckWorkspaceNameValid(workspace);
	if (err.code != kSuccess) {
		return err;
	}

	// create JSON with:
	//    CommandId = kCmdDeleteWorkspace and
	//    Workspace = workspace
	json_error_t json_error;
	json_t *request_json = json_pack_ex(&json_error, 0,
					    kDeleteJSON,
					    kCommandId, kCmdDeleteWorkspace,
					    kWorkspacePath, workspace);
	if (request_json == NULL) {
		return util::getError(kJsonEncodingError, json_error.text);
	}

	ApiContext context;
	context.SetRequestJsonObject(request_json);

	err = this->SendJson(&context);
	if (err.code != kSuccess) {
		return err;
	}

	return util::getError(kSuccess);
}

Error ApiImpl::SetBlock(const std::vector<byte> &key,
			const std::vector<byte> &data) {
	// convert key and data to base64 before stuffing into JSON
	std::string base64_key;
	std::string base64_data;
	Error err;

	err = util::base64_encode(key, &base64_key);
	if (err.code != kSuccess) {
		return err;
	}
	err = util::base64_encode(data, &base64_data);
	if (err.code != kSuccess) {
		return err;
	}

	// create JSON with:
	//    CommandId = kCmdSetBlock and
	//    Key = key
	//    Data = data
	json_error_t json_error;
	json_t *request_json = json_pack_ex(&json_error, 0,
					    kSetBlockJSON,
					    kCommandId, kCmdSetBlock,
					    kKey, base64_key.c_str(),
					    kData, base64_data.c_str());
	if (request_json == NULL) {
		return util::getError(kJsonEncodingError, json_error.text);
	}

	ApiContext context;
	context.SetRequestJsonObject(request_json);

	err = this->SendJson(&context);
	if (err.code != kSuccess) {
		return err;
	}

	return util::getError(kSuccess);
}

Error ApiImpl::GetBlock(const std::vector<byte> &key, std::vector<byte> *data) {
	// convert key to base64 before stuffing into JSON
	std::string base64_key;
	Error err;

	err = util::base64_encode(key, &base64_key);

	if (err.code != kSuccess) {
		return err;
	}

	// create JSON with:
	//    CommandId = kCmdGetBlock and
	//    Key = key
	json_error_t json_error;
	json_t *request_json = json_pack_ex(&json_error, 0,
					    kGetBlockJSON,
					    kCommandId, kCmdGetBlock,
					    kKey, base64_key.c_str());
	if (request_json == NULL) {
		return util::getError(kJsonEncodingError, json_error.text);
	}

	ApiContext context;
	context.SetRequestJsonObject(request_json);

	err = this->SendJson(&context);
	if (err.code != kSuccess) {
		return err;
	}

	json_t *response_json = context.GetResponseJsonObject();

	json_t *data_json_obj = json_object_get(response_json, kData);
	if (data_json_obj == NULL) {
		return util::getError(kMissingJsonObject, kData);
	}
	if (!json_is_string(data_json_obj)) {
		return util::getError(kJsonObjectWrongType,
				      "expected string for " + std::string(kData));
	}

	const char *data_base64 = json_string_value(data_json_obj);

	// convert data_base64 from base64 to binary before setting value in data
	err = util::base64_decode(data_base64, data);
	if (err.code != kSuccess) {
		return err;
	}

	return util::getError(kSuccess);
}

Error ApiImpl::PrepareAccessedListResponse(
	const ApiContext *context,
	PathsAccessed *accessed_list) {

	json_error_t json_error;
	json_t *response_json = context->GetResponseJsonObject();

	json_t *path_list_json_obj = json_object_get(response_json, kPathList);
	if (path_list_json_obj == NULL) {
		return util::getError(kMissingJsonObject, kPathList);
	}

	json_t *accessed_list_json_obj = json_object_get(path_list_json_obj, kPaths);
	if (accessed_list_json_obj == NULL) {
		return util::getError(kMissingJsonObject, kPaths);
	}

	const char *k;
	json_t *v;
	json_object_foreach(accessed_list_json_obj, k, v) {
		if (json_is_integer(v)) {
			accessed_list->paths[std::string(k)] = json_integer_value(v);
		}
	}

	return util::getError(kSuccess);
}

}  // namespace qfsclient

