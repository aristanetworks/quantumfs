// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_CLIENT_H_
#define QFS_CLIENT_H_

namespace qfsclient {

// Potential error values that may be returned by methods of the Api class
enum ErrorCode {
	// Success
	kSuccess = 0,

	// Couldn't determine the current working directory while looking for the
	// API file
	kDontKnowCwd = 1,

	// The file named 'api' with inode ID 2 couldn't be found when searching
	// upwards from the current working directory towards the root.
	kCantFindApiFile = 2,

	// The API file whose name was passed to the Api constructor or the API
	// file found by searching upwards from the current working directory
	// failed to open
	kCantOpenApiFile = 3,

	// Api.Open() was not called successfully before attempting to call an
	// API function
	kApiFileNotOpen = 4,

	// An attempt to seek to the start of the API file before reading or
	// writing a command failed
	kApiFileSeekFail = 5,

	// Writing a command to the API file failed
	kApiFileWriteFail = 6,

	// Writing a command to the API file failed
	kApiFileFlushFail = 7,

	// Reading a command from the API file failed
	kApiFileReadFail = 8,

	// The given workspace was not valid
	kWorkspacePathInvalid = 9,

	// Error encoding a request to JSON
	kJsonEncodingError = 10,

	// Error parsing JSON received from quantumfs
	kJsonDecodingError = 11,

	// An expected JSON object was missing
	kMissingJsonObject = 12,

	// The quantumfs API returned an error
	kApiError = 13,

	// An internal buffer is getting too big to increase in size
	kBufferTooBig = 14,
};

// An error object returned by many member functions of the Api class. The possible
// error code values are defined above in the ErrorCode enum, and the message
// member contains a human-readable error message, possibly with further information.
struct Error {
	ErrorCode code;
	std::string message;
};

// Api provides the public interface to QuantumFS API calls.
class Api {
 public:
	virtual Error GetAccessed(const char *workspace_root) = 0;
};

// Get an instance of an Api object that can be used to call QuantumFS API
// functions. Takes a pointer to an Api pointer that will be modified on success
// and returns an Error object that indicates success or failure.
Error GetApi(Api **api);

// Get an instance of an Api object that can be used to call QuantumFS API
// functions. This version takes a path to a directory in the filesystem that
// will be used as the start point from which to look for an api file. Takes a
// pointer to an Api pointer that will be modified on success, and returns an
// Error object that indicates success or failure.
Error GetApi(const char *path, Api **api);

// Release an Api object and any resources (such as open files) associated
// with it. The pointer will no longer be valid after it has been released.
void ReleaseApi(Api *api);

} // namespace qfsclient

#endif // QFS_CLIENT_H_

