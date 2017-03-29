// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFSCLIENT_QFS_CLIENT_H_
#define QFSCLIENT_QFS_CLIENT_H_

#include <string>
#include <vector>

namespace qfsclient {

typedef uint8_t byte;

/// Potential error values that may be returned in an ErrorCode object
/// by methods of the Api class
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
	kBufAlignmentFail = 7,

	// Reading a command from the API file failed
	kApiFileReadFail = 8,

	// The given workspace path was not valid
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

	// The given workspace name was not valid
	kWorkspaceNameInvalid = 15,

	// a JSON object was found with the wrong type
	kJsonObjectWrongType = 16,
};

/// An `Error` object is returned by many member functions of the Api class. The
/// possible error code values are defined above in the `ErrorCode` enum, and the
/// message member contains a human-readable error message, possibly with further
/// information.
struct Error {
	ErrorCode code;
	std::string message;
};

/// `Api` provides the public interface to QuantumFS API calls.
class Api {
 public:
	/// Retrieve the list of accessed and created files for a specified
	/// workspace. This list will be written to standard output.
	///
	/// @param [in] `workspace_root` A string containing the workspace root name
	/// whose list of accessed files is to be retrieved.
	///
	/// @return An `Error` object that indicates success or failure.
	virtual Error GetAccessed(const char *workspace_root) = 0;

	/// Takes an extended key along with other file metadata (permissions,
	/// UID and GID) and inserts it into the given destination directory.
	///
	/// @param [in] destination A string containing a path to the intended
	/// location of the file, which should include the typespace, namespace and
	/// workspace names. For example: `user/joe/myworkspace/usr/lib/destfile`.
	/// @param [in] key A base64 string containing the extended key of the file
	/// that comes from the file's `quantumfs.key` extended attribute.
	/// @param [in] permissions The the unix permissions for the file
	/// @param [in] uid The user ID of the file.
	/// @param [in] gid The group ID of the file.
	///
	/// @return An `Error` object that indicates success or failure.
	virtual Error InsertInode(const char *destination,
				  const char *key,
				  uint32_t permissions,
				  uint32_t uid,
				  uint32_t gid) = 0;

	/// Branch a given workspace into a new workspace with the supplied name.
	///
	/// @param [in] `source` A string containing the root name of the workspace
	/// that is to be branched.
	/// @param [in] `destination` A string containing the new root name of the
	/// workspace that should result from the branch operation.
	///
	/// @return An `Error` object that indicates success or failure.
	virtual Error Branch(const char *source,
			     const char *destination) = 0;

	/// Store a block of data persistently.
	///
	/// @param [in] `key` A base64 string (which could represent a binary key)
	/// to be used to identify the data and which will be needed later to
	/// retrieve the data.
	/// @param [in] `data` A string that holds a base64 representation of the
	/// block of data to store.
	///
	/// @return An `Error` object that indicates success or failure.
	virtual Error SetBlock(const std::vector<byte> &key,
			       const std::vector<byte> &data) = 0;

	/// Retrieve a block of data from the persistent data store.
	///
	/// @param [in] `key` A base64 string (which could represent a binary key)
	/// to be used to identify the data to be retrieved.
	/// @param [out] `data` A `std::string` that will be modified to hold a
	/// base64 representation of the data block.
	///
	/// @return An `Error` object that indicates success or failure.
	virtual Error GetBlock(const std::vector<byte> &key,
			       std::vector<byte> *data) = 0;
};

/// Get an instance of an `Api` object that can be used to call QuantumFS API
/// functions. The API file is searched for starting in the current working
/// directory and walking up the directory tree from there.
///
/// @param [out] `api` A pointer to an `Api` pointer that will be modified.
///
/// @return An` Error` object that indicates success or failure.
Error GetApi(Api **api);

/// Get an instance of an `Api` object that can be used to call QuantumFS API
/// functions. The API file is searched for starting in the given directory and
/// walking up the directory tree from there.
///
/// @param [in] A path to a directory where the search for the API file will begin.
/// @param [out] `api` A pointer to an `Api` pointer that will be modified.
///
/// @return An `Error` object that indicates success or failure.
Error GetApi(const char *path, Api **api);

/// Release an Api object and any resources (such as open files) associated
/// with it. The pointer will no longer be valid after it has been released.
///
/// @param [in] `api` A pointer to an `Api` object that will be released.
void ReleaseApi(Api *api);

}  // namespace qfsclient

#endif  // QFSCLIENT_QFS_CLIENT_H_

