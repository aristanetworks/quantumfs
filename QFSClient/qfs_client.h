// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_CLIENT_H_
#define QFS_CLIENT_H_

#include <gtest/gtest_prod.h>
#include <fstream>
#include <vector>
#include <sys/types.h>
#include <stdint.h>

namespace qfsclient {

class Api;

typedef uint8_t byte;

const int kCmdBufferSize = 4096;
const char *kApiPath = "api";
const int kInodeIdApi = 2;

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
	kApiFileReadFail = 8
};

// An error object returned by many member functions of the Api class. The possible
// error code values are defined above in the ErrorCode enum, and the message
// member contains a human-readable error message, possibly with further information.
struct Error {
	ErrorCode code;
	std::string message;
};

// Api (except for its private members) provides the public interface to QuantumFS
// API calls. If an Api object is constructed with no path, it will start looking
// for the API file in the current working directory and work upwards towards the
// root from there. If it is constructed with a path, then it is assumed that the
// API file will be found at the given location.
class Api {
 public:
	Api();
	explicit Api(const char *path);
	virtual ~Api();

	// Attempts to open the api file - including attempting to determine
	// its location if the Api object was constructed without being given
	// a path to the location of the api file. Returns an error object to
	// indicate the outcome.
	Error Open();

	// Closes the api file if it's still open.
	void Close();

	// Sample API function; just implementing this one for now (see:
	// http://gut/repos/quantumfs/blob/master/cmds.go for other API
	// functions to provide)
	Error GetAccessed(const char *workspace_root);

 private:
	// CommandBuffer is used internally to store the raw content of commands to be
	// sent to (or received from) the API - typically in JSON format.
	struct CommandBuffer {
		byte data[kCmdBufferSize];
		size_t size;
	};

	// Work out the location of the api file (which must be called 'api'
	// and have an inode ID of 2) by looking in the current directory
	// and walking up the directory tree towards the root until it's found.
	// Returns an error object to indicate the outcome.
	Error DeterminePath();

	// Writes the given command to the api file and immediately tries to
	// read a response form the same file. Returns an error object to
	// indicate the outcome.
	Error SendCommand(const CommandBuffer &command, CommandBuffer *result);

	// Writes the given command to the api file. Returns an error object to
	// indicate the outcome.
	Error WriteCommand(const CommandBuffer &command);

	// Attempts to read a response from the api file. Returns an error object to
	// indicate the outcome.
	Error ReadResponse(CommandBuffer *command);

	std::fstream file;

	// We use the presence of a value in this member variable to indicate that
	// the API file's location is known (either because it was passed to the
	// Api constructor, or because it was found by DeterminePath()). It doesn't
	// necessarily mean that the file has been opened: Api::Open() should do
	// that. Api::Open() should still be called before trying to call an API
	// function.
	std::string path;

	// Expected inode ID of the api file. The only reason we have this
	// instead of using the INODE_ID_API constant is that the unit tests
	// need to modify it (so that they can test against an arbitrary
	// temporary file which won't have an inode ID that's known in
	// advance)
	ino_t api_inode_id;

	friend class QfsClientTest;
	FRIEND_TEST(QfsClientTest, SendCommandTest);
	FRIEND_TEST(QfsClientTest, SendCommandFileRemovedTest);
	FRIEND_TEST(QfsClientTest, SendCommandNoFileTest);
	FRIEND_TEST(QfsClientTest, SendCommandCantOpenFileTest);
	FRIEND_TEST(QfsClientTest, WriteCommandFileNotOpenTest);
	FRIEND_TEST(QfsClientTest, OpenTest);
	FRIEND_TEST(QfsClientDeterminePathTest, DeterminePathTest);
};

} // namespace qfsclient

#endif // QFS_CLIENT_H_

