// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_CLIENT_IMPLEMENTATION_H_
#define QFS_CLIENT_IMPLEMENTATION_H_

#include <stdint.h>
#include <sys/types.h>

#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

#include "qfs_client.h"

namespace qfsclient {

typedef uint8_t byte;

const char kApiPath[] = "api";
const int kInodeIdApi = 2;

// Class used for holding internal context about an in-flight API call. It may be
// passed between functions used to handle an API call and could (for example)
// hold an object representing a parsed JSON response string to avoid having to
// parse that string more than once.
class ApiContext {
 public:
	ApiContext();
	virtual ~ApiContext() = 0;
};

// Class to be implemented by tests ONLY that a test can supply; if an instance
// of this class is supplied, then its SendTestHook() method will be called
// by SendCommand() in between writing a command and reading the
// response. This allows a test to check exactly what got written to the
// api file by WriteCommand() and to place a test response in the same
// file to be read back by ReadCommand().
class SendCommandHook {
 public:
	virtual Error SendTestHook() = 0;
};

// ApiImpl provides the concrete implentation for QuantumFS API calls and whatever
// related support logic they need. If an ApiImpl object is constructed with no
// path, it will start looking for the API file in the current working directory
// and work upwards towards the root from there. If it is constructed with a path,
// then it is assumed that the API file will be found at the given location.
class ApiImpl: public Api {
 public:
	ApiImpl();
	explicit ApiImpl(const char *path);
	virtual ~ApiImpl();

	// Attempts to open the api file - including attempting to determine
	// its location if the Api object was constructed without being given
	// a path to the location of the api file. Returns an error object to
	// indicate the outcome.
	Error Open();

	// Closes the api file if it's still open.
	void Close();

	// implemented API functions
	Error GetAccessed(const char *workspace_root);

 private:
	// CommandBuffer is used internally to store the raw content of a command to
	// send to (or a response received from) the API - typically in JSON format.
	class CommandBuffer {
	 public:
		CommandBuffer();
		virtual ~CommandBuffer();

		// Return a const pointer to the data in the buffer
		const byte *Data() const;

		// Return the size of the data stored in the buffer
		size_t Size() const;

		// Reset the buffer such that it will contain no data and will
		// have a zero size
		void Reset();

		// Append a block of data to the buffer. Returns an error if the
		// buffer would have to be grown too large to add this block
		ErrorCode Append(const byte *data, size_t size);

		// copy a string into the buffer. An error will be returned if
		// the buffer would have to be grown too large to fit the string.
		ErrorCode CopyString(const char * s);

	 private:
		std::vector<byte> data;

		FRIEND_TEST(QfsClientApiTest, CheckCommonApiResponseBadJsonTest);
		FRIEND_TEST(QfsClientApiTest, CheckCommonApiMissingJsonObjectTest);

		FRIEND_TEST(QfsClientCommandBufferTest, AppendTest);
		FRIEND_TEST(QfsClientCommandBufferTest, AppendAndCopyLotsTest);
		FRIEND_TEST(QfsClientCommandBufferTest, CopyStringTest);
	};

	// Work out the location of the api file (which must be called 'api'
	// and have an inode ID of 2) by looking in the current directory
	// and walking up the directory tree towards the root until it's found.
	// Returns an error object to indicate the outcome.
	Error DeterminePath();

	// Writes the given command to the api file and immediately tries to
	// read a response form the same file. Returns an error object to
	// indicate the outcome.
	Error SendCommand(const CommandBuffer &command, CommandBuffer *response);

	// Writes the given command to the api file. Returns an error object to
	// indicate the outcome.
	Error WriteCommand(const CommandBuffer &command);

	// Attempts to read a response from the api file. Returns an error object to
	// indicate the outcome.
	Error ReadResponse(CommandBuffer *command);

	// Given a workspace path, test it for validity, returning an error to
	// indicate the path's validity.
	Error CheckWorkspacePathValid(const char *workspace_root);

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

	// Pointer to a SendCommandHook instance (used for testing ONLY). The
	// purpose of the SendCommandHook class is described along with its
	// definition.
	SendCommandHook *send_test_hook;

	// Internal member function to perform processing common to all API calls,
	// such as parsing JSON and checking for response errors
	Error CheckCommonApiResponse(const CommandBuffer &response,
				     ApiContext *context);

	// Send the JSON representation of the command to the API file and parse the
	// response, then check the response for an error. The context object will
	// be used to carry the parsed JSON response for use by the next stage.
	// note: the parameter request_json_ptr is a void pointer because it's not
	// possible to forward-declare json_t in Jansson versions before 2.5;
	// in previous versions of Jansson, json_t is a typedef to an anonymous
	// C struct, which can't be forward declared.
	Error SendJson(const void *request_json_ptr, ApiContext *context);

	// Convert the JSON response received for the GetAccessed() API call into
	// a structure ready for formatting and then writing to stdout. Returns
	// an Error struct to indicate success or otherwise
	Error PrepareAccessedListResponse(
		const ApiContext *context,
		std::unordered_map<std::string, bool> *accessed_list);

	// Convert the response to the GetAccessed() API call for a string to be
	// pretty-printed to stdout
	std::string FormatAccessedList(
		const std::unordered_map<std::string, bool> &accessed);

	friend class QfsClientTest;
	FRIEND_TEST(QfsClientTest, SendCommandTest);
	FRIEND_TEST(QfsClientTest, SendLargeCommandTest);
	FRIEND_TEST(QfsClientTest, SendCommandFileRemovedTest);
	FRIEND_TEST(QfsClientTest, SendCommandNoFileTest);
	FRIEND_TEST(QfsClientTest, SendCommandCantOpenFileTest);
	FRIEND_TEST(QfsClientTest, WriteCommandFileNotOpenTest);
	FRIEND_TEST(QfsClientTest, OpenTest);
	FRIEND_TEST(QfsClientTest, CheckWorkspacePathValidTest);

	friend class QfsClientApiTest;
	FRIEND_TEST(QfsClientApiTest, CheckCommonApiResponseTest);
	FRIEND_TEST(QfsClientApiTest, CheckCommonApiResponseBadJsonTest);
	FRIEND_TEST(QfsClientApiTest, CheckCommonApiMissingJsonObjectTest);
	FRIEND_TEST(QfsClientApiTest, PrepareAccessedListResponseTest);
	FRIEND_TEST(QfsClientApiTest, PrepareAccessedListResponseNoAccessListTest);
	FRIEND_TEST(QfsClientApiTest, FormatAccessedListTest);
	FRIEND_TEST(QfsClientApiTest, SendJsonTest);
	FRIEND_TEST(QfsClientApiTest, SendJsonTestJsonTooBig);

	FRIEND_TEST(QfsClientDeterminePathTest, DeterminePathTest);

	FRIEND_TEST(QfsClientCommandBufferTest, FreshBufferTest);
	FRIEND_TEST(QfsClientCommandBufferTest, ResetTest);
	FRIEND_TEST(QfsClientCommandBufferTest, AppendTest);
	FRIEND_TEST(QfsClientCommandBufferTest, AppendAndCopyLotsTest);
	FRIEND_TEST(QfsClientCommandBufferTest, CopyStringTest);
};

} // namespace qfsclient

#endif // QFS_CLIENT_IMPLEMENTATION_H_
