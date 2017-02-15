// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_CLIENT_H_
#define QFS_CLIENT_H_

#include <stdint.h>
#include <sys/types.h>

#include <fstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <gtest/gtest_prod.h>

namespace qfsclient {

typedef uint8_t byte;

const size_t kCmdBufferSize = 4096;
const char kApiPath[] = "api";
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

	// The JSON data was too large to fit into the command buffer
	kJsonTooBig = 14,

	// An internal buffer is getting too big to increase in size
	kBufferTooBig = 15,
};
wibbles
// An error object returned by many member functions of the Api class. The possible
// error code values are defined above in the ErrorCode enum, and the message
// member contains a human-readable error message, possibly with further information.
struct Error {
	ErrorCode code;
	std::string message;
};

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

		// Add one byte to the buffer and grow its internal data store if
		// necessary. Returns an ErrorCode if the buffer would have to be
		// grown too large to add this byte
		ErrorCode Add(byte datum);

		// copy a string up to the maximum buffer size (including NUL
		// terminator) into the buffer. Note that if the string is larger
		// than will fit in the buffer, only the first kCmdBufferSize - 1
		// characters will be copied, but the number of characters actaully
		// copied will be returned.
		ErrorCode CopyString(const char * s);

	 private:
		static const size_t kBufferSizeIncrement;
		std::vector<byte> data;

		FRIEND_TEST(QfsClientApiTest, CheckCommonApiResponseBadJsonTest);
		FRIEND_TEST(QfsClientApiTest, CheckCommonApiMissingJsonObjectTest);

		FRIEND_TEST(QfsClientCommandBufferTest, AddTest);
		FRIEND_TEST(QfsClientCommandBufferTest, AddAndCopyLotsTest);
		FRIEND_TEST(QfsClientCommandBufferTest, AddAndResizeTest);
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
	FRIEND_TEST(QfsClientCommandBufferTest, AddTest);
	FRIEND_TEST(QfsClientCommandBufferTest, AddAndCopyLotsTest);
	FRIEND_TEST(QfsClientCommandBufferTest, AddAndResizeTest);
	FRIEND_TEST(QfsClientCommandBufferTest, CopyStringTest);
};

} // namespace qfsclient

#endif // QFS_CLIENT_H_

