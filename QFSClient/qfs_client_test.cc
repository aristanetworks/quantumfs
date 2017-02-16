// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "qfs_client_test.h"

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <iostream>

#include <gtest/gtest.h>
#include <jansson.h>

#include "qfs_client.h"
#include "qfs_client_util.h"

namespace qfsclient {

void QfsClientTest::CreateTempDirTree(const std::vector<std::string> &path) {
	char temp_directory_template[128] = "/tmp/qfs-client-test-XXXXXX";
	char *temp_directory_name = mkdtemp(temp_directory_template);

	if (!temp_directory_name) {
		util::fperror(__func__, "mkdtemp()");
		this->tree.clear();
		return;
	}

	std::string temp_directory_path(temp_directory_name);

	for (auto path_part : path) {
		temp_directory_path += "/" + path_part;

		if (mkdir(temp_directory_path.c_str(), S_IRWXU) == -1) {
			util::fperror(__func__, "mkdir()");
			this->tree.clear();
			return;
		}
	}

	this->tree = temp_directory_path;
}

// create a test API file
void QfsClientTest::CreateTestApiFile() {
	CreateTempDirTree( { "one", "two", "three", "four", "five" } );

	if (this->tree.length() == 0) {
		return;
	}

	if (chdir(this->tree.c_str()) != 0) {
		util::fperror(__func__, "chdir()");
		this->tree.clear();
		return;
	}

	std::string api_location = this->tree.substr(0, this->tree.find("two"));
	api_location.append(kApiPath);

	int fd = creat(api_location.c_str(), S_IRWXU);
	if (fd == -1) {
		util::fperror(__func__, "creat()");
		this->tree.clear();
		return;
	}
	close(fd);

	struct stat file_status;

	if (lstat(api_location.c_str(), &file_status) != 0) {
		util::fperror(__func__, "lstat()");
		this->tree.clear();
		return;
	}

	this->api_inode_id = file_status.st_ino;
	this->api_path = api_location;
}

void QfsClientTest::SetUp() {
	this->api = NULL;

	QfsClientTest::CreateTestApiFile();

	if (this->tree.empty()) {
		// We just return in case of an error because
		// testing::Test::SetUp() has a void return type. In such
		// a case though, the api member will be NULL and tests that
		// depend on it should do something like ASSERT_FALSE(api == NULL)
		// first, so that the test will fail if this method has failed
		return;
	}

	this->api = new Api();
	this->api->api_inode_id = api_inode_id;
}

void QfsClientTest::TearDown() {
	if (this->api_path.length() > 0) {
		unlink(this->api_path.c_str());
	}
	delete this->api;
	this->api = NULL;
}

TEST_F(QfsClientTest, OpenTest) {
	ASSERT_FALSE(this->api == NULL);

	Error err = this->api->Open();
	ASSERT_EQ(err.code, kSuccess);

	// Test again with path passed to constructor
	std::string path = this->api->path;
	delete this->api;
	this->api = new Api(path.c_str());
	this->api->api_inode_id = this->api_inode_id;

	err = this->api->Open();
	ASSERT_EQ(err.code, kSuccess);
}

// Api::SendCommand() calls WriteCommand() and ReadResponse(), so
// this test should cover those methods too
TEST_F(QfsClientTest, SendCommandTest) {
	ASSERT_FALSE(this->api == NULL);

	const Api::CommandBuffer send { "sausages",  sizeof("sausages") };
	Api::CommandBuffer result;

	Error err = this->api->SendCommand(send, &result);

	ASSERT_EQ(err.code, kSuccess);
	ASSERT_EQ(send.size, result.size);
	ASSERT_STREQ((const char *)send.data, (const char *)result.data);
}

TEST_F(QfsClientTest, SendCommandNoFileTest) {
	ASSERT_FALSE(this->api == NULL);

	const Api::CommandBuffer send { "sausages",  sizeof("sausages") };
	Api::CommandBuffer result;

	// delete the API file before it ever gets opened, then try to send a
	// command (SendCommand() will attempt to find and then open the file)
	unlink(this->api_path.c_str());
	Error err = this->api->SendCommand(send, &result);
	ASSERT_EQ(err.code, kCantFindApiFile);
}

TEST_F(QfsClientTest, WriteCommandFileNotOpenTest) {
	ASSERT_FALSE(this->api == NULL);

	const Api::CommandBuffer send { "sausages",  sizeof("sausages") };
	Api::CommandBuffer result;

	Error err = this->api->WriteCommand(send);
	ASSERT_EQ(err.code, kApiFileNotOpen);
}

TEST_F(QfsClientTest, CheckWorkspacePathValidTest) {
	ASSERT_FALSE(this->api == NULL);

	Error err = this->api->CheckWorkspacePathValid("badworkspacepath1");
	ASSERT_EQ(err.code, kWorkspacePathInvalid);

	err = this->api->CheckWorkspacePathValid("bad/workspacepath2");
	ASSERT_EQ(err.code, kWorkspacePathInvalid);

	err = this->api->CheckWorkspacePathValid("bad/workspace/path/3");
	ASSERT_EQ(err.code, kWorkspacePathInvalid);

	err = this->api->CheckWorkspacePathValid("good/workspace/path");
	ASSERT_EQ(err.code, kSuccess);
}

void QfsClientApiTest::SetUp() {
	QfsClientTest::SetUp();

	// set up hook for SendCommand() (see QfsClientApiTest::SendTestHook())
	this->api->send_test_hook = this;
	this->expected_written_command.size = 0;
	this->actual_written_command.size = 0;

	// JSON that we want tested functions to read
	std::string read_command_json =
		"{'CommandId':2,"
		"'ErrorCode':0,"
		"'Message':'success',"
		"'AccessList':{'file1':true,'file2':false,'file3':true}}";
	util::requote(read_command_json);
	this->read_command.CopyString(read_command_json.c_str());
}

void QfsClientApiTest::TearDown() {
	QfsClientTest::TearDown();

	this->expected_written_command.size = 0;
	this->actual_written_command.size = 0;
	this->read_command.size = 0;
}

Error QfsClientApiTest::SendTestHook() {
	// conveniently, we can use ReadResponse() and WriteCommand() to do
	// the file IO for us. They are already tested by other tests.

	// set this->actual_written_command to what's been written to API file
	Error err = this->api->ReadResponse(&(this->actual_written_command));
	if (err.code != kSuccess) {
		return err;
	}

	// write what's in this->read_command to the API file, ready to be read
	// by the API function's SendCommand()
	err = this->api->WriteCommand(this->read_command);
	if (err.code != kSuccess) {
		return err;
	}

	return util::getError(kSuccess);
}

// This test covers Api::GetAccessed(). There are no negative tests for
// Api::GetAccessed() because the Jansson calls it makes should be covered by
// Jansson's own unit tests, and our functions that GetAccessed() calls all have
// their own tests, including negative tests.
TEST_F(QfsClientApiTest, GetAccessedTest) {
	ASSERT_FALSE(this->api == NULL);

	Error err = this->api->Open();
	ASSERT_EQ(err.code, kSuccess);

	// set up expected written JSON:
	std::string expected_written_command_json =
		"{'CommandId':2,'WorkspaceRoot':'test/workspace/root'}";
	util::requote(expected_written_command_json);
	this->expected_written_command.CopyString(
	       expected_written_command_json.c_str());

	// we have provided hooks for WriteCommand() and ReadResponse() so that:
	// 1. we can test that WriteCommand() writes correct JSON to the API file
	// 2. ReadResponse() returns appropriate JSON when called by GetAccessed()
	// 3. GetAccessed() does what it's supposed to do in response to the JSON
	//    it thinks it's read from the API file
	err = this->api->GetAccessed("test/workspace/root");
	ASSERT_EQ(err.code, kSuccess);

	// compare what the API function actually wrote with what we expected
	ASSERT_EQ(this->actual_written_command.size,
		  this->expected_written_command.size);
	ASSERT_STREQ((char*)this->actual_written_command.data,
		     (char*)this->expected_written_command.data);
}

// Test Api::SendJson(), which is shared by all API handlers
TEST_F(QfsClientApiTest, SendJsonTest) {
	ASSERT_FALSE(this->api == NULL);

	// create JSON for request
	// See http://jansson.readthedocs.io/en/2.4/apiref.html#building-values for
	// an explanation of the format strings that json_pack_ex can take.
	json_error_t json_error;
	json_t *request_json = json_pack_ex(&json_error, 0,
					    "{s:i,s:s}",
					    "CommandId", kCmdGetAccessed,
					    "WorkspaceRoot", "one/two/three");
	ASSERT_FALSE(request_json == NULL);

	// create expected JSON string to have been written
	std::string expected_written_command_json =
		"{'CommandId':2,'WorkspaceRoot':'one/two/three'}";
	util::requote(expected_written_command_json);
	this->expected_written_command.CopyString(
	       expected_written_command_json.c_str());

	util::JsonApiContext context;
	Error err = this->api->SendJson(request_json, &context);
	json_decref(request_json); // release the JSON object
	ASSERT_EQ(err.code, kSuccess);

	// compare what the API function actually wrote with what we expected
	ASSERT_EQ(this->actual_written_command.size,
		  this->expected_written_command.size);
	ASSERT_STREQ((char*)this->actual_written_command.data,
		     (char*)this->expected_written_command.data);
}

// Negative SendJson test to trigger kJSONTooBig
TEST_F(QfsClientApiTest, SendJsonTestJsonTooBig) {
	ASSERT_FALSE(this->api == NULL);

	// create very long JSON for request
	const size_t long_string_size = kCmdBufferSize + 10;
	char long_string[long_string_size + 10];

	char c = ' ';
	for (int i = 0; i < long_string_size; i++) {
		long_string[i] = c;
		if (++c >= 0x7F) {
			c = ' ';
		}
	}
	long_string[long_string_size - 1] = '\0';

	// create extra-long JSON string to send
	// See http://jansson.readthedocs.io/en/2.4/apiref.html#building-values for
	// an explanation of the format strings that json_pack_ex can take.
	json_error_t json_error;
	json_t *request_json = json_pack_ex(&json_error, 0,
					    "{s:i,s:s,s:s}",
					    kCommandId, kCmdGetAccessed,
					    kWorkspaceRoot, "one/two/three",
					    "LongField", long_string);
	ASSERT_FALSE(request_json == NULL);

	util::JsonApiContext context;
	Error err = this->api->SendJson(request_json, &context);
	json_decref(request_json); // release the JSON object
	ASSERT_EQ(err.code, kJsonTooBig);
}

// Test Api::CheckCommonApiResponse(), which is shared by all API handlers
TEST_F(QfsClientApiTest, CheckCommonApiResponseTest) {
	ASSERT_FALSE(this->api == NULL);

	util::JsonApiContext context;
	Error err = this->api->CheckCommonApiResponse(this->read_command, &context);
	ASSERT_EQ(err.code, kSuccess);
}

// Negative test for Api::CheckCommonApiResponse(), which should trigger a JSON
// parse error
TEST_F(QfsClientApiTest, CheckCommonApiResponseBadJsonTest) {
	ASSERT_FALSE(this->api == NULL);

	Api::CommandBuffer test_response;

	// corrupt the JSON that CheckCommonApiResponse will try to parse
	ASSERT_TRUE(this->read_command.size > 0);	// fail if no JSON
	this->read_command.size = this->read_command.size / 2;
	this->read_command.data[read_command.size] = '\0';

	util::JsonApiContext context;
	Error err = this->api->CheckCommonApiResponse(this->read_command, &context);
	ASSERT_EQ(err.code, kJsonDecodingError);
}

// Negative test for Api::CheckCommonApiResponse(), which should trigger a missing
// JSON object
TEST_F(QfsClientApiTest, CheckCommonApiMissingJsonObjectTest) {
	ASSERT_FALSE(this->api == NULL);

	Api::CommandBuffer test_response;

	// corrupt ErrorCode in the JSON that CheckCommonApiResponse will try
	// to parse
	char *error_code_loc = strstr((char*)this->read_command.data, kErrorCode);
	ASSERT_TRUE(error_code_loc != NULL);	// fail if no ErrorCode field

	if (error_code_loc != NULL) {
		error_code_loc[1] = 'Q';
	}

	util::JsonApiContext context;
	Error err = this->api->CheckCommonApiResponse(this->read_command, &context);
	ASSERT_EQ(err.code, kMissingJsonObject);
}

TEST_F(QfsClientApiTest, PrepareAccessedListResponseTest) {
	ASSERT_FALSE(this->api == NULL);

	std::unordered_map<std::string, bool> accessed_list;

	util::JsonApiContext context;
	Error err = this->api->CheckCommonApiResponse(this->read_command, &context);
	ASSERT_EQ(err.code, kSuccess);

	err = this->api->PrepareAccessedListResponse(&context, &accessed_list);
	ASSERT_EQ(err.code, kSuccess);

	ASSERT_EQ(accessed_list.size(), 3);
	ASSERT_TRUE(accessed_list.at("file1"));
	ASSERT_FALSE(accessed_list.at("file2"));
	ASSERT_TRUE(accessed_list.at("file3"));
}

// Negative test for Api::PrepareAccessedListResponse() to check that a missing
// AccessList triggers a kMissingJSONObject error
TEST_F(QfsClientApiTest, PrepareAccessedListResponseNoAccessListTest) {
	ASSERT_FALSE(this->api == NULL);

	std::unordered_map<std::string, bool> accessed_list;

	// corrupt AccessList in the JSON that CheckCommonApiResponse will try
	// to parse
	char *access_list_loc = strstr((char*)this->read_command.data, kAccessList);
	ASSERT_TRUE(access_list_loc != NULL);	// fail if no AccessList field

	if (access_list_loc != NULL) {
		access_list_loc[1] = 'Q';
	}

	util::JsonApiContext context;
	Error err = this->api->CheckCommonApiResponse(this->read_command, &context);
	ASSERT_EQ(err.code, kSuccess);

	err = this->api->PrepareAccessedListResponse(&context, &accessed_list);
	ASSERT_EQ(err.code, kMissingJsonObject);
}

TEST_F(QfsClientApiTest, FormatAccessedListTest) {
	std::string expected_output = "------ Created Files ------\n"
	"file3\n"
	"file1\n"
	"------ Accessed Files ------\n"
	"file2\n";

	std::unordered_map<std::string, bool> accessed_list;
	accessed_list["file1"] = true;
	accessed_list["file2"] = false;
	accessed_list["file3"] = true;

	std::string actual_output = this->api->FormatAccessedList(accessed_list);
	ASSERT_STREQ(actual_output.c_str(), expected_output.c_str());
}

void QfsClientDeterminePathTest::SetUp() {
	QfsClientTest::SetUp();

	std::string dummy_api_file_location = this->tree.substr(
	    0, this->tree.find("four"));

	dummy_api_file_location.append(kApiPath);

	int fd = creat(dummy_api_file_location.c_str(), S_IRWXU);
	if (fd == -1) {
		util::fperror(__func__, "creat()");
		return;
	}
	close(fd);

	std::string dummy_api_dir_location = this->tree.substr(
	    0, this->tree.find("three"));

	dummy_api_dir_location.append(kApiPath);

	if (mkdir(dummy_api_dir_location.c_str(), S_IRWXU) == -1) {
		util::fperror(__func__, "mkdir()");
		return;
	}
}

void QfsClientDeterminePathTest::TearDown() {
	QfsClientTest::TearDown();
}

TEST_F(QfsClientDeterminePathTest, DeterminePathTest) {
	ASSERT_FALSE(this->api == NULL);

	Error err = this->api->DeterminePath();
	ASSERT_EQ(err.code, kSuccess);

	if (this->api->path.length() <= this->api_path.length()) {
		ASSERT_STREQ(this->api->path.c_str(), this->api_path.c_str());
	} else {
		// the reason we don't always use ASSERT_STREQ is that on macOS,
		// mkdtemp() creates tmp files in /private/tmp, even when the
		// template begins with "/tmp", so the call to getcwd() in
		// Api::DeterminePath() will return a string beginning
		// "/private/tmp/..."
		// This test verifies that this->api->path *ends* with whatever is
		// in this->api_path.
		ASSERT_EQ(0,
			  this->api->path.compare(
				this->api->path.length() - this->api_path.length(),
				this->api_path.length(),
				this->api_path));
	}

	// Test again when the api file doesn't exist anywhere
	unlink(this->api_path.c_str());
	err = this->api->DeterminePath();
	ASSERT_EQ(err.code, kCantFindApiFile);
}

} // namespace qfsclient

int main(int argc, char **argv) {

	::testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();

	return 0;
}

