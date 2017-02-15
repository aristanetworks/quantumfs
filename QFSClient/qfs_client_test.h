// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_CLIENT_TEST_H_
#define QFS_CLIENT_TEST_H_

#include <string>

#include <gtest/gtest.h>

#include "qfs_client.h"

namespace qfsclient {

class QfsClientTest : public testing::Test {
 protected:
	virtual void SetUp();
	virtual void TearDown();

	std::string api_path;
	ino_t api_inode_id;
	Api *api;

 protected:
	// path of temporary directory tree created by CreateTempDirTree()
	std::string tree;

 private:
	void CreateTempDirTree(const std::vector<std::string> &path);
	void CreateTestApiFile();
};

class QfsClientApiTest : public QfsClientTest, SendCommandHook {
	virtual void SetUp();
	virtual void TearDown();

 public:
	// SendTestHook, together with expected_written_command and read_command
	// is for the use of API tests - they can set the expected command that
	// WriteCommand() is expected to write and provide a test response
	// for ReadResponse() can return (via SendCommand) to the API function.
	virtual Error SendTestHook();

 protected:
	Api::CommandBuffer expected_written_command;
	Api::CommandBuffer actual_written_command;
	Api::CommandBuffer read_command;
};

class QfsClientDeterminePathTest : public QfsClientTest {
	virtual void SetUp();
	virtual void TearDown();
};

class QfsClientCommandBufferTest : public testing::Test {
 protected:
	virtual void SetUp();
	virtual void TearDown();
};

} // namespace qfsclient

#endif // QFS_CLIENT_TEST_H_
