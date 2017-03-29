// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFSCLIENT_QFS_CLIENT_TEST_H_
#define QFSCLIENT_QFS_CLIENT_TEST_H_

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "QFSClient/qfs_client.h"
#include "QFSClient/qfs_client_implementation.h"

namespace qfsclient {

class QfsClientTest : public testing::Test {
 protected:
	virtual void SetUp();
	virtual void TearDown();

	std::string api_path;
	ino_t api_inode_id;
	ApiImpl *api;

 protected:
	// path of temporary directory tree created by CreateTempDirTree()
	std::string tree;

 private:
	void CreateTempDirTree(const std::vector<std::string> &path);
	void CreateTestApiFile();
};

#ifdef REAL_QFS
// tests to run on a real QuantumFS filesystem
class QfsClientQFSTest : public testing::Test {
 protected:
	virtual void SetUp();
	virtual void TearDown();

 protected:
	ApiImpl *api;

	// path to location in a QuantumFS filesystem
	std::string location;
};
#endif  // REAL_QFS

class QfsClientApiTest : public QfsClientTest, TestHook {
	virtual void SetUp();
	virtual void TearDown();

 private:
	// TestHook, together with expected_written_command and read_command
	// is for the use of API tests - they can set the expected command that
	// WriteCommand() is expected to write and provide a test response
	// that ReadResponse() can return (via SendCommand) to the API function.
	virtual Error PostWriteHook();
	virtual Error PreReadHook(CommandBuffer *read_result);

 protected:
	CommandBuffer expected_written_command;
	CommandBuffer actual_written_command;
	CommandBuffer read_command;
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

}  // namespace qfsclient

#endif  // QFSCLIENT_QFS_CLIENT_TEST_H_
