// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <gtest/gtest.h>

#include "qfs_client.h"
#include "qfs_client_util.h"

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

TEST_F(QfsClientTest, SendCommandFileRemovedTest) {
	ASSERT_FALSE(this->api == NULL);

	const Api::CommandBuffer send { "sausages",  sizeof("sausages") };
	Api::CommandBuffer result;

	// delete the API file after it's been used once, then try to send a command
	this->api->SendCommand(send, &result);
	unlink(this->api_path.c_str());
	Error err = this->api->SendCommand(send, &result);

	ASSERT_EQ(err.code, kApiFileSeekFail);
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

class QfsClientDeterminePathTest : public QfsClientTest {
	virtual void SetUp();
	virtual void TearDown();
};

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
	ASSERT_STREQ(this->api->path.c_str(),
		     this->api_path.c_str());

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

