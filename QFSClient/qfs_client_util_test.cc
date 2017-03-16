// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "QFSClient/qfs_client_util.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace qfsclient {

class QfsClientUtilTest : public testing::Test {
 protected:
	static void SetUpTestCase();
	static void TearDownTestCase();

	void RandomiseBlock(byte *block, size_t size);
};

void QfsClientUtilTest::SetUpTestCase() {
}

void QfsClientUtilTest::TearDownTestCase() {
}

TEST_F(QfsClientUtilTest, SplitTest) {
	std::vector<std::string> tokens;

	// test basic case
	std::string string_to_split("dogs/cats/turtles");

	util::Split(string_to_split, "/", &tokens);

	ASSERT_EQ(tokens.size(), 3);
	ASSERT_EQ(tokens.at(0), "dogs");
	ASSERT_EQ(tokens.at(1), "cats");
	ASSERT_EQ(tokens.at(2), "turtles");

	string_to_split.clear();
	util::Split(string_to_split, "/", &tokens);

	ASSERT_EQ(tokens.size(), 0);

	// test with multiple leading and intermediate delimiters
	string_to_split = ("//one///two/three////four//");

	util::Split(string_to_split, "/", &tokens);

	ASSERT_EQ(tokens.size(), 4);
	ASSERT_EQ(tokens.at(0), "one");
	ASSERT_EQ(tokens.at(1), "two");
	ASSERT_EQ(tokens.at(2), "three");
	ASSERT_EQ(tokens.at(3), "four");

	// test with no delimiters and one token
	string_to_split = ("donkeys");

	util::Split(string_to_split, "/", &tokens);

	ASSERT_EQ(tokens.size(), 1);
	ASSERT_EQ(tokens.at(0), "donkeys");

	// test with empty string
	string_to_split.clear();

	util::Split(string_to_split, "/", &tokens);

	ASSERT_EQ(tokens.size(), 0);
}

TEST_F(QfsClientUtilTest, JoinTest) {
	std::vector<std::string> tokens{ "dogs", "cats", "turtles" };
	std::string expected("dogs/cats/turtles");
	std::string actual;

	util::Join(tokens, "/", &actual);

	ASSERT_STREQ(expected.c_str(), actual.c_str());

	tokens.clear();
	expected.clear();

	util::Join(tokens, "/", &actual);

	ASSERT_STREQ(expected.c_str(), actual.c_str());
}

// Test base64 encoding of a short block of data
TEST_F(QfsClientUtilTest, Base64EncodeTest) {
	std::string b64;
	std::vector<byte> data;

	const char *data_value = "Mary had a little lamb.\001\002\003";
	data.assign(data_value, data_value + strlen(data_value));

	util::base64_encode(data, &b64);

	ASSERT_STREQ("TWFyeSBoYWQgYSBsaXR0bGUgbGFtYi4BAgM=", b64.c_str());
}

// Test base64 encoding of a short base64 value
TEST_F(QfsClientUtilTest, Base64DecodeTest) {
	std::string b64;
	std::vector<byte> data, result;

	b64.assign("TWFyeSBoYWQgYSBsaXR0bGUgbGFtYi4BAgM=");

	const char *data_value = "Mary had a little lamb.\001\002\003";
	data.assign(data_value, data_value + strlen(data_value));

	util::base64_decode(b64, &result);

	ASSERT_EQ(data.size(), result.size());
	ASSERT_EQ(memcmp(data.data(), result.data(), data.size()), 0);
}

void QfsClientUtilTest::RandomiseBlock(byte *block, size_t size) {
	unsigned int seed = time(NULL);
	srand(seed);
	for (size_t i = 0; i < size; i++) {
		block[i] = rand_r(&seed);
	}
}

// Test base64 encoding and decoding of a larger block of random data
TEST_F(QfsClientUtilTest, Base64BigTest) {
	byte block[1024];
	QfsClientUtilTest::RandomiseBlock(block, 1024);

	std::string b64;
	std::vector<byte> data, result;

	data.assign(block, block + 1024);

	util::base64_encode(data, &b64);
	util::base64_decode(b64, &result);

	ASSERT_EQ(data.size(), result.size());
	ASSERT_EQ(memcmp(data.data(), result.data(), data.size()), 0);
}

// Negative test for base64 encoding and decoding
TEST_F(QfsClientUtilTest, Base64BigBadTest) {
	byte block[1024];
	QfsClientUtilTest::RandomiseBlock(block, 1024);

	std::string b64;
	std::vector<byte> data, result;

	data.assign(block, block + 1024);

	util::base64_encode(data, &b64);

	// change the base64 string (but keep it a valid base64 string) before
	// trying to decode it
	b64[123] = (b64[123] == 'r' ? 'R' : 'r');

	util::base64_decode(b64, &result);

	ASSERT_EQ(data.size(), result.size());
	ASSERT_NE(memcmp(data.data(), result.data(), data.size()), 0);
}

}  // namespace qfsclient
