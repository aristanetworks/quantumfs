// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "qfs_client_util.h"

namespace qfsclient {

class QfsClientUtilTest : public testing::Test {
 protected:
	static void SetUpTestCase();
	static void TearDownTestCase();
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

} // namespace qfsclient
