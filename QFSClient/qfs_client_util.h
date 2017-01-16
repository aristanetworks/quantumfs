// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_CLIENT_UTIL_H_
#define QFS_CLIENT_UTIL_H_

#include <string>
#include <vector>

namespace qfsclient {
namespace util {

// Tokenise the given string (with tokens separated by one of the characters
// in delimiters) and place the tokens in the given vector.
void Split(const std::string &str,
	   const std::string &delimiters,
	   std::vector<std::string> *tokens);

// Glue the strings in a vector together into a single string, each part
// separated by the string in glue.
void Join(const std::vector<std::string> &pieces,
	  const std::string &glue,
	  std::string *result);

// Call perror() with a formatted string containing the name of the calling function
// and further information (such as the name of the operation that triggered the
// error).
void fperror(std::string function_name, std::string detail);

} // namespace util
} // namespace qfsclient

#endif // QFS_CLIENT_UTIL_H_

