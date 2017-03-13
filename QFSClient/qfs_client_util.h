// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFSCLIENT_QFS_CLIENT_UTIL_H_
#define QFSCLIENT_QFS_CLIENT_UTIL_H_

#include <jansson.h>

#include <string>
#include <vector>

#include "QFSClient/qfs_client.h"
#include "QFSClient/qfs_client_implementation.h"
#include "QFSClient/qfs_client_data.h"

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

// Given an error code (and optional further information about the error),
// return a useful string describing the error
std::string getErrorMessage(ErrorCode code, const std::string &details);

// Given an error code (and optional further information about the error),
// return an Error struct containing both
Error getError(ErrorCode code, const std::string &details = "");

// Given a CommandError code (returned from QuantumFS) and a message string
// (also returned from QuantumFS), return a useful string describing the error
std::string getApiError(CommandError code, std::string message);

// Call perror() with a formatted string containing the name of the calling function
// and further information (such as the name of the operation that triggered the
// error).
void fperror(std::string function_name, std::string detail);

// Build an error string from an existing error string but with a related JSON
// string added for debugging context
std::string buildJsonErrorDetails(const std::string &error, const char *json);

// replace all single quotes in the given string with double quotes. This is
// to make string literals containing JSON more readable, since double quote
// characters are very common in JSON but need to be escaped in string literals.
void requote(std::string *s);

}  // namespace util
}  // namespace qfsclient

#endif  // QFSCLIENT_QFS_CLIENT_UTIL_H_

