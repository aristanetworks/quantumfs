// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "qfs_client_util.h"

namespace qfsclient {
namespace util {

// Tokenise the given string (with tokens separated by one of the characters
// in delimiters) and place the tokens in the given vector.
void Split(const std::string &str,
	   const std::string &delimiters,
	   std::vector<std::string> *tokens) {
	tokens->clear();

	// get the position of the first character after any initial delimiters
	size_t token_start = str.find_first_not_of(delimiters, 0);
	size_t token_end;

	// loop as long as there are any tokens...
	while (token_start != std::string::npos) {
		// get the position of the first delimiter after the token
		token_end = str.find_first_of(delimiters, token_start);

		if (token_end == std::string::npos ) {
			// no next delimiter? save this token and exit the loop
			tokens->push_back(str.substr(token_start));
			break;
		}

		tokens->push_back(str.substr(token_start, token_end - token_start));

		// get the position of the next token after any more delimiters
		token_start = str.find_first_not_of(delimiters, token_end);
	}
}

// Glue the strings in a vector together into a single string, each part
// separated by the string in glue.
void Join(const std::vector<std::string> &pieces,
	  const std::string &glue,
	  std::string *result) {
	result->clear();

	for (auto piece : pieces) {
		if (!result->empty()) {
			*result += glue;
		}

		*result += piece;
	}
}

// Call perror() with a formatted string containing the name of the calling function
// and further information (such as the name of the operation that triggered the
// error).
void fperror(std::string function_name, std::string detail) {
	std::string full_message = function_name + "() (" + detail + ")";
	perror(full_message.c_str());
}

} // namespace util
} // namespace qfsclient

