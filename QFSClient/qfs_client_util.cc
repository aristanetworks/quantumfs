// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "qfs_client_util.h"

#include <algorithm>

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

std::string getErrorMessage(ErrorCode code, const std::string &details) {
	switch (code) {
	case kSuccess:
		return "success";
	case kDontKnowCwd:
		return "couldn't determine current working directory";
	case kCantFindApiFile:
		return "couldn't find API file (started looking at " +
		details + ")";
	case kCantOpenApiFile:
		return "couldn't open API file " + details;
	case kApiFileNotOpen:
		return "call Api.Open() before calling API functions";
	case kApiFileSeekFail:
		return "couldn't seek within API file " + details;
	case kApiFileWriteFail:
		return "couldn't write to API file " + details;
	case kApiFileFlushFail:
		return "couldn't flush API file " + details;
	case kApiFileReadFail:
		return "couldn't read from API file " + details;
	case kWorkspacePathInvalid:
		return "there must be exactly one '/' in '" + details + "'";
	case kJsonEncodingError:
		return "couldn't encode the request into JSON (" +
		details + ")";
	case kJsonDecodingError:
		return "couldn't parse the response as JSON (" +
		details + ")";
	case kMissingJsonObject:
		return "an expected JSON object was missing: " + details;
	case kApiError:
		return "the API returned an error: " + details;
	case kBufferTooBig:
		return "An internal buffer is getting too big";
	}

	std::string result("unknown error (");
	result += code;
	result += ")";

	return result;
}

Error getError(ErrorCode code, const std::string &details) {
	return Error { code, getErrorMessage(code, details) };
}

std::string getApiError(CommandError code, std::string message) {
	switch (code) {
	case kCmdOk:
		return "command successful";
	case kCmdBadArgs:
		return "the argument is wrong (" + message + ")";
	case kCmdBadJson:
		return "failed to parse command (" + message + ")";
	case kCmdBadCommandId:
		return "unknown command ID (" + message + ")";
	case kCmdCommandFailed:
		return "command failed (" + message + ")";
	case kCmdKeyNotFound:
		return "extended key not found in datastore (" + message + ")";
	}

	std::string result("unrecognised error code (");
	result += code;
	result += ")";

	return result;
}

// Call perror() with a formatted string containing the name of the calling function
// and further information (such as the name of the operation that triggered the
// error).
void fperror(std::string function_name, std::string detail) {
	std::string full_message = function_name + "() (" + detail + ")";
	perror(full_message.c_str());
}

// Build an error string from an existing error string but with a related JSON
// string added for debugging context
std::string buildJsonErrorDetails(const std::string &error, const char *json) {
	return error + " (JSON: " + std::string(json) +")";
}

// replace all single quotes in the given string with double quotes. This is
// to make string literals containing JSON more readable, since double quote
// characters are very common in JSON but need to be escaped in string literals.
void requote(std::string &s) {
	std::replace(s.begin(), s.end(), '\'', '"');
}

// Implementation of ApiContext. Instances of this class should be created on the
// stack so that useful cleanup happens automatically.
JsonApiContext::JsonApiContext() : ApiContext(), json_object(NULL) {

}

JsonApiContext::~JsonApiContext() {
	SetJsonObject(NULL);
}

void JsonApiContext::SetJsonObject(json_t *json_object) {
	if (this->json_object) {
		json_decref(this->json_object);
		this->json_object = NULL;
	}

	this->json_object = json_object;
}

json_t *JsonApiContext::GetJsonObject() const {
	return json_object;
}

} // namespace util
} // namespace qfsclient

