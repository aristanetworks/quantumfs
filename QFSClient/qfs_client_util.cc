// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "QFSClient/qfs_client_util.h"

#include <openssl/bio.h>
#include <openssl/buffer.h>
#include <openssl/evp.h>

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

		if (token_end == std::string::npos) {
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
		return "there must be at least two '/' in '" + details + "'";
	case kWorkspaceNameInvalid:
		return "there must be exactly two '/' in '" + details + "'";
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
		return "an internal buffer is getting too big";
	case kJsonObjectWrongType:
		return "a JSON object had the wrong type: " + details;
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
	case kErrorBlockTooLarge:
		return "SetBlock was passed a block that was too large: (" +
			message + ")";
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
std::string buildJsonErrorDetails(const std::string &error,
				  const char *json,
				  size_t json_length) {
	return error + " (JSON: " + std::string(json, json_length) +")";
}

// replace all single quotes in the given string with double quotes. This is
// to make string literals containing JSON more readable, since double quote
// characters are very common in JSON but need to be escaped in string literals.
void requote(std::string *s) {
	std::replace(s->begin(), s->end(), '\'', '"');
}

void base64_encode(const std::vector<byte> &data, std::string *b64) {
	b64->clear();

	BIO *bio = BIO_new(BIO_f_base64());
	BIO *bio_mem = BIO_new(BIO_s_mem());

	if(!bio || !bio_mem) {
		return;
	}

	// we have no need for linebreaks in the generated base64 text
	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);

	bio = BIO_push(bio, bio_mem);

	if (BIO_write(bio, data.data(), data.size()) == data.size()) {
		BIO_flush(bio);
		BUF_MEM *result;
		BIO_get_mem_ptr(bio, &result);
		b64->assign((const char *)result->data, (size_t)result->length);
	}

	BIO_free_all(bio);
}

void base64_decode(const std::string &b64, std::vector<byte> *data) {
	data->clear();

	int max_result_size = ((b64.length() * 6) + 7) / 8;
	data->resize(max_result_size);

	BIO *bio = BIO_new(BIO_f_base64());
	BIO *bio_mem = BIO_new_mem_buf(const_cast<char*>(b64.c_str()),
	                               b64.length() + 1);
	if(!bio || !bio_mem) {
		return;
	}

	// if we don't set this flag, the decoder will expect at least a
	// terminating linebreak in the input string
	BIO_set_flags(bio, BIO_FLAGS_BASE64_NO_NL);

	bio = BIO_push(bio, bio_mem);

	int actual_result_size = BIO_read(bio, data->data(), max_result_size);
	data->resize(actual_result_size);

	BIO_free_all(bio);
}

}  // namespace util
}  // namespace qfsclient
