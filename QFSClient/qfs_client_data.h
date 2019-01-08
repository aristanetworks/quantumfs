// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

#ifndef QFSCLIENT_QFS_CLIENT_DATA_H_
#define QFSCLIENT_QFS_CLIENT_DATA_H_

// These enums (CommandID and CommandError) are based on their master
// definitions in quantumfs/cmds.go
enum CommandID {
	kCmdInvalid = 0,
	kCmdError = 1,
	kCmdBranchRequest = 2,
	kCmdGetAccessed = 3,    // not in aid/3015
	kCmdClearAccessed = 4,  // not in aid/3015
	kCmdSyncAll = 5,        // not in aid/3015
	kCmdInsertInode = 6,
	kCmdDeleteWorkspace = 7,
	kCmdSetBlock = 8,
	kCmdGetBlock = 9,
	kCmdEnableRootWrite = 10,
};

enum CommandError {
	// Command Successful
	kCmdOk = 0,

	// The argument is wrong
	kCmdBadArgs = 1,

	// Failed to parse command
	kCmdBadJson = 2,

	// Unknown command ID
	kCmdBadCommandId = 3,

	// The Command failed, see the error for more info
	kCmdCommandFailed = 4,

	// The extended key is not stored in the datastore
	kCmdKeyNotFound = 5,

	// SetBlock was passed a block that was too large
	kErrorBlockTooLarge = 6,
};

// names of commonly-used JSON fields
static const char kCommandId[] = "CommandId";
static const char kWorkspaceRoot[] = "WorkspaceRoot";
static const char kWorkspacePath[] = "WorkspacePath";
static const char kErrorCode[] = "ErrorCode";
static const char kMessage[] = "Message";
static const char kPathList[] = "PathList";
static const char kPaths[] = "Paths";
static const char kDstPath[] = "DstPath";
static const char kKey[] = "Key";
static const char kData[] = "Data";
static const char kUid[] = "Uid";
static const char kGid[] = "Gid";
static const char kPermissions[] = "Permissions";
static const char kSource[] = "Src";
static const char kDestination[] = "Dst";

// from datastore.go:
// base64 consume more memory than daemon.sourceDataLength: 30 * 4 / 3
const int kExtendedKeyLength = 40;

// format strings (as used by json_pack_ex() for building JSON) used whenever we
// need to build a JSON string.
// See http://jansson.readthedocs.io/en/2.4/apiref.html#building-values for
// an explanation of the format strings that json_pack_ex() can take.
// These must match the structures in quantumfs/cmds.go
static const char kGetAccessedJSON[] = "{s:i,s:s}";
static const char kInsertInodeJSON[] = "{s:i,s:s,s:s,s:i,s:i,s:i}";
static const char kBranchJSON[] = "{s:i,s:s,s:s}";
static const char kDeleteJSON[] = "{s:i,s:s}";
static const char kSetBlockJSON[] = "{s:i,s:s,s:s}";
static const char kGetBlockJSON[] = "{s:i,s:s}";

#endif  // QFSCLIENT_QFS_CLIENT_DATA_H_

