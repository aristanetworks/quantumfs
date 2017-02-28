// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_CLIENT_DATA_H
#define QFS_CLIENT_DATA_H

// These enums (CommandID and CommandError) are based on their master
// definitions in quantumfs/cmds.go
enum CommandID {
	kCmdError = 0,
	kCmdBranchRequest = 1,
	kCmdGetAccessed = 2,	// not in aid/3015
	kCmdClearAccessed = 3,	// not in aid/3015
	kCmdSyncAll = 4,	// not in aid/3015
	kCmdInsertInode = 5,
	// missing: Merge, Freeze, Checkout, WorkspaceDelete, GetBlock, SetBlock
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
};

// names of commonly-used JSON fields
static const char kCommandId[] = "CommandId";
static const char kWorkspaceRoot[] = "WorkspaceRoot";
static const char kErrorCode[] = "ErrorCode";
static const char kMessage[] = "Message";
static const char kAccessList[] = "AccessList";
static const char kDstPath[] = "DstPath";
static const char kKey[] = "Key";
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
static const char kGetAccessedJSON[] = "{s:i,s:s}";
static const char kInsertInodeJSON[] = "{s:i,s:s,s:s,s:i,s:i,s:i}";
static const char kBranchJSON[] = "{s:i,s:s,s:s}";

#endif // QFS_CLIENT_DATA_H

