// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#ifndef QFS_CLIENT_DATA_H
#define QFS_CLIENT_DATA_H

// These enums (CommandID and CommandError) are based on their master
// definitions in quantumfs/cmds.go
enum CommandID {
	kCmdError = 0,
	kCmdBranchRequest,
	kCmdGetAccessed,	// not in aid/3015
	kCmdClearAccessed,	// not in aid/3015
	kCmdSyncAll,		// not in aid/3015
	kCmdInsertInode,
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

#endif // QFS_CLIENT_DATA_H

