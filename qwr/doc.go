// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

/*
 Package qwr provides routines for writing/reading entities like files (of
 different types like device files, symlinks, hardlinks etc) and directories
 into QuantumFS supported datastores. Using this package, one can build
 tools that write and read filesystem entities from QFS. For example -
 a tool that uploads a directory hierarchy to QFS supported datastore.
 The routines in this package, do not depend on a QFS instance to be
 available. They rely on QFS datastore and workspaceDB APIs to interact
 directly with the storage.

 This package abstracts away the QFS specific storage metadata structures and
 encoding schemes from the tool developer. This package relies on QFS to export
 such information.
*/
package qwr
