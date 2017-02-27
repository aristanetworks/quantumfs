# Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

@0xbf1bd07c841779d2;

using Go = import "../../../../github.com/glycerine/go-capnproto/go.capnp";
$Go.package("encoding");
$Go.import("github.com/aristanetworks.com/quantumfs/encoding");

# Maximum size of a block which can be stored in a datastore
const maxBlockSize :UInt32 = 1048576;

# Maximum length of a filename
const maxFilenameLength :UInt32 = 256;

# Maximum length of an extended attribute
const maxXAttrnameLength :UInt32 = 256;

struct ObjectKey {
        keyType @0 :UInt8; # 1
        part2   @1 :UInt64; # 9
        part3   @2 :UInt64; # 17
        part4   @3 :UInt32;  # 21
}

struct DirectoryRecord {
        filename           @0 :Text;
        id                 @1 :ObjectKey;
        type               @2 :UInt8;
        permissions        @3 :UInt32;
        owner              @4 :UInt16;
        group              @5 :UInt16;
        size               @6 :UInt64;
        extendedAttributes @7 :ObjectKey;
        contentTime        @8 :UInt64;
        modificationTime   @9 :UInt64;
}

struct DirectoryEntry {
        numEntries @0 :UInt32;
        next       @1 :ObjectKey;
        entries    @2 :List(DirectoryRecord);
}

struct HardlinkRecord {
	hardlinkID @0 :UInt64;
	record     @1 :DirectoryRecord;
	nlinks     @2 :UInt32;
}

struct HardlinkEntry {
	numEntries @0 :UInt32;
	next       @1 :ObjectKey;
	entries    @2 :List(HardlinkRecord);
}

struct WorkspaceRoot {
        baseLayer  @0 :ObjectKey;
        vcsLayer   @1 :ObjectKey;
        buildLayer @2 :ObjectKey;
        userLayer  @3 :ObjectKey;

	hardlinkEntry @4 :HardlinkEntry;
}

struct VeryLargeFile {
        numberOfParts @0 :UInt32;
        largeFileKeys @1 :List(ObjectKey);
}

struct MultiBlockFile {
        blockSize       @0 :UInt32;
        numberOfBlocks  @1 :UInt32;
        sizeOfLastBlock @2 :UInt32;
        listOfBlocks    @3 :List(ObjectKey);
}

struct ExtendedAttribute {
        name @0 :Text;
        id   @1 :ObjectKey;
}

struct ExtendedAttributes {
        numAttributes    @0 :UInt32;
        attributes @1 :List(ExtendedAttribute);
}
