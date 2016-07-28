# Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
# Arista Networks, Inc. Confidential and Proprietary.

@0xbf1bd07c841779d2;

using Go = import "../../../../github.com/glycerine/go-capnproto/go.capnp";
$Go.package("encoding");
$Go.import("github.com/aristanetworks.com/quantumfs/encoding");

# Maximum size of a block which can be stored in a datastore
const maxBlockSize :UInt32 = 1048576;

# Maximum number of blocks for each file type
const maxBlocksMediumFile :UInt32 = 32;

# TODO: Increase these to 48000 when we choose a more efficient encoding than json
const maxBlocksLargeFile :UInt32 = 22000;

# TODO: Increase this to 48000 when we switch away from json
const maxPartsVeryLargeFile :UInt32 = 22000;

const maxDirectoryRecords :UInt32 = 1200;

# Maximum length of a filename
const maxFilenameLength :UInt32 = 256;

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
        owner              @4 :UInt8;
        group              @5 :UInt8;
        size               @6 :UInt64;
        extendedAttributes @7 :ObjectKey;
        creationTime       @8 :UInt64;
        modificationTime   @9 :UInt64;
}

struct DirectoryEntry {
        numEntries @0 :UInt32;
        next       @1 :ObjectKey;
        entries    @2 :List(DirectoryRecord);
}

struct WorkspaceRoot {
        baseLayer  @0 :ObjectKey;
        vcsLayer   @1 :ObjectKey;
        buildLayer @2 :ObjectKey;
        userLayer  @3 :ObjectKey;
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
