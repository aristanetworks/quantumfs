// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The datastore interface
package quantumfs

import "fmt"
import "time"

import "crypto/sha1"
import "encoding/json"
import "encoding/hex"
import "github.com/aristanetworks/quantumfs/qlog"

// Maximum size of a block which can be stored in a datastore
const MaxBlockSize = 1 * 1024 * 1024

// Maximum number of blocks for each file type (determined by the encoding, which is
// JSON currently, but will change. These should be updated then)
const MaxBlocksMediumFile = 32
// TODO: Increase these to 48000 when we choose a more efficient encoding than json
const MaxBlocksLargeFile = 22000

// Maximum length of a filename
const MaxFilenameLength = 256

// Special reserved namespace/workspace names
const (
	ApiPath           = "api" // File used for the qfs api
	NullNamespaceName = "_null"
	NullWorkspaceName = "null"
)

// Special reserved inode numbers
const (
	_                  = iota // Invalid
	InodeIdRoot        = iota // Same as fuse.FUSE_ROOT_ID
	InodeIdApi         = iota // /api file
	InodeId_null       = iota // /_null namespace
	InodeId_nullNull   = iota // /_null/null workspace
	InodeIdReservedEnd = iota // End of the reserved range
)

// Object key types, possibly used for datastore routing
const (
	KeyTypeConstant  = iota // A statically known object, such as the empty block
	KeyTypeOther     = iota // A nonspecific type
	KeyTypeMetadata  = iota // Metadata block ie a directory or file descrptor
	KeyTypeBuildable = iota // A block generated by a build
	KeyTypeData      = iota // A block generated by a user
	KeyTypeVCS       = iota // A block which is backed by a VCS
)

// One of the KeyType* values above
type KeyType uint8

// The size of the object ID is determined by a number of bytes sufficient to contain
// the identification hashes used by all the backing stores (most notably the VCS
// such as git or Mercurial) and additional space to be used for datastore routing.
//
// In this case we use a 20 byte hash sufficient to store sha1 values and one
// additional byte used for routing.
const ObjectKeyLength = 1 + sha1.Size

func NewObjectKey(type_ KeyType, hash [ObjectKeyLength - 1]byte) ObjectKey {
	key := ObjectKey{}
	key.Key[0] = byte(type_)
	for i := 1; i < ObjectKeyLength; i++ {
		key.Key[i] = hash[i-1]
	}
	return key
}

type ObjectKey struct {
	Key [ObjectKeyLength]byte
}

// Extract the type of the object. Returns a KeyType*
func (key *ObjectKey) Type() byte {
	return key.Key[0]
}

func (key ObjectKey) String() string {
	hex := hex.EncodeToString(key.Key[:])

	return "Key: " + hex
}

type DirectoryEntry struct {
	NumEntries uint32
	Entries    []DirectoryRecord
}

func NewDirectoryEntry(capacity int) *DirectoryEntry {
	var dirEntry DirectoryEntry
	dirEntry.Entries = make([]DirectoryRecord, 0, capacity)

	return &dirEntry
}

// The various types the next referenced object could be
const (
	ObjectTypeBuildProduct      = iota
	ObjectTypeDirectoryEntry    = iota
	ObjectTypeExtendedAttribute = iota
	ObjectTypeHardlink          = iota
	ObjectTypeSymlink           = iota
	ObjectTypeVCSFile           = iota
	ObjectTypeWorkspaceRoot     = iota
	ObjectTypeSmallFile         = iota
	ObjectTypeMediumFile        = iota
	ObjectTypeLargeFile         = iota
	ObjectTypeVeryLargeFile     = iota
)

// One of the ObjectType* values
type ObjectType uint8

// Quantumfs doesn't keep precise ownership values. Instead files and directories may
// be owned by some special system accounts or the current user. The translation to
// UID is done at access time.
const (
	UIDRoot = iota
	UIDUser = iota // The currently accessing user
)

// Convert object UID to system UID.
//
// userId is the UID of the current user
func SystemUid(uid UID, userId uint32) uint32 {
	switch uid {
	case UIDRoot:
		return 0
	case UIDUser:
		return userId
	default:
		return 0
	}
}

// Convert system UID to object UID
//
// userId is the UID of the current user
func ObjectUid(c Ctx, uid uint32, userId uint32) UID {
	if uid == userId {
		return UIDUser
	}

	switch uid {
	case 0:
		return UIDRoot
	default:
		c.Elog(qlog.LogDatastore, "Unknown UID %d", uid)
		return UIDUser
	}
}

// One of the UID* values
type UID uint8

// Similar to the UIDs above, group ownership is divided into special classes.
const (
	GIDRoot = iota
	GIDUser = iota // The currently accessing user
)

// Convert object GID to system GID.
//
// userId is the GID of the current user
func SystemGid(gid GID, userId uint32) uint32 {
	switch gid {
	case GIDRoot:
		return 0
	case GIDUser:
		return userId
	default:
		return 0
	}
}

// Convert system GID to object GID
//
// userId is the GID of the current user
func ObjectGid(c Ctx, gid uint32, userId uint32) GID {
	if gid == userId {
		return GIDUser
	}

	switch gid {
	case 0:
		return GIDRoot
	default:
		c.Elog(qlog.LogDatastore, "Unknown GID", gid)
		return GIDUser
	}
}

// One of the GID* values
type GID uint8

// Quantumfs stores time in microseconds since the Unix epoch
type Time uint64

func (t *Time) Seconds() uint64 {
	return uint64(*t / 1000000)
}

func (t *Time) Nanoseconds() uint32 {
	return uint32(*t % 1000000)
}

func NewTime(instant time.Time) Time {
	t := instant.Unix() * 1000000
	t += int64(instant.Nanosecond() / 1000)

	return Time(t)
}

func NewTimeSeconds(seconds uint64, nanoseconds uint32) Time {
	t := seconds * 1000000
	t += uint64(nanoseconds / 1000)

	return Time(t)
}

type DirectoryRecord struct {
	Filename           [MaxFilenameLength]byte
	ID                 ObjectKey
	Type               ObjectType
	Permissions        uint8
	Owner              UID
	Group              GID
	Size               uint64
	ExtendedAttributes ObjectKey
	CreationTime       Time
	ModificationTime   Time
}

var EmptyDirKey ObjectKey

func createEmptyDirectory() ObjectKey {
	emptyDir := DirectoryEntry{
		NumEntries: 0,
		Entries:    make([]DirectoryRecord, 0),
	}

	bytes, err := json.Marshal(emptyDir)
	if err != nil {
		panic("Failed to marshal empty directory")
	}

	hash := sha1.Sum(bytes)
	emptyDirKey := NewObjectKey(KeyTypeConstant, hash)
	constStore.store[emptyDirKey] = bytes
	return emptyDirKey
}

var EmptyBlockKey ObjectKey

func createEmptyBlock() ObjectKey {
	var bytes []byte

	hash := sha1.Sum(bytes)
	emptyBlockKey := NewObjectKey(KeyTypeConstant, hash)
	constStore.store[emptyBlockKey] = bytes
	return emptyBlockKey
}

type WorkspaceRoot struct {
	BaseLayer  ObjectKey
	VCSLayer   ObjectKey
	BuildLayer ObjectKey
	UserLayer  ObjectKey
}

var EmptyWorkspaceKey ObjectKey

func createEmptyWorkspace(emptyDirKey ObjectKey) ObjectKey {
	emptyWorkspace := WorkspaceRoot{
		BaseLayer:  emptyDirKey,
		VCSLayer:   emptyDirKey,
		BuildLayer: emptyDirKey,
		UserLayer:  emptyDirKey,
	}

	bytes, err := json.Marshal(emptyWorkspace)
	if err != nil {
		panic("Failed to marhal empty workspace")
	}

	hash := sha1.Sum(bytes)
	emptyWorkspaceKey := NewObjectKey(KeyTypeConstant, hash)
	constStore.store[emptyWorkspaceKey] = bytes
	return emptyWorkspaceKey
}

type Buffer struct {
	data []byte
}

func (buf *Buffer) Set(in []byte) {
	buf.data = in
}

func (buf *Buffer) Write(in []byte, offset uint32) uint32 {
	// Sanity check offset and length
	maxWriteLen := MaxBlockSize - int(offset)
	if maxWriteLen <= 0 {
		return 0
	}

	if len(in) > maxWriteLen {
		in = in[:maxWriteLen]
	}

	// Ensure that our data ends where we need it to. This allows us to write
	// past the end of a block, but not past the block's max capacity
	deltaLen := int(offset) - len(buf.data)
	if deltaLen > 0 {
		buf.data = append(buf.data, make([]byte, deltaLen)...)
	}

	var finalBuffer []byte
	// append our write data to the first split of the existing data
	finalBuffer = append(buf.data[:offset], in...)

	// record how much was actually appended (in case len(in) < size)
	copied := uint32(len(finalBuffer)) - uint32(offset)

	// then add on the rest of the existing data afterwards, excluding the amount
	// that we just wrote (to overwrite instead of insert)
	remainingStart := offset + copied
	if int(remainingStart) < len(buf.data) {
		finalBuffer = append(finalBuffer, buf.data[remainingStart:]...)
	}

	buf.data = finalBuffer

	return copied
}

func (buf *Buffer) Get() []byte {
	return buf.data
}

func (buf *Buffer) ContentHash() [ObjectKeyLength - 1]byte {
	return sha1.Sum(buf.data)
}

func (buf *Buffer) Key(keyType KeyType) ObjectKey {
	return NewObjectKey(keyType, buf.ContentHash())
}

func NewBuffer(in []byte) *Buffer {
	return &Buffer{
		data: in,
	}
}

type DataStore interface {
	Get(key ObjectKey, buffer *Buffer) error
	Set(key ObjectKey, buffer *Buffer) error
	Exists(key ObjectKey) bool
}

// A pseudo-store which contains all the constant objects
var constStore = newConstantStore()
var ConstantStore = DataStore(constStore)

func newConstantStore() *ConstDataStore {
	return &ConstDataStore{
		store: make(map[ObjectKey][]byte),
	}
}

type ConstDataStore struct {
	store map[ObjectKey][]byte
}

func (store *ConstDataStore) Get(key ObjectKey, buffer *Buffer) error {
	if data, ok := store.store[key]; ok {
		buffer.Set(data)
		return nil
	}
	return fmt.Errorf("Object not found")
}

func (store *ConstDataStore) Set(key ObjectKey, buffer *Buffer) error {
	return fmt.Errorf("Cannot set in constant datastore")
}

func (store *ConstDataStore) Exists(key ObjectKey) bool {
	return false
}

func init() {
	emptyDirKey := createEmptyDirectory()
	emptyBlockKey := createEmptyBlock()
	emptyWorkspaceKey := createEmptyWorkspace(emptyDirKey)
	EmptyDirKey = emptyDirKey
	EmptyBlockKey = emptyBlockKey
	EmptyWorkspaceKey = emptyWorkspaceKey
}
