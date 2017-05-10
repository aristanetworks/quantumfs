// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The datastore interface
package quantumfs

import "encoding/base64"
import "encoding/binary"
import "encoding/hex"
import "fmt"
import "sort"
import "time"

import "github.com/aristanetworks/quantumfs/encoding"
import capn "github.com/glycerine/go-capnproto"

// 160 bit hash, must match hash.HashSize
const HashSize = 20

// Maximum size of a block which can be stored in a datastore
const MaxBlockSize = int(encoding.MaxBlockSize)

// The size of the ObjectKey: 21 + 1 + 8
// The length decides the length in datastore.go: quantumfs.ExtendedKeyLength
const sourceDataLength = 30

// Maximum length of a filename
const MaxFilenameLength = int(encoding.MaxFilenameLength)
const MaxXAttrnameLength = int(encoding.MaxXAttrnameLength)

// Maximum number of blocks for each file type
var maxBlocksMediumFile int
var maxBlocksLargeFile int
var maxPartsVeryLargeFile int
var maxDirectoryRecords int
var maxNumExtendedAttributes int

// accessors for max blocks information per file type
func MaxBlocksMediumFile() int {
	return maxBlocksMediumFile
}

func MaxBlocksLargeFile() int {
	return maxBlocksLargeFile
}

func MaxPartsVeryLargeFile() int {
	return maxPartsVeryLargeFile
}

func MaxDirectoryRecords() int {
	return maxDirectoryRecords
}

func MaxNumExtendedAttributes() int {
	return maxNumExtendedAttributes
}

// max file sizes based on file type
func MaxSmallFileSize() uint64 {
	return uint64(MaxBlockSize)
}

func MaxMediumFileSize() uint64 {
	return uint64(MaxBlocksMediumFile() * MaxBlockSize)
}

func MaxLargeFileSize() uint64 {
	return uint64(MaxBlocksLargeFile() * MaxBlockSize)
}

func MaxVeryLargeFileSize() uint64 {
	return uint64(MaxPartsVeryLargeFile()) * MaxLargeFileSize()
}

// Special reserved file paths
const (
	ApiPath = "api" // File used for the qfs api

	// The reserved typespace/namespace/workspace name for the empty workspace,
	// ie. _/_/_.
	NullSpaceName = "_"
)

// Special reserved inode numbers
const (
	InodeIdInvalid     = 0 // Invalid
	InodeIdRoot        = 1 // Same as fuse.FUSE_ROOT_ID
	InodeIdApi         = 2 // /api file
	InodeId_nullType   = 3 // /_typespace
	InodeId_nullName   = 4 // /_/_namespace
	InodeId_nullWork   = 5 // /_/_/_ workspace
	InodeIdReservedEnd = 6 // End of the reserved range
)

// Object key types, possibly used for datastore routing
const (
	KeyTypeInvalid   = 0 // Invalid
	KeyTypeConstant  = 1 // A statically known object, such as the empty block
	KeyTypeOther     = 2 // A nonspecific type
	KeyTypeMetadata  = 3 // Metadata block ie a directory or file descriptor
	KeyTypeBuildable = 4 // A block generated by a build
	KeyTypeData      = 5 // A block generated by a user
	KeyTypeVCS       = 6 // A block which is backed by a VCS

	// A key which isn't a key and should never be found in a datastore. Instead
	// this value indicates that the rest of the key is embedded data which is
	// interpreted directly.
	KeyTypeEmbedded = 7

	KeyTypeApi = 8 // A key-value pair provided entirely by the api
)

// String names for KeyTypes
func KeyTypeToString(keyType KeyType) string {
	switch keyType {
	default:
		return "Unknown"
	case KeyTypeConstant:
		return "Constant"
	case KeyTypeOther:
		return "Other"
	case KeyTypeMetadata:
		return "Metadata"
	case KeyTypeBuildable:
		return "Buildable"
	case KeyTypeData:
		return "Data"
	case KeyTypeVCS:
		return "VCS"
	case KeyTypeEmbedded:
		return "Embedded"
	}
}

// One of the KeyType* values above
type KeyType uint8

// Convert to primitive type for qlog
func (v KeyType) Primitive() interface{} {
	return uint8(v)
}

// The size of the object ID is determined by a number of bytes sufficient to contain
// the identification hashes used by all the backing stores (most notably the VCS
// such as git or Mercurial) and additional space to be used for datastore routing.
//
// In this case we use a 20 byte hash sufficient to store hash values and one
// additional byte used for routing.
const ObjectKeyLength = 1 + HashSize

// base64 consume more memory than daemon.sourceDataLength: 30 * 4 / 3
const ExtendedKeyLength = 40
const XAttrTypeKey = "quantumfs.key"

type ObjectKey struct {
	key encoding.ObjectKey
}

func NewObjectKey(type_ KeyType, hash [HashSize]byte) ObjectKey {
	segment := capn.NewBuffer(nil)
	key := ObjectKey{
		key: encoding.NewRootObjectKey(segment),
	}

	key.key.SetKeyType(byte(type_))
	key.key.SetPart2(binary.LittleEndian.Uint64(hash[:8]))
	key.key.SetPart3(binary.LittleEndian.Uint64(hash[8:16]))
	key.key.SetPart4(binary.LittleEndian.Uint32(hash[16:]))
	return key
}

func NewObjectKeyFromBytes(bytes []byte) ObjectKey {
	segment := capn.NewBuffer(nil)
	key := ObjectKey{
		key: encoding.NewRootObjectKey(segment),
	}

	key.key.SetKeyType(byte(bytes[0]))
	key.key.SetPart2(binary.LittleEndian.Uint64(bytes[1:9]))
	key.key.SetPart3(binary.LittleEndian.Uint64(bytes[9:17]))
	key.key.SetPart4(binary.LittleEndian.Uint32(bytes[17:]))
	return key
}

func overlayObjectKey(k encoding.ObjectKey) ObjectKey {
	key := ObjectKey{
		key: k,
	}
	return key
}

// Extract the type of the object. Returns a KeyType
func (key ObjectKey) Type() KeyType {
	return KeyType(key.key.KeyType())
}

func (key ObjectKey) String() string {
	hex := fmt.Sprintf("(%s: %016x%016x%08x)", KeyTypeToString(key.Type()),
		key.key.Part2(), key.key.Part3(), key.key.Part4())

	return hex
}

func (key ObjectKey) Bytes() []byte {
	return key.key.Segment.Data
}

func (key ObjectKey) Hash() [ObjectKeyLength - 1]byte {
	var hash [ObjectKeyLength - 1]byte
	binary.LittleEndian.PutUint64(hash[0:8], key.key.Part2())
	binary.LittleEndian.PutUint64(hash[8:16], key.key.Part3())
	binary.LittleEndian.PutUint32(hash[16:20], key.key.Part4())
	return hash
}

func (key ObjectKey) Value() []byte {
	var value [ObjectKeyLength]byte
	value[0] = key.key.KeyType()
	binary.LittleEndian.PutUint64(value[1:9], key.key.Part2())
	binary.LittleEndian.PutUint64(value[9:17], key.key.Part3())
	binary.LittleEndian.PutUint32(value[17:21], key.key.Part4())
	return value[:]
}

func (key ObjectKey) IsEqualTo(other ObjectKey) bool {
	if key.key.KeyType() == other.key.KeyType() &&
		key.key.Part2() == other.key.Part2() &&
		key.key.Part3() == other.key.Part3() &&
		key.key.Part4() == other.key.Part4() {

		return true
	}
	return false
}

func DecodeSpecialKey(key ObjectKey) (fileType uint32, device uint32, err error) {
	if key.Type() != KeyTypeEmbedded {
		return 0, 0, fmt.Errorf("Non-embedded key when decoding " +
			"special file")
	}
	hash := key.Hash()
	fileType = binary.LittleEndian.Uint32(hash[0:4])
	device = binary.LittleEndian.Uint32(hash[4:8])

	return fileType, device, nil
}

func EncodeSpecialKey(fileType uint32, device uint32) ObjectKey {
	var hash [ObjectKeyLength - 1]byte
	binary.LittleEndian.PutUint32(hash[0:4], fileType)
	binary.LittleEndian.PutUint32(hash[4:8], device)
	return NewObjectKey(KeyTypeEmbedded, hash)
}

type DirectoryEntry struct {
	dir encoding.DirectoryEntry
}

func NewDirectoryEntry(entryCapacity int) (remain int, entry *DirectoryEntry) {
	remain = 0
	recs := entryCapacity
	if entryCapacity > MaxDirectoryRecords() {
		recs = MaxDirectoryRecords()
		remain = entryCapacity - recs
	}

	return remain, newDirectoryEntryRecords(recs)
}

func newDirectoryEntryRecords(recs int) *DirectoryEntry {
	segment := capn.NewBuffer(nil)

	dirEntry := DirectoryEntry{
		dir: encoding.NewRootDirectoryEntry(segment),
	}
	dirEntry.dir.SetNumEntries(0)

	recordList := encoding.NewDirectoryRecordList(segment, recs)
	dirEntry.dir.SetEntries(recordList)

	return &dirEntry

}

func OverlayDirectoryEntry(edir encoding.DirectoryEntry) DirectoryEntry {
	dir := DirectoryEntry{
		dir: edir,
	}
	return dir
}

type sortDirRecordsByName struct {
	de *DirectoryEntry
}

func (s sortDirRecordsByName) Len() int {
	return int(s.de.dir.NumEntries())
}

func (s sortDirRecordsByName) Swap(i, j int) {
	oldi := overlayDirectoryRecord(s.de.dir.Entries().At(i)).ShallowCopy()
	curj := overlayDirectoryRecord(s.de.dir.Entries().At(j)).ShallowCopy()
	s.de.dir.Entries().Set(i, curj.Record().record)
	s.de.dir.Entries().Set(j, oldi.Record().record)
}

func (s sortDirRecordsByName) Less(i, j int) bool {
	return s.de.dir.Entries().At(i).Filename() <
		s.de.dir.Entries().At(j).Filename()
}

func (dir *DirectoryEntry) SortRecordsByName() {
	sort.Sort(sortDirRecordsByName{de: dir})
}

func (dir *DirectoryEntry) Bytes() []byte {
	return dir.dir.Segment.Data
}

func (dir *DirectoryEntry) NumEntries() int {
	return int(dir.dir.NumEntries())
}

func (dir *DirectoryEntry) SetNumEntries(n int) {
	dir.dir.SetNumEntries(uint32(n))
}

func (dir *DirectoryEntry) Entry(i int) *DirectRecord {
	return overlayDirectoryRecord(dir.dir.Entries().At(i))
}

func (dir *DirectoryEntry) SetEntry(i int, record *DirectRecord) {
	dir.dir.Entries().Set(i, record.record)
}

func (dir *DirectoryEntry) Next() ObjectKey {
	return overlayObjectKey(dir.dir.Next())
}

func (dir *DirectoryEntry) HasNext() bool {
	return !dir.Next().IsEqualTo(EmptyDirKey) && dir.NumEntries() > 0
}

func (dir *DirectoryEntry) SetNext(key ObjectKey) {
	dir.dir.SetNext(key.key)
}

// The various types the next referenced object could be
const (
	ObjectTypeInvalid           = 0
	ObjectTypeBuildProduct      = 1
	ObjectTypeDirectoryEntry    = 2
	ObjectTypeExtendedAttribute = 3
	ObjectTypeHardlink          = 4
	ObjectTypeSymlink           = 5
	ObjectTypeVCSFile           = 6
	ObjectTypeWorkspaceRoot     = 7
	ObjectTypeSmallFile         = 8
	ObjectTypeMediumFile        = 9
	ObjectTypeLargeFile         = 10
	ObjectTypeVeryLargeFile     = 11
	ObjectTypeSpecial           = 12
)

func ObjectType2String(typ ObjectType) string {
	switch typ {
	case ObjectTypeBuildProduct:
		return "ObjectTypeBuildProduct"
	case ObjectTypeDirectoryEntry:
		return "ObjectTypeDirectoryEntry"
	case ObjectTypeExtendedAttribute:
		return "ObjectTypeExtendedAttribute"
	case ObjectTypeHardlink:
		return "ObjectTypeHardlink"
	case ObjectTypeSymlink:
		return "ObjectTypeSymlink"
	case ObjectTypeVCSFile:
		return "ObjectTypeVCSFile"
	case ObjectTypeWorkspaceRoot:
		return "ObjectTypeWorkspaceRoot"
	case ObjectTypeSmallFile:
		return "ObjectTypeSmallFile"
	case ObjectTypeMediumFile:
		return "ObjectTypeMediumFile"
	case ObjectTypeLargeFile:
		return "ObjectTypeLargeFile"
	case ObjectTypeVeryLargeFile:
		return "ObjectTypeVeryLargeFile"
	case ObjectTypeSpecial:
		return "ObjectTypeSpecial"
	default:
		return fmt.Sprintf("Unknown ObjectType %d\n", typ)
	}
}

// One of the ObjectType* values
type ObjectType uint8

// Convert to primitive type for qlog
func (v ObjectType) Primitive() interface{} {
	return uint8(v)
}

// Quantumfs doesn't keep precise ownership values. Instead files and directories may
// be owned by some special system accounts or the current user. The translation to
// UID is done at access time.
const UIDUser = 1001 // The currently accessing user

// UID to use if a system user (ie. root) views the attributes of a file owned by the
// user.
const UniversalUID = 10000

// Convert object UID to system UID.
//
// userId is the UID of the current user
func SystemUid(uid UID, userId uint32) uint32 {
	if uid < UIDUser {
		return uint32(uid)
	} else if uid == UIDUser {
		if userId < UIDUser {
			// If the user is running as a system account then we don't
			// want the files to appear to be owned by that account.
			// Instead make it appear owned by a normal user.
			return UniversalUID
		}
		return userId
	} else {
		panic(fmt.Sprintf("Unknown Owner %d", uid))
	}
}

// Convert system UID to object UID
//
// userId is the UID of the current user
func ObjectUid(uid uint32, userId uint32) UID {
	if uid < UIDUser {
		return UID(uid)
	} else {
		return UIDUser
	}
}

// One of the UID* values
type UID uint16

// Convert to primitive type for qlog
func (v UID) Primitive() interface{} {
	return uint8(v)
}

// Quantumfs doesn't keep precise ownership values. Instead files and directories may
// be owned by some special system accounts or the current user. The translation to
// GID is done at access time.
const GIDUser = 1001 // The currently accessing user

// GID to use if a system user (ie. root) views the attributes of a file owned by the
// user group.
const UniversalGID = 10000

// Convert object GID to system GID.
//
// userId is the GID of the current user
func SystemGid(gid GID, userId uint32) uint32 {
	if gid < GIDUser {
		return uint32(gid)
	} else if gid == GIDUser {
		if userId < GIDUser {
			// If the user is running as a system account then we don't
			// want the files to appear to be owned by that account.
			// Instead make it appear owned by a normal user.
			return UniversalGID
		}
		return userId
	} else {
		panic(fmt.Sprintf("Unknown Group %d", gid))
	}
}

// Convert system GID to object GID
//
// userId is the GID of the current user
func ObjectGid(gid uint32, groupId uint32) GID {
	if gid < GIDUser {
		return GID(gid)
	} else {
		return GIDUser
	}
}

// One of the GID* values
type GID uint16

// Convert to primitive type for qlog
func (v GID) Primitive() interface{} {
	return uint8(v)
}

// Quantumfs stores time in microseconds since the Unix epoch
type Time uint64

// Convert to primitive type for qlog
func (t Time) Primitive() interface{} {
	return uint64(t)
}

func (t Time) Seconds() uint64 {
	return uint64(t / 1000000)
}

func (t Time) Nanoseconds() uint32 {
	return uint32(t % 1000000)
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

// Take constant hashes produced by the emptykeys tool and convert them into a byte
// string as if we have computed the hash ourselves. This is necessary to allow
// clients to import the quantumfs package without requiring that the specific
// QuantumFS hash is built and available.
func decodeHashConstant(hash string) [ObjectKeyLength - 1]byte {
	var out [ObjectKeyLength - 1]byte

	bytes, err := hex.DecodeString(hash)
	if err != nil {
		panic(err.Error())
	}

	for i := range bytes {
		out[i] = bytes[1]
	}
	return out
}

// The key of the directory with no entries
var EmptyDirKey ObjectKey

func createEmptyDirectory() ObjectKey {
	_, emptyDir := NewDirectoryEntry(0)

	bytes := emptyDir.Bytes()

	hash := decodeHashConstant("2ee5784d3bdfcd8617885fdac9aee5f7c890fce8")
	emptyDirKey := NewObjectKey(KeyTypeConstant, hash)
	constStore.store[emptyDirKey.String()] = bytes
	return emptyDirKey
}

// The key of the datablock with zero length
var EmptyBlockKey ObjectKey

func createEmptyBlock() ObjectKey {
	var bytes []byte

	hash := decodeHashConstant("30f9a5e6242f1695e006ebf1f4bd0868824d627b")
	emptyBlockKey := NewObjectKey(KeyTypeConstant, hash)
	constStore.store[emptyBlockKey.String()] = bytes
	return emptyBlockKey
}

func NewWorkspaceRoot() *WorkspaceRoot {
	segment := capn.NewBuffer(nil)
	wsr := WorkspaceRoot{
		wsr: encoding.NewRootWorkspaceRoot(segment),
	}

	return &wsr
}

type WorkspaceRoot struct {
	wsr encoding.WorkspaceRoot
}

func OverlayWorkspaceRoot(ewsr encoding.WorkspaceRoot) WorkspaceRoot {
	wsr := WorkspaceRoot{
		wsr: ewsr,
	}
	return wsr
}

func NewHardlinkRecord() *HardlinkRecord {
	segment := capn.NewBuffer(nil)
	record := HardlinkRecord{
		record: encoding.NewRootHardlinkRecord(segment),
	}

	return &record
}

type HardlinkRecord struct {
	record encoding.HardlinkRecord
}

func overlayHardlinkRecord(r encoding.HardlinkRecord) *HardlinkRecord {
	record := HardlinkRecord{
		record: r,
	}
	return &record
}

func (r *HardlinkRecord) HardlinkID() uint64 {
	return r.record.HardlinkID()
}

func (r *HardlinkRecord) SetHardlinkID(v uint64) {
	r.record.SetHardlinkID(v)
}

func (r *HardlinkRecord) Record() *DirectRecord {
	return overlayDirectoryRecord(r.record.Record())
}

func (r *HardlinkRecord) SetRecord(v *DirectRecord) {
	r.record.SetRecord(v.record)
}

func (r *HardlinkRecord) Nlinks() uint32 {
	return r.record.Nlinks()
}

func (r *HardlinkRecord) SetNlinks(n uint32) {
	r.record.SetNlinks(n)
}

type HardlinkEntry struct {
	entry encoding.HardlinkEntry
}

func NewHardlinkEntry(entryCapacity int) (remain int, dirEntry *HardlinkEntry) {
	remain = 0
	recs := entryCapacity
	if entryCapacity > MaxDirectoryRecords() {
		recs = MaxDirectoryRecords()
		remain = entryCapacity - recs
	}

	segment := capn.NewBuffer(nil)

	dirEntry = &HardlinkEntry{
		entry: encoding.NewRootHardlinkEntry(segment),
	}
	dirEntry.entry.SetNumEntries(0)

	recordList := encoding.NewHardlinkRecordList(segment, recs)
	dirEntry.entry.SetEntries(recordList)

	return remain, dirEntry
}

func OverlayHardlinkEntry(edir encoding.HardlinkEntry) HardlinkEntry {
	dir := HardlinkEntry{
		entry: edir,
	}
	return dir
}

func (dir *HardlinkEntry) Bytes() []byte {
	return dir.entry.Segment.Data
}

func (dir *HardlinkEntry) NumEntries() int {
	return int(dir.entry.NumEntries())
}

func (dir *HardlinkEntry) SetNumEntries(n int) {
	dir.entry.SetNumEntries(uint32(n))
}

func (dir *HardlinkEntry) Entry(i int) *HardlinkRecord {
	return overlayHardlinkRecord(dir.entry.Entries().At(i))
}

func (dir *HardlinkEntry) SetEntry(i int, record *HardlinkRecord) {
	dir.entry.Entries().Set(i, record.record)
}

func (dir *HardlinkEntry) Next() ObjectKey {
	return overlayObjectKey(dir.entry.Next())
}

func (dir *HardlinkEntry) HasNext() bool {
	return !dir.Next().IsEqualTo(EmptyDirKey) && dir.NumEntries() > 0
}

func (dir *HardlinkEntry) SetNext(key ObjectKey) {
	dir.entry.SetNext(key.key)
}

func (wsr *WorkspaceRoot) HardlinkEntry() HardlinkEntry {
	return OverlayHardlinkEntry(wsr.wsr.HardlinkEntry())
}

func (wsr *WorkspaceRoot) SetHardlinkEntry(v *HardlinkEntry) {
	wsr.wsr.SetHardlinkEntry(v.entry)
}

func (wsr *WorkspaceRoot) Bytes() []byte {
	return wsr.wsr.Segment.Data
}

func (wsr *WorkspaceRoot) BaseLayer() ObjectKey {
	return overlayObjectKey(wsr.wsr.BaseLayer())
}

func (wsr *WorkspaceRoot) SetBaseLayer(key ObjectKey) {
	wsr.wsr.SetBaseLayer(key.key)
}

func (wsr *WorkspaceRoot) VcsLayer() ObjectKey {
	return overlayObjectKey(wsr.wsr.VcsLayer())
}

func (wsr *WorkspaceRoot) SetVcsLayer(key ObjectKey) {
	wsr.wsr.SetBaseLayer(key.key)
}

func (wsr *WorkspaceRoot) BuildLayer() ObjectKey {
	return overlayObjectKey(wsr.wsr.BuildLayer())
}

func (wsr *WorkspaceRoot) SetBuildLayer(key ObjectKey) {
	wsr.wsr.SetBuildLayer(key.key)
}

func (wsr *WorkspaceRoot) UserLayer() ObjectKey {
	return overlayObjectKey(wsr.wsr.UserLayer())
}

func (wsr *WorkspaceRoot) SetUserLayer(key ObjectKey) {
	wsr.wsr.SetUserLayer(key.key)
}

// The key of the workspace with no contents
var EmptyWorkspaceKey ObjectKey

func createEmptyWorkspace(emptyDirKey ObjectKey) ObjectKey {
	emptyWorkspace := NewWorkspaceRoot()
	emptyWorkspace.SetBaseLayer(emptyDirKey)
	emptyWorkspace.SetVcsLayer(emptyDirKey)
	emptyWorkspace.SetBuildLayer(emptyDirKey)
	emptyWorkspace.SetUserLayer(emptyDirKey)

	bytes := emptyWorkspace.Bytes()

	hash := decodeHashConstant("fd14336f054b393fdd4e298859565c91337e5791")
	emptyWorkspaceKey := NewObjectKey(KeyTypeConstant, hash)
	constStore.store[emptyWorkspaceKey.String()] = bytes
	return emptyWorkspaceKey
}

// Permission bit names
const (
	PermExecOther  = 1 << 0
	PermWriteOther = 1 << 1
	PermReadOther  = 1 << 2
	PermExecGroup  = 1 << 3
	PermWriteGroup = 1 << 4
	PermReadGroup  = 1 << 5
	PermExecOwner  = 1 << 6
	PermWriteOwner = 1 << 7
	PermReadOwner  = 1 << 8
	PermSticky     = 1 << 9
	PermSGID       = 1 << 10
	PermSUID       = 1 << 11
)

const PermReadAll = PermReadOther | PermReadGroup | PermReadOwner
const PermWriteAll = PermWriteOther | PermWriteGroup | PermWriteOwner
const PermExecAll = PermExecOther | PermExecGroup | PermExecOwner

type DirectoryRecord interface {
	Filename() string
	SetFilename(v string)

	ID() ObjectKey
	SetID(v ObjectKey)

	Type() ObjectType
	SetType(v ObjectType)

	Permissions() uint32
	SetPermissions(v uint32)

	Owner() UID
	SetOwner(v UID)

	Group() GID
	SetGroup(v GID)

	Size() uint64
	SetSize(v uint64)

	ExtendedAttributes() ObjectKey
	SetExtendedAttributes(v ObjectKey)

	ContentTime() Time
	SetContentTime(v Time)

	ModificationTime() Time
	SetModificationTime(v Time)

	Record() DirectRecord
	Nlinks() uint32

	EncodeExtendedKey() []byte

	// returns a direct record copy, regardless of underlying class, causing
	// all future changes to the copy to be unreflected in the original.
	// Mainly used for reads
	ShallowCopy() DirectoryRecord

	// returns a real copy, which can result in future changes changing the
	// original depending on the underlying class.
	Clone() DirectoryRecord
}

func NewDirectoryRecord() *DirectRecord {
	segment := capn.NewBuffer(nil)

	// for more records, nlinksCache is 1. If we're a shallow copy of a hardlink,
	// it must be a different value greater than 1
	record := DirectRecord{
		record: encoding.NewRootDirectoryRecord(segment),
	}

	return &record
}

type DirectRecord struct {
	record      encoding.DirectoryRecord
	nlinksCache uint32
}

func overlayDirectoryRecord(r encoding.DirectoryRecord) *DirectRecord {
	record := DirectRecord{
		record: r,
	}
	return &record
}

func (record *DirectRecord) Record() DirectRecord {
	return *record
}

func (record *DirectRecord) SetNlinks(links uint32) {
	record.nlinksCache = links
}

func (record *DirectRecord) Nlinks() uint32 {
	// Unless otherwise set, this is a normal record with one link
	if record.nlinksCache == 0 {
		return 1
	}

	return record.nlinksCache
}

func (record *DirectRecord) Filename() string {
	return record.record.Filename()
}

func (record *DirectRecord) SetFilename(name string) {
	record.record.SetFilename(name)
}

func (record *DirectRecord) Type() ObjectType {
	return ObjectType(record.record.Type())
}

func (record *DirectRecord) SetType(t ObjectType) {
	record.record.SetType(uint8(t))
}

func (record *DirectRecord) ID() ObjectKey {
	return overlayObjectKey(record.record.Id())
}

func (record *DirectRecord) SetID(key ObjectKey) {
	record.record.SetId(key.key)
}

func (record *DirectRecord) Size() uint64 {
	return record.record.Size()
}

func (record *DirectRecord) SetSize(s uint64) {
	record.record.SetSize(s)
}

func (record *DirectRecord) ModificationTime() Time {
	return Time(record.record.ModificationTime())
}

func (record *DirectRecord) SetModificationTime(t Time) {
	record.record.SetModificationTime(uint64(t))
}

func (record *DirectRecord) ContentTime() Time {
	return Time(record.record.ContentTime())
}

func (record *DirectRecord) SetContentTime(t Time) {
	record.record.SetContentTime(uint64(t))
}

func (record *DirectRecord) Permissions() uint32 {
	return record.record.Permissions()
}

func (record *DirectRecord) SetPermissions(p uint32) {
	record.record.SetPermissions(p)
}

func (record *DirectRecord) Owner() UID {
	return UID(record.record.Owner())
}

func (record *DirectRecord) SetOwner(u UID) {
	record.record.SetOwner(uint16(u))
}

func (record *DirectRecord) Group() GID {
	return GID(record.record.Group())
}

func (record *DirectRecord) SetGroup(g GID) {
	record.record.SetGroup(uint16(g))
}

func (record *DirectRecord) ExtendedAttributes() ObjectKey {
	return overlayObjectKey(record.record.ExtendedAttributes())
}

func (record *DirectRecord) SetExtendedAttributes(key ObjectKey) {
	record.record.SetExtendedAttributes(key.key)
}

func (record *DirectRecord) EncodeExtendedKey() []byte {
	return EncodeExtendedKey(record.ID(), record.Type(), record.Size())
}

func (record *DirectRecord) ShallowCopy() DirectoryRecord {
	newEntry := NewDirectoryRecord()
	newEntry.SetNlinks(record.Nlinks())
	newEntry.SetFilename(record.Filename())
	newEntry.SetID(record.ID())
	newEntry.SetType(record.Type())
	newEntry.SetPermissions(record.Permissions())
	newEntry.SetOwner(record.Owner())
	newEntry.SetGroup(record.Group())
	newEntry.SetSize(record.Size())
	newEntry.SetExtendedAttributes(record.ExtendedAttributes())
	newEntry.SetContentTime(record.ContentTime())
	newEntry.SetModificationTime(record.ModificationTime())

	return newEntry
}

func (record *DirectRecord) Clone() DirectoryRecord {
	// For DirectRecord, ShallowCopy is as good as a clone
	return record.ShallowCopy()
}

func EncodeExtendedKey(key ObjectKey, type_ ObjectType,
	size uint64) []byte {

	append_ := make([]byte, 9)
	append_[0] = uint8(type_)
	binary.LittleEndian.PutUint64(append_[1:], size)

	data := append(key.Value(), append_...)
	return []byte(base64.StdEncoding.EncodeToString(data))
}

func DecodeExtendedKey(packet string) (ObjectKey, ObjectType, uint64, error) {

	bDec, err := base64.StdEncoding.DecodeString(packet)
	if err != nil {
		return ZeroKey, 0, 0, err
	}

	key := NewObjectKeyFromBytes(bDec[:sourceDataLength-9])
	type_ := ObjectType(bDec[sourceDataLength-9])
	size := binary.LittleEndian.Uint64(bDec[sourceDataLength-8:])
	return key, type_, size, nil
}

func NewMultiBlockFile(maxBlocks int) *MultiBlockFile {
	segment := capn.NewBuffer(nil)
	mb := MultiBlockFile{
		mb: encoding.NewRootMultiBlockFile(segment),
	}

	blocks := encoding.NewObjectKeyList(segment, maxBlocks)
	mb.mb.SetListOfBlocks(blocks)
	return &mb
}

func OverlayMultiBlockFile(emb encoding.MultiBlockFile) MultiBlockFile {
	mb := MultiBlockFile{
		mb: emb,
	}
	return mb
}

type MultiBlockFile struct {
	mb encoding.MultiBlockFile
}

func (mb *MultiBlockFile) BlockSize() uint32 {
	return mb.mb.BlockSize()
}

func (mb *MultiBlockFile) SetBlockSize(n uint32) {
	mb.mb.SetBlockSize(n)
}

func (mb *MultiBlockFile) SizeOfLastBlock() uint32 {
	return mb.mb.SizeOfLastBlock()
}

func (mb *MultiBlockFile) SetSizeOfLastBlock(n uint32) {
	mb.mb.SetSizeOfLastBlock(n)
}

func (mb *MultiBlockFile) SetNumberOfBlocks(n int) {
	mb.mb.SetNumberOfBlocks(uint32(n))
}

func (mb *MultiBlockFile) ListOfBlocks() []ObjectKey {
	blocks := mb.mb.ListOfBlocks().ToArray()
	blocks = blocks[:mb.mb.NumberOfBlocks()]

	keys := make([]ObjectKey, 0, len(blocks))
	for _, block := range blocks {
		keys = append(keys, overlayObjectKey(block))
	}

	return keys
}

func (mb *MultiBlockFile) SetListOfBlocks(keys []ObjectKey) {
	for i, key := range keys {
		mb.mb.ListOfBlocks().Set(i, key.key)
	}
}

func (mb *MultiBlockFile) Bytes() []byte {
	return mb.mb.Segment.Data
}

func NewVeryLargeFile(entryCapacity int) *VeryLargeFile {
	segment := capn.NewBuffer(nil)
	vlf := VeryLargeFile{
		vlf: encoding.NewRootVeryLargeFile(segment),
	}

	if entryCapacity > MaxPartsVeryLargeFile() {
		entryCapacity = MaxPartsVeryLargeFile()
	}
	largeFiles := encoding.NewObjectKeyList(segment, entryCapacity)
	vlf.vlf.SetLargeFileKeys(largeFiles)
	return &vlf
}

func OverlayVeryLargeFile(evlf encoding.VeryLargeFile) VeryLargeFile {
	vlf := VeryLargeFile{
		vlf: evlf,
	}
	return vlf
}

type VeryLargeFile struct {
	vlf encoding.VeryLargeFile
}

func (vlf *VeryLargeFile) NumberOfParts() int {
	return int(vlf.vlf.NumberOfParts())
}

func (vlf *VeryLargeFile) SetNumberOfParts(n int) {
	vlf.vlf.SetNumberOfParts(uint32(n))
}

func (vlf *VeryLargeFile) LargeFileKey(i int) ObjectKey {
	return overlayObjectKey(vlf.vlf.LargeFileKeys().At(i))
}

func (vlf *VeryLargeFile) SetLargeFileKey(i int, key ObjectKey) {
	vlf.vlf.LargeFileKeys().Set(i, key.key)
}

func (vlf *VeryLargeFile) Bytes() []byte {
	return vlf.vlf.Segment.Data
}

func NewExtendedAttributes() *ExtendedAttributes {
	return newExtendedAttributesAttrs(MaxNumExtendedAttributes())
}

func newExtendedAttributesAttrs(attrs int) *ExtendedAttributes {
	segment := capn.NewBuffer(nil)
	ea := ExtendedAttributes{
		ea: encoding.NewRootExtendedAttributes(segment),
	}

	attributes := encoding.NewExtendedAttributeList(segment,
		attrs)
	ea.ea.SetAttributes(attributes)
	return &ea
}

func OverlayExtendedAttributes(eea encoding.ExtendedAttributes) ExtendedAttributes {
	ea := ExtendedAttributes{
		ea: eea,
	}
	return ea
}

type ExtendedAttributes struct {
	ea encoding.ExtendedAttributes
}

func (ea *ExtendedAttributes) NumAttributes() int {
	return int(ea.ea.NumAttributes())
}

func (ea *ExtendedAttributes) SetNumAttributes(num int) {
	ea.ea.SetNumAttributes(uint32(num))
}

func (ea *ExtendedAttributes) Attribute(i int) (name string, id ObjectKey) {
	attribute := ea.ea.Attributes().At(i)
	return attribute.Name(), overlayObjectKey(attribute.Id())
}

func (ea *ExtendedAttributes) SetAttribute(i int, name string, id ObjectKey) {
	segment := capn.NewBuffer(nil)
	attribute := encoding.NewRootExtendedAttribute(segment)
	attribute.SetName(name)
	attribute.SetId(id.key)
	ea.ea.Attributes().Set(i, attribute)
}

func (ea *ExtendedAttributes) Bytes() []byte {
	return ea.ea.Segment.Data
}

// Buffer represents a bundle of data from a datastore.
type Buffer interface {
	Write(c *Ctx, in []byte, offset uint32) uint32
	Read(out []byte, offset uint32) int
	Get() []byte
	Set(data []byte, keyType KeyType)
	ContentHash() [ObjectKeyLength - 1]byte
	Key(c *Ctx) (ObjectKey, error)
	SetSize(size int)
	Size() int

	// These methods interpret the Buffer as various metadata types
	AsWorkspaceRoot() WorkspaceRoot
	AsDirectoryEntry() DirectoryEntry
	AsMultiBlockFile() MultiBlockFile
	AsVeryLargeFile() VeryLargeFile
	AsExtendedAttributes() ExtendedAttributes
	AsHardlinkEntry() HardlinkEntry
}

type DataStore interface {
	Get(c *Ctx, key ObjectKey, buf Buffer) error
	Set(c *Ctx, key ObjectKey, buf Buffer) error
}

// A pseudo-store which contains all the constant objects
var constStore = newConstantStore()
var ConstantStore = DataStore(constStore)

func newConstantStore() *constDataStore {
	return &constDataStore{
		store: make(map[string][]byte),
	}
}

type constDataStore struct {
	store map[string][]byte
}

func (store *constDataStore) Get(c *Ctx, key ObjectKey, buf Buffer) error {
	if data, ok := store.store[key.String()]; ok {
		newData := make([]byte, len(data))
		copy(newData, data)
		buf.Set(newData, key.Type())
		return nil
	}
	return fmt.Errorf("Object not found")
}

func (store *constDataStore) Set(c *Ctx, key ObjectKey, buf Buffer) error {
	return fmt.Errorf("Cannot set in constant datastore")
}

var ZeroKey ObjectKey

func calcMaxNumExtendedAttributes(maxSize int) int {
	attrs0 := newExtendedAttributesAttrs(0)
	size0attrs := len(attrs0.Bytes())

	attrs1 := newExtendedAttributesAttrs(1)
	// setup the pointers in ExtendedAttribute to practical max values
	attrs1.SetAttribute(0, string(make([]byte, MaxXAttrnameLength)),
		createEmptyBlock())
	size1attrs := len(attrs1.Bytes())

	return (maxSize - size0attrs) / (size1attrs - size0attrs)
}

func calcMaxDirectoryRecords(maxSize int) int {
	dir0 := newDirectoryEntryRecords(0)
	size0recs := len(dir0.Bytes())

	// setup the pointers in DirectRecord to practical max values
	record := NewDirectoryRecord()
	record.SetFilename(string(make([]byte, MaxFilenameLength)))
	record.SetExtendedAttributes(createEmptyBlock())

	dir1 := newDirectoryEntryRecords(1)
	dir1.dir.Entries().Set(0, record.record)
	size1recs := len(dir1.Bytes())

	return (maxSize - size0recs) / (size1recs - size0recs)
}

func calcMaxBlocksLargeFile(maxSize int) int {
	mb0 := NewMultiBlockFile(0)
	size0keys := len(mb0.Bytes())

	mb1 := NewMultiBlockFile(1)
	// all keys are of same size so use any key
	mb1.SetListOfBlocks([]ObjectKey{createEmptyBlock()})
	size1keys := len(mb1.Bytes())

	return (maxSize - size0keys) / (size1keys - size0keys)
}

func init() {
	ZeroKey = NewObjectKey(KeyTypeEmbedded, [ObjectKeyLength - 1]byte{})

	if MaxBlockSize > 1024*1024*1024 || MaxBlockSize < 32*1024 {
		// if a MaxBlockSize beyond this range is needed then
		// notions like max medium file type size is 32MB etc needs
		// to be revisited
		panic(fmt.Sprintf("MaxBlockSize %d .Valid range is 32KB..1MB\n",
			MaxBlockSize))
	}

	maxBlocksLargeFile = calcMaxBlocksLargeFile(MaxBlockSize)
	if maxBlocksLargeFile == 0 {
		panic(fmt.Sprintf("MaxBlockSize %d is small for Large file type",
			MaxBlockSize))
	}

	// maximum medium file size should be 32MB
	// for supported range, maxBlocksMediumFile < maxBlocksLargeFile
	maxBlocksMediumFile = (32 * 1024 * 1024) / MaxBlockSize
	// if MaxBlockSize is within supported size then
	// maxBlocksMediumFile < (maxBlocksLargeFile-1)

	maxPartsVeryLargeFile = MaxBlocksLargeFile()

	maxDirectoryRecords = calcMaxDirectoryRecords(int(MaxBlockSize))
	if maxDirectoryRecords == 0 {
		panic(fmt.Sprintf("MaxBlockSize %d is small for DirectoryEntry",
			MaxBlockSize))
	}

	maxNumExtendedAttributes = calcMaxNumExtendedAttributes(int(MaxBlockSize))
	if maxNumExtendedAttributes == 0 {
		panic(fmt.Sprintf("MaxBlockSize %d is small for ExtendedAttributes",
			MaxBlockSize))
	}

	emptyDirKey := createEmptyDirectory()
	emptyBlockKey := createEmptyBlock()
	emptyWorkspaceKey := createEmptyWorkspace(emptyDirKey)
	EmptyDirKey = emptyDirKey
	EmptyBlockKey = emptyBlockKey
	EmptyWorkspaceKey = emptyWorkspaceKey
}
