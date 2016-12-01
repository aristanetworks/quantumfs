package encoding

// AUTO GENERATED - DO NOT EDIT

import (
	"bufio"
	"bytes"
	"encoding/json"
	C "github.com/glycerine/go-capnproto"
	"io"
)

const (
	MaxBlockSize             = uint32(1048576)
	MaxBlocksMediumFile      = uint32(32)
	MaxBlocksLargeFile       = uint32(22000)
	MaxPartsVeryLargeFile    = uint32(22000)
	MaxDirectoryRecords      = uint32(1200)
	MaxFilenameLength        = uint32(256)
	MaxNumExtendedAttributes = uint32(3700)
)

type ObjectKey C.Struct

func NewObjectKey(s *C.Segment) ObjectKey      { return ObjectKey(s.NewStruct(24, 0)) }
func NewRootObjectKey(s *C.Segment) ObjectKey  { return ObjectKey(s.NewRootStruct(24, 0)) }
func AutoNewObjectKey(s *C.Segment) ObjectKey  { return ObjectKey(s.NewStructAR(24, 0)) }
func ReadRootObjectKey(s *C.Segment) ObjectKey { return ObjectKey(s.Root(0).ToStruct()) }
func (s ObjectKey) KeyType() uint8             { return C.Struct(s).Get8(0) }
func (s ObjectKey) SetKeyType(v uint8)         { C.Struct(s).Set8(0, v) }
func (s ObjectKey) Part2() uint64              { return C.Struct(s).Get64(8) }
func (s ObjectKey) SetPart2(v uint64)          { C.Struct(s).Set64(8, v) }
func (s ObjectKey) Part3() uint64              { return C.Struct(s).Get64(16) }
func (s ObjectKey) SetPart3(v uint64)          { C.Struct(s).Set64(16, v) }
func (s ObjectKey) Part4() uint32              { return C.Struct(s).Get32(4) }
func (s ObjectKey) SetPart4(v uint32)          { C.Struct(s).Set32(4, v) }
func (s ObjectKey) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"keyType\":")
	if err != nil {
		return err
	}
	{
		s := s.KeyType()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"part2\":")
	if err != nil {
		return err
	}
	{
		s := s.Part2()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"part3\":")
	if err != nil {
		return err
	}
	{
		s := s.Part3()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"part4\":")
	if err != nil {
		return err
	}
	{
		s := s.Part4()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s ObjectKey) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s ObjectKey) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("keyType = ")
	if err != nil {
		return err
	}
	{
		s := s.KeyType()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("part2 = ")
	if err != nil {
		return err
	}
	{
		s := s.Part2()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("part3 = ")
	if err != nil {
		return err
	}
	{
		s := s.Part3()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("part4 = ")
	if err != nil {
		return err
	}
	{
		s := s.Part4()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s ObjectKey) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type ObjectKey_List C.PointerList

func NewObjectKeyList(s *C.Segment, sz int) ObjectKey_List {
	return ObjectKey_List(s.NewCompositeList(24, 0, sz))
}
func (s ObjectKey_List) Len() int           { return C.PointerList(s).Len() }
func (s ObjectKey_List) At(i int) ObjectKey { return ObjectKey(C.PointerList(s).At(i).ToStruct()) }
func (s ObjectKey_List) ToArray() []ObjectKey {
	n := s.Len()
	a := make([]ObjectKey, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s ObjectKey_List) Set(i int, item ObjectKey) { C.PointerList(s).Set(i, C.Object(item)) }

type DirectoryRecord C.Struct

func NewDirectoryRecord(s *C.Segment) DirectoryRecord { return DirectoryRecord(s.NewStruct(40, 3)) }
func NewRootDirectoryRecord(s *C.Segment) DirectoryRecord {
	return DirectoryRecord(s.NewRootStruct(40, 3))
}
func AutoNewDirectoryRecord(s *C.Segment) DirectoryRecord {
	return DirectoryRecord(s.NewStructAR(40, 3))
}
func ReadRootDirectoryRecord(s *C.Segment) DirectoryRecord {
	return DirectoryRecord(s.Root(0).ToStruct())
}
func (s DirectoryRecord) Filename() string        { return C.Struct(s).GetObject(0).ToText() }
func (s DirectoryRecord) FilenameBytes() []byte   { return C.Struct(s).GetObject(0).ToDataTrimLastByte() }
func (s DirectoryRecord) SetFilename(v string)    { C.Struct(s).SetObject(0, s.Segment.NewText(v)) }
func (s DirectoryRecord) Id() ObjectKey           { return ObjectKey(C.Struct(s).GetObject(1).ToStruct()) }
func (s DirectoryRecord) SetId(v ObjectKey)       { C.Struct(s).SetObject(1, C.Object(v)) }
func (s DirectoryRecord) Type() uint8             { return C.Struct(s).Get8(0) }
func (s DirectoryRecord) SetType(v uint8)         { C.Struct(s).Set8(0, v) }
func (s DirectoryRecord) Permissions() uint32     { return C.Struct(s).Get32(4) }
func (s DirectoryRecord) SetPermissions(v uint32) { C.Struct(s).Set32(4, v) }
func (s DirectoryRecord) Owner() uint16           { return C.Struct(s).Get16(2) }
func (s DirectoryRecord) SetOwner(v uint16)       { C.Struct(s).Set16(2, v) }
func (s DirectoryRecord) Group() uint16           { return C.Struct(s).Get16(8) }
func (s DirectoryRecord) SetGroup(v uint16)       { C.Struct(s).Set16(8, v) }
func (s DirectoryRecord) Size() uint64            { return C.Struct(s).Get64(16) }
func (s DirectoryRecord) SetSize(v uint64)        { C.Struct(s).Set64(16, v) }
func (s DirectoryRecord) ExtendedAttributes() ObjectKey {
	return ObjectKey(C.Struct(s).GetObject(2).ToStruct())
}
func (s DirectoryRecord) SetExtendedAttributes(v ObjectKey) { C.Struct(s).SetObject(2, C.Object(v)) }
func (s DirectoryRecord) ContentTime() uint64               { return C.Struct(s).Get64(24) }
func (s DirectoryRecord) SetContentTime(v uint64)           { C.Struct(s).Set64(24, v) }
func (s DirectoryRecord) ModificationTime() uint64          { return C.Struct(s).Get64(32) }
func (s DirectoryRecord) SetModificationTime(v uint64)      { C.Struct(s).Set64(32, v) }
func (s DirectoryRecord) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"filename\":")
	if err != nil {
		return err
	}
	{
		s := s.Filename()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"id\":")
	if err != nil {
		return err
	}
	{
		s := s.Id()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"type\":")
	if err != nil {
		return err
	}
	{
		s := s.Type()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"permissions\":")
	if err != nil {
		return err
	}
	{
		s := s.Permissions()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"owner\":")
	if err != nil {
		return err
	}
	{
		s := s.Owner()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"group\":")
	if err != nil {
		return err
	}
	{
		s := s.Group()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"size\":")
	if err != nil {
		return err
	}
	{
		s := s.Size()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"extendedAttributes\":")
	if err != nil {
		return err
	}
	{
		s := s.ExtendedAttributes()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"contentTime\":")
	if err != nil {
		return err
	}
	{
		s := s.ContentTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"modificationTime\":")
	if err != nil {
		return err
	}
	{
		s := s.ModificationTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s DirectoryRecord) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s DirectoryRecord) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("filename = ")
	if err != nil {
		return err
	}
	{
		s := s.Filename()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("id = ")
	if err != nil {
		return err
	}
	{
		s := s.Id()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("type = ")
	if err != nil {
		return err
	}
	{
		s := s.Type()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("permissions = ")
	if err != nil {
		return err
	}
	{
		s := s.Permissions()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("owner = ")
	if err != nil {
		return err
	}
	{
		s := s.Owner()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("group = ")
	if err != nil {
		return err
	}
	{
		s := s.Group()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("size = ")
	if err != nil {
		return err
	}
	{
		s := s.Size()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("extendedAttributes = ")
	if err != nil {
		return err
	}
	{
		s := s.ExtendedAttributes()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("contentTime = ")
	if err != nil {
		return err
	}
	{
		s := s.ContentTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("modificationTime = ")
	if err != nil {
		return err
	}
	{
		s := s.ModificationTime()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s DirectoryRecord) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type DirectoryRecord_List C.PointerList

func NewDirectoryRecordList(s *C.Segment, sz int) DirectoryRecord_List {
	return DirectoryRecord_List(s.NewCompositeList(40, 3, sz))
}
func (s DirectoryRecord_List) Len() int { return C.PointerList(s).Len() }
func (s DirectoryRecord_List) At(i int) DirectoryRecord {
	return DirectoryRecord(C.PointerList(s).At(i).ToStruct())
}
func (s DirectoryRecord_List) ToArray() []DirectoryRecord {
	n := s.Len()
	a := make([]DirectoryRecord, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s DirectoryRecord_List) Set(i int, item DirectoryRecord) {
	C.PointerList(s).Set(i, C.Object(item))
}

type DirectoryEntry C.Struct

func NewDirectoryEntry(s *C.Segment) DirectoryEntry      { return DirectoryEntry(s.NewStruct(8, 2)) }
func NewRootDirectoryEntry(s *C.Segment) DirectoryEntry  { return DirectoryEntry(s.NewRootStruct(8, 2)) }
func AutoNewDirectoryEntry(s *C.Segment) DirectoryEntry  { return DirectoryEntry(s.NewStructAR(8, 2)) }
func ReadRootDirectoryEntry(s *C.Segment) DirectoryEntry { return DirectoryEntry(s.Root(0).ToStruct()) }
func (s DirectoryEntry) NumEntries() uint32              { return C.Struct(s).Get32(0) }
func (s DirectoryEntry) SetNumEntries(v uint32)          { C.Struct(s).Set32(0, v) }
func (s DirectoryEntry) Next() ObjectKey                 { return ObjectKey(C.Struct(s).GetObject(0).ToStruct()) }
func (s DirectoryEntry) SetNext(v ObjectKey)             { C.Struct(s).SetObject(0, C.Object(v)) }
func (s DirectoryEntry) Entries() DirectoryRecord_List {
	return DirectoryRecord_List(C.Struct(s).GetObject(1))
}
func (s DirectoryEntry) SetEntries(v DirectoryRecord_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s DirectoryEntry) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"numEntries\":")
	if err != nil {
		return err
	}
	{
		s := s.NumEntries()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"next\":")
	if err != nil {
		return err
	}
	{
		s := s.Next()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"entries\":")
	if err != nil {
		return err
	}
	{
		s := s.Entries()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s DirectoryEntry) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s DirectoryEntry) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("numEntries = ")
	if err != nil {
		return err
	}
	{
		s := s.NumEntries()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("next = ")
	if err != nil {
		return err
	}
	{
		s := s.Next()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("entries = ")
	if err != nil {
		return err
	}
	{
		s := s.Entries()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s DirectoryEntry) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type DirectoryEntry_List C.PointerList

func NewDirectoryEntryList(s *C.Segment, sz int) DirectoryEntry_List {
	return DirectoryEntry_List(s.NewCompositeList(8, 2, sz))
}
func (s DirectoryEntry_List) Len() int { return C.PointerList(s).Len() }
func (s DirectoryEntry_List) At(i int) DirectoryEntry {
	return DirectoryEntry(C.PointerList(s).At(i).ToStruct())
}
func (s DirectoryEntry_List) ToArray() []DirectoryEntry {
	n := s.Len()
	a := make([]DirectoryEntry, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s DirectoryEntry_List) Set(i int, item DirectoryEntry) { C.PointerList(s).Set(i, C.Object(item)) }

type HardlinkRecord C.Struct

func NewHardlinkRecord(s *C.Segment) HardlinkRecord      { return HardlinkRecord(s.NewStruct(8, 1)) }
func NewRootHardlinkRecord(s *C.Segment) HardlinkRecord  { return HardlinkRecord(s.NewRootStruct(8, 1)) }
func AutoNewHardlinkRecord(s *C.Segment) HardlinkRecord  { return HardlinkRecord(s.NewStructAR(8, 1)) }
func ReadRootHardlinkRecord(s *C.Segment) HardlinkRecord { return HardlinkRecord(s.Root(0).ToStruct()) }
func (s HardlinkRecord) HardlinkID() uint64              { return C.Struct(s).Get64(0) }
func (s HardlinkRecord) SetHardlinkID(v uint64)          { C.Struct(s).Set64(0, v) }
func (s HardlinkRecord) Record() DirectoryRecord {
	return DirectoryRecord(C.Struct(s).GetObject(0).ToStruct())
}
func (s HardlinkRecord) SetRecord(v DirectoryRecord) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s HardlinkRecord) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"hardlinkID\":")
	if err != nil {
		return err
	}
	{
		s := s.HardlinkID()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"record\":")
	if err != nil {
		return err
	}
	{
		s := s.Record()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s HardlinkRecord) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s HardlinkRecord) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("hardlinkID = ")
	if err != nil {
		return err
	}
	{
		s := s.HardlinkID()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("record = ")
	if err != nil {
		return err
	}
	{
		s := s.Record()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s HardlinkRecord) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type HardlinkRecord_List C.PointerList

func NewHardlinkRecordList(s *C.Segment, sz int) HardlinkRecord_List {
	return HardlinkRecord_List(s.NewCompositeList(8, 1, sz))
}
func (s HardlinkRecord_List) Len() int { return C.PointerList(s).Len() }
func (s HardlinkRecord_List) At(i int) HardlinkRecord {
	return HardlinkRecord(C.PointerList(s).At(i).ToStruct())
}
func (s HardlinkRecord_List) ToArray() []HardlinkRecord {
	n := s.Len()
	a := make([]HardlinkRecord, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s HardlinkRecord_List) Set(i int, item HardlinkRecord) { C.PointerList(s).Set(i, C.Object(item)) }

type HardlinkEntry C.Struct

func NewHardlinkEntry(s *C.Segment) HardlinkEntry      { return HardlinkEntry(s.NewStruct(8, 2)) }
func NewRootHardlinkEntry(s *C.Segment) HardlinkEntry  { return HardlinkEntry(s.NewRootStruct(8, 2)) }
func AutoNewHardlinkEntry(s *C.Segment) HardlinkEntry  { return HardlinkEntry(s.NewStructAR(8, 2)) }
func ReadRootHardlinkEntry(s *C.Segment) HardlinkEntry { return HardlinkEntry(s.Root(0).ToStruct()) }
func (s HardlinkEntry) NumEntries() uint32             { return C.Struct(s).Get32(0) }
func (s HardlinkEntry) SetNumEntries(v uint32)         { C.Struct(s).Set32(0, v) }
func (s HardlinkEntry) Next() ObjectKey                { return ObjectKey(C.Struct(s).GetObject(0).ToStruct()) }
func (s HardlinkEntry) SetNext(v ObjectKey)            { C.Struct(s).SetObject(0, C.Object(v)) }
func (s HardlinkEntry) Entries() HardlinkRecord_List {
	return HardlinkRecord_List(C.Struct(s).GetObject(1))
}
func (s HardlinkEntry) SetEntries(v HardlinkRecord_List) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s HardlinkEntry) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"numEntries\":")
	if err != nil {
		return err
	}
	{
		s := s.NumEntries()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"next\":")
	if err != nil {
		return err
	}
	{
		s := s.Next()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"entries\":")
	if err != nil {
		return err
	}
	{
		s := s.Entries()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s HardlinkEntry) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s HardlinkEntry) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("numEntries = ")
	if err != nil {
		return err
	}
	{
		s := s.NumEntries()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("next = ")
	if err != nil {
		return err
	}
	{
		s := s.Next()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("entries = ")
	if err != nil {
		return err
	}
	{
		s := s.Entries()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s HardlinkEntry) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type HardlinkEntry_List C.PointerList

func NewHardlinkEntryList(s *C.Segment, sz int) HardlinkEntry_List {
	return HardlinkEntry_List(s.NewCompositeList(8, 2, sz))
}
func (s HardlinkEntry_List) Len() int { return C.PointerList(s).Len() }
func (s HardlinkEntry_List) At(i int) HardlinkEntry {
	return HardlinkEntry(C.PointerList(s).At(i).ToStruct())
}
func (s HardlinkEntry_List) ToArray() []HardlinkEntry {
	n := s.Len()
	a := make([]HardlinkEntry, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s HardlinkEntry_List) Set(i int, item HardlinkEntry) { C.PointerList(s).Set(i, C.Object(item)) }

type WorkspaceRoot C.Struct

func NewWorkspaceRoot(s *C.Segment) WorkspaceRoot      { return WorkspaceRoot(s.NewStruct(0, 5)) }
func NewRootWorkspaceRoot(s *C.Segment) WorkspaceRoot  { return WorkspaceRoot(s.NewRootStruct(0, 5)) }
func AutoNewWorkspaceRoot(s *C.Segment) WorkspaceRoot  { return WorkspaceRoot(s.NewStructAR(0, 5)) }
func ReadRootWorkspaceRoot(s *C.Segment) WorkspaceRoot { return WorkspaceRoot(s.Root(0).ToStruct()) }
func (s WorkspaceRoot) BaseLayer() ObjectKey           { return ObjectKey(C.Struct(s).GetObject(0).ToStruct()) }
func (s WorkspaceRoot) SetBaseLayer(v ObjectKey)       { C.Struct(s).SetObject(0, C.Object(v)) }
func (s WorkspaceRoot) VcsLayer() ObjectKey            { return ObjectKey(C.Struct(s).GetObject(1).ToStruct()) }
func (s WorkspaceRoot) SetVcsLayer(v ObjectKey)        { C.Struct(s).SetObject(1, C.Object(v)) }
func (s WorkspaceRoot) BuildLayer() ObjectKey          { return ObjectKey(C.Struct(s).GetObject(2).ToStruct()) }
func (s WorkspaceRoot) SetBuildLayer(v ObjectKey)      { C.Struct(s).SetObject(2, C.Object(v)) }
func (s WorkspaceRoot) UserLayer() ObjectKey           { return ObjectKey(C.Struct(s).GetObject(3).ToStruct()) }
func (s WorkspaceRoot) SetUserLayer(v ObjectKey)       { C.Struct(s).SetObject(3, C.Object(v)) }
func (s WorkspaceRoot) HardlinkEntry() HardlinkEntry {
	return HardlinkEntry(C.Struct(s).GetObject(4).ToStruct())
}
func (s WorkspaceRoot) SetHardlinkEntry(v HardlinkEntry) { C.Struct(s).SetObject(4, C.Object(v)) }
func (s WorkspaceRoot) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"baseLayer\":")
	if err != nil {
		return err
	}
	{
		s := s.BaseLayer()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"vcsLayer\":")
	if err != nil {
		return err
	}
	{
		s := s.VcsLayer()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"buildLayer\":")
	if err != nil {
		return err
	}
	{
		s := s.BuildLayer()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"userLayer\":")
	if err != nil {
		return err
	}
	{
		s := s.UserLayer()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"hardlinkEntry\":")
	if err != nil {
		return err
	}
	{
		s := s.HardlinkEntry()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s WorkspaceRoot) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s WorkspaceRoot) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("baseLayer = ")
	if err != nil {
		return err
	}
	{
		s := s.BaseLayer()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("vcsLayer = ")
	if err != nil {
		return err
	}
	{
		s := s.VcsLayer()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("buildLayer = ")
	if err != nil {
		return err
	}
	{
		s := s.BuildLayer()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("userLayer = ")
	if err != nil {
		return err
	}
	{
		s := s.UserLayer()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("hardlinkEntry = ")
	if err != nil {
		return err
	}
	{
		s := s.HardlinkEntry()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s WorkspaceRoot) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type WorkspaceRoot_List C.PointerList

func NewWorkspaceRootList(s *C.Segment, sz int) WorkspaceRoot_List {
	return WorkspaceRoot_List(s.NewCompositeList(0, 5, sz))
}
func (s WorkspaceRoot_List) Len() int { return C.PointerList(s).Len() }
func (s WorkspaceRoot_List) At(i int) WorkspaceRoot {
	return WorkspaceRoot(C.PointerList(s).At(i).ToStruct())
}
func (s WorkspaceRoot_List) ToArray() []WorkspaceRoot {
	n := s.Len()
	a := make([]WorkspaceRoot, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s WorkspaceRoot_List) Set(i int, item WorkspaceRoot) { C.PointerList(s).Set(i, C.Object(item)) }

type VeryLargeFile C.Struct

func NewVeryLargeFile(s *C.Segment) VeryLargeFile         { return VeryLargeFile(s.NewStruct(8, 1)) }
func NewRootVeryLargeFile(s *C.Segment) VeryLargeFile     { return VeryLargeFile(s.NewRootStruct(8, 1)) }
func AutoNewVeryLargeFile(s *C.Segment) VeryLargeFile     { return VeryLargeFile(s.NewStructAR(8, 1)) }
func ReadRootVeryLargeFile(s *C.Segment) VeryLargeFile    { return VeryLargeFile(s.Root(0).ToStruct()) }
func (s VeryLargeFile) NumberOfParts() uint32             { return C.Struct(s).Get32(0) }
func (s VeryLargeFile) SetNumberOfParts(v uint32)         { C.Struct(s).Set32(0, v) }
func (s VeryLargeFile) LargeFileKeys() ObjectKey_List     { return ObjectKey_List(C.Struct(s).GetObject(0)) }
func (s VeryLargeFile) SetLargeFileKeys(v ObjectKey_List) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s VeryLargeFile) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"numberOfParts\":")
	if err != nil {
		return err
	}
	{
		s := s.NumberOfParts()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"largeFileKeys\":")
	if err != nil {
		return err
	}
	{
		s := s.LargeFileKeys()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s VeryLargeFile) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s VeryLargeFile) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("numberOfParts = ")
	if err != nil {
		return err
	}
	{
		s := s.NumberOfParts()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("largeFileKeys = ")
	if err != nil {
		return err
	}
	{
		s := s.LargeFileKeys()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s VeryLargeFile) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type VeryLargeFile_List C.PointerList

func NewVeryLargeFileList(s *C.Segment, sz int) VeryLargeFile_List {
	return VeryLargeFile_List(s.NewCompositeList(8, 1, sz))
}
func (s VeryLargeFile_List) Len() int { return C.PointerList(s).Len() }
func (s VeryLargeFile_List) At(i int) VeryLargeFile {
	return VeryLargeFile(C.PointerList(s).At(i).ToStruct())
}
func (s VeryLargeFile_List) ToArray() []VeryLargeFile {
	n := s.Len()
	a := make([]VeryLargeFile, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s VeryLargeFile_List) Set(i int, item VeryLargeFile) { C.PointerList(s).Set(i, C.Object(item)) }

type MultiBlockFile C.Struct

func NewMultiBlockFile(s *C.Segment) MultiBlockFile       { return MultiBlockFile(s.NewStruct(16, 1)) }
func NewRootMultiBlockFile(s *C.Segment) MultiBlockFile   { return MultiBlockFile(s.NewRootStruct(16, 1)) }
func AutoNewMultiBlockFile(s *C.Segment) MultiBlockFile   { return MultiBlockFile(s.NewStructAR(16, 1)) }
func ReadRootMultiBlockFile(s *C.Segment) MultiBlockFile  { return MultiBlockFile(s.Root(0).ToStruct()) }
func (s MultiBlockFile) BlockSize() uint32                { return C.Struct(s).Get32(0) }
func (s MultiBlockFile) SetBlockSize(v uint32)            { C.Struct(s).Set32(0, v) }
func (s MultiBlockFile) NumberOfBlocks() uint32           { return C.Struct(s).Get32(4) }
func (s MultiBlockFile) SetNumberOfBlocks(v uint32)       { C.Struct(s).Set32(4, v) }
func (s MultiBlockFile) SizeOfLastBlock() uint32          { return C.Struct(s).Get32(8) }
func (s MultiBlockFile) SetSizeOfLastBlock(v uint32)      { C.Struct(s).Set32(8, v) }
func (s MultiBlockFile) ListOfBlocks() ObjectKey_List     { return ObjectKey_List(C.Struct(s).GetObject(0)) }
func (s MultiBlockFile) SetListOfBlocks(v ObjectKey_List) { C.Struct(s).SetObject(0, C.Object(v)) }
func (s MultiBlockFile) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"blockSize\":")
	if err != nil {
		return err
	}
	{
		s := s.BlockSize()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"numberOfBlocks\":")
	if err != nil {
		return err
	}
	{
		s := s.NumberOfBlocks()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"sizeOfLastBlock\":")
	if err != nil {
		return err
	}
	{
		s := s.SizeOfLastBlock()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"listOfBlocks\":")
	if err != nil {
		return err
	}
	{
		s := s.ListOfBlocks()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s MultiBlockFile) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s MultiBlockFile) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("blockSize = ")
	if err != nil {
		return err
	}
	{
		s := s.BlockSize()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("numberOfBlocks = ")
	if err != nil {
		return err
	}
	{
		s := s.NumberOfBlocks()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("sizeOfLastBlock = ")
	if err != nil {
		return err
	}
	{
		s := s.SizeOfLastBlock()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("listOfBlocks = ")
	if err != nil {
		return err
	}
	{
		s := s.ListOfBlocks()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s MultiBlockFile) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type MultiBlockFile_List C.PointerList

func NewMultiBlockFileList(s *C.Segment, sz int) MultiBlockFile_List {
	return MultiBlockFile_List(s.NewCompositeList(16, 1, sz))
}
func (s MultiBlockFile_List) Len() int { return C.PointerList(s).Len() }
func (s MultiBlockFile_List) At(i int) MultiBlockFile {
	return MultiBlockFile(C.PointerList(s).At(i).ToStruct())
}
func (s MultiBlockFile_List) ToArray() []MultiBlockFile {
	n := s.Len()
	a := make([]MultiBlockFile, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s MultiBlockFile_List) Set(i int, item MultiBlockFile) { C.PointerList(s).Set(i, C.Object(item)) }

type ExtendedAttribute C.Struct

func NewExtendedAttribute(s *C.Segment) ExtendedAttribute { return ExtendedAttribute(s.NewStruct(0, 2)) }
func NewRootExtendedAttribute(s *C.Segment) ExtendedAttribute {
	return ExtendedAttribute(s.NewRootStruct(0, 2))
}
func AutoNewExtendedAttribute(s *C.Segment) ExtendedAttribute {
	return ExtendedAttribute(s.NewStructAR(0, 2))
}
func ReadRootExtendedAttribute(s *C.Segment) ExtendedAttribute {
	return ExtendedAttribute(s.Root(0).ToStruct())
}
func (s ExtendedAttribute) Name() string      { return C.Struct(s).GetObject(0).ToText() }
func (s ExtendedAttribute) NameBytes() []byte { return C.Struct(s).GetObject(0).ToDataTrimLastByte() }
func (s ExtendedAttribute) SetName(v string)  { C.Struct(s).SetObject(0, s.Segment.NewText(v)) }
func (s ExtendedAttribute) Id() ObjectKey     { return ObjectKey(C.Struct(s).GetObject(1).ToStruct()) }
func (s ExtendedAttribute) SetId(v ObjectKey) { C.Struct(s).SetObject(1, C.Object(v)) }
func (s ExtendedAttribute) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"name\":")
	if err != nil {
		return err
	}
	{
		s := s.Name()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"id\":")
	if err != nil {
		return err
	}
	{
		s := s.Id()
		err = s.WriteJSON(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s ExtendedAttribute) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s ExtendedAttribute) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("name = ")
	if err != nil {
		return err
	}
	{
		s := s.Name()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("id = ")
	if err != nil {
		return err
	}
	{
		s := s.Id()
		err = s.WriteCapLit(b)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s ExtendedAttribute) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type ExtendedAttribute_List C.PointerList

func NewExtendedAttributeList(s *C.Segment, sz int) ExtendedAttribute_List {
	return ExtendedAttribute_List(s.NewCompositeList(0, 2, sz))
}
func (s ExtendedAttribute_List) Len() int { return C.PointerList(s).Len() }
func (s ExtendedAttribute_List) At(i int) ExtendedAttribute {
	return ExtendedAttribute(C.PointerList(s).At(i).ToStruct())
}
func (s ExtendedAttribute_List) ToArray() []ExtendedAttribute {
	n := s.Len()
	a := make([]ExtendedAttribute, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s ExtendedAttribute_List) Set(i int, item ExtendedAttribute) {
	C.PointerList(s).Set(i, C.Object(item))
}

type ExtendedAttributes C.Struct

func NewExtendedAttributes(s *C.Segment) ExtendedAttributes {
	return ExtendedAttributes(s.NewStruct(8, 1))
}
func NewRootExtendedAttributes(s *C.Segment) ExtendedAttributes {
	return ExtendedAttributes(s.NewRootStruct(8, 1))
}
func AutoNewExtendedAttributes(s *C.Segment) ExtendedAttributes {
	return ExtendedAttributes(s.NewStructAR(8, 1))
}
func ReadRootExtendedAttributes(s *C.Segment) ExtendedAttributes {
	return ExtendedAttributes(s.Root(0).ToStruct())
}
func (s ExtendedAttributes) NumAttributes() uint32     { return C.Struct(s).Get32(0) }
func (s ExtendedAttributes) SetNumAttributes(v uint32) { C.Struct(s).Set32(0, v) }
func (s ExtendedAttributes) Attributes() ExtendedAttribute_List {
	return ExtendedAttribute_List(C.Struct(s).GetObject(0))
}
func (s ExtendedAttributes) SetAttributes(v ExtendedAttribute_List) {
	C.Struct(s).SetObject(0, C.Object(v))
}
func (s ExtendedAttributes) WriteJSON(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('{')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"numAttributes\":")
	if err != nil {
		return err
	}
	{
		s := s.NumAttributes()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(',')
	if err != nil {
		return err
	}
	_, err = b.WriteString("\"attributes\":")
	if err != nil {
		return err
	}
	{
		s := s.Attributes()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteJSON(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte('}')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s ExtendedAttributes) MarshalJSON() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteJSON(&b)
	return b.Bytes(), err
}
func (s ExtendedAttributes) WriteCapLit(w io.Writer) error {
	b := bufio.NewWriter(w)
	var err error
	var buf []byte
	_ = buf
	err = b.WriteByte('(')
	if err != nil {
		return err
	}
	_, err = b.WriteString("numAttributes = ")
	if err != nil {
		return err
	}
	{
		s := s.NumAttributes()
		buf, err = json.Marshal(s)
		if err != nil {
			return err
		}
		_, err = b.Write(buf)
		if err != nil {
			return err
		}
	}
	_, err = b.WriteString(", ")
	if err != nil {
		return err
	}
	_, err = b.WriteString("attributes = ")
	if err != nil {
		return err
	}
	{
		s := s.Attributes()
		{
			err = b.WriteByte('[')
			if err != nil {
				return err
			}
			for i, s := range s.ToArray() {
				if i != 0 {
					_, err = b.WriteString(", ")
				}
				if err != nil {
					return err
				}
				err = s.WriteCapLit(b)
				if err != nil {
					return err
				}
			}
			err = b.WriteByte(']')
		}
		if err != nil {
			return err
		}
	}
	err = b.WriteByte(')')
	if err != nil {
		return err
	}
	err = b.Flush()
	return err
}
func (s ExtendedAttributes) MarshalCapLit() ([]byte, error) {
	b := bytes.Buffer{}
	err := s.WriteCapLit(&b)
	return b.Bytes(), err
}

type ExtendedAttributes_List C.PointerList

func NewExtendedAttributesList(s *C.Segment, sz int) ExtendedAttributes_List {
	return ExtendedAttributes_List(s.NewCompositeList(8, 1, sz))
}
func (s ExtendedAttributes_List) Len() int { return C.PointerList(s).Len() }
func (s ExtendedAttributes_List) At(i int) ExtendedAttributes {
	return ExtendedAttributes(C.PointerList(s).At(i).ToStruct())
}
func (s ExtendedAttributes_List) ToArray() []ExtendedAttributes {
	n := s.Len()
	a := make([]ExtendedAttributes, n)
	for i := 0; i < n; i++ {
		a[i] = s.At(i)
	}
	return a
}
func (s ExtendedAttributes_List) Set(i int, item ExtendedAttributes) {
	C.PointerList(s).Set(i, C.Object(item))
}
