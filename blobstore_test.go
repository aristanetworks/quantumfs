// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The set of tests in this file are for unit testing
// These tests talk to a mock implementation of the gocql
// library.
package cql

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type storeTests struct {
	suite.Suite
	bls *cqlBlobStore
}

func (s *storeTests) SetupSuite() {
}

func (s *storeTests) SetupTest() {

	// Set expectations
	mockcc := &MockCluster{}
	mocksession := &MockSession{}
	mocksession.On("Close").Return()
	mockcc.On("CreateSession").Return(mocksession, nil)
	mockIsTablePresent(mocksession, "blobStore", nil)

	mockCfg := &Config{
		Cluster: ClusterConfig{
			KeySpace: "ether",
		},
	}

	cqlBS, err := newCqlBS(mockcc, mockCfg)
	s.Require().NoError(err, "Failed %q newCqlBS", err)
	s.bls = cqlBS.(*cqlBlobStore)
}

func (s *storeTests) TestNewCqlStoreFailure() {

	// Since this is a setup Test
	resetCqlStore()
	// Set expectations
	mockcc := &MockCluster{}
	mocksession := &MockSession{}
	mocksession.On("Close").Return()
	mockcc.On("CreateSession").Return(mocksession, errors.New("initFailed"))
	_, err := initCqlStore(mockcc)

	s.Require().Error(err, "initCqlStore should have Failed")
}

func (s *storeTests) TestInsert() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`INSERT
INTO %s.blobStore (key, value)
VALUES (?, ?)
USING TTL %s`, s.bls.keyspace, "0")
	mocksession.On("Query", qstr,
		[]byte(testKey),
		[]byte(testValue)).Return(mockquery)

	mockquery.On("Exec").Return(nil)
	mocksession.On("Close").Return()
	err := s.bls.Insert(unitTestEtherCtx, []byte(testKey), []byte(testValue),
		map[string]string{TimeToLive: "0"})

	s.Require().NoError(err, "Insert returned an error")
}

func (s *storeTests) TestInsertFailure() {
	mocksession := s.bls.store.session.(*MockSession)
	qstr := fmt.Sprintf(`INSERT
INTO %s.blobStore (key, value)
VALUES (?, ?)
USING TTL %s`, s.bls.keyspace, "0")

	mockquery := &MockQuery{}
	mocksession.On("Query", qstr,
		[]byte(testKey),
		[]byte(testValue)).Return(mockquery)
	mocksession.On("Close").Return()
	errVal := errors.New("Some random error")
	mockquery.On("Exec").Return(errVal)

	err := s.bls.Insert(unitTestEtherCtx, []byte(testKey), []byte(testValue),
		map[string]string{TimeToLive: "0"})
	s.Require().Error(err, "Insert returned incorrect ErrorCode")

	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Insert is of type %T", err))
	s.Require().Equal(blobstore.ErrOperationFailed, verr.Code,
		"Invalid Error Code from Insert")
}

func (s *storeTests) TestGetNoErr() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	key := []byte(testKey)
	qstr := fmt.Sprintf(`SELECT value, ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*[]uint8"),
		mock.AnythingOfType("*int")).Return(nil)

	_, metadata, err := s.bls.Get(unitTestEtherCtx, []byte(testKey))
	s.Require().NoError(err, "Get returned an error")
	s.Require().NotNil(metadata, "Get returned incorrect metadata")
	s.Require().Contains(metadata, TimeToLive,
		"metadata doesn't contain expected key TimeToLive")
}

func (s *storeTests) TestGetFailureNoKey() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`SELECT value, ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	key := []byte(unknownKey)
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*[]uint8"),
		mock.AnythingOfType("*int")).Return(gocql.ErrNotFound)

	// Verify return value for a non existent key
	value, metadata, err := s.bls.Get(unitTestEtherCtx, []byte(unknownKey))
	s.Require().Error(err, "Get returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Get is of type %T", err))
	s.Require().Equal(blobstore.ErrKeyNotFound, verr.Code, "Invalid Error Code from get")
	s.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	s.Require().Nil(metadata, "metadata was not Nil when error is ErrKeyNotFound")
}

func (s *storeTests) TestGetFailureGeneric() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`SELECT value, ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	key := []byte(unknownKey)
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*[]uint8"),
		mock.AnythingOfType("*int")).Return(gocql.ErrUnavailable)

	// Verify return value for a non existent key
	value, metadata, err := s.bls.Get(unitTestEtherCtx, []byte(unknownKey))
	s.Require().Error(err, "Get returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Get is of type %T", err))
	s.Require().Equal(blobstore.ErrOperationFailed, verr.Code,
		"Invalid Error Code from Get")
	s.Require().Nil(value, "value was not Nil when error is ErrUnavailable")
	s.Require().Nil(metadata, "metadata was not Nil when error is ErrUnavailable")
}

func (s *storeTests) TestGetNonZeroTTL() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`SELECT value, ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, []byte(testKey)).Return(mockquery)
	mocksession.On("Close").Return()

	readMockTTL := func(dest ...interface{}) error {
		valPtr, _ := dest[1].(*int)
		*valPtr = 1122
		return nil
	}
	mockquery.On("Scan", mock.AnythingOfType("*[]uint8"),
		mock.AnythingOfType("*int")).Return(readMockTTL)

	_, metadata, err := s.bls.Get(unitTestEtherCtx, []byte(testKey))
	s.Require().NoError(err, "Get returned an error")
	s.Require().NotNil(metadata, "Get returned incorrect metadata")
	s.Require().Contains(metadata, TimeToLive,
		"metadata doesn't contain expected key TimeToLive")
	s.Require().Equal("1122", metadata[TimeToLive],
		"metadata contains unexpected value")
}

func (s *storeTests) TestMetadataOK() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`SELECT ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, []byte(testKey)).Return(mockquery)
	mocksession.On("Close").Return()

	readMockTTL := func(dest ...interface{}) error {
		valPtr, _ := dest[0].(*int)
		*valPtr = 1122
		return nil
	}
	mockquery.On("Scan", mock.AnythingOfType("*int")).Return(readMockTTL)

	metadata, err := s.bls.Metadata(unitTestEtherCtx, []byte(testKey))
	s.Require().NoError(err, "Metadata returned an error")
	s.Require().NotNil(metadata, "Metadata returned incorrect metadata")
	s.Require().Contains(metadata, TimeToLive,
		"metadata doesn't contain expected key TimeToLive")
	s.Require().Equal("1122", metadata[TimeToLive],
		"metadata contains unexpected value")
}

func (s *storeTests) TestMetadataFailNoKey() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`SELECT ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, []byte(unknownKey)).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*int")).Return(gocql.ErrNotFound)

	// Verify return value for a non existent key
	metadata, err := s.bls.Metadata(unitTestEtherCtx, []byte(unknownKey))
	s.Require().Error(err, "Metadata returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Metadata is of type %T", err))
	s.Require().Equal(blobstore.ErrKeyNotFound, verr.Code,
		"Invalid Error Code from Metadata")
	s.Require().Nil(metadata, "metadata was not Nil when error is ErrKeyNotFound")
}

func (s *storeTests) TestMetadataFailGeneric() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`SELECT ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, []byte(unknownKey)).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*int")).Return(gocql.ErrUnavailable)

	metadata, err := s.bls.Metadata(unitTestEtherCtx, []byte(unknownKey))
	s.Require().Error(err, "Metadata returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Metadata is of type %T", err))
	s.Require().Equal(blobstore.ErrOperationFailed, verr.Code,
		"Invalid Error Code from Metadata")
	s.Require().Nil(metadata, "metadata was not Nil when error is ErrUnavailable")
}

func (s *storeTests) TestGetExtKeyInfoOK() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`SELECT ttl(value), writetime(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, []byte(testKey)).Return(mockquery)
	mocksession.On("Close").Return()

	secs := int64(2)
	ttl := 99
	readMockData := func(dest ...interface{}) error {
		valPtr, _ := dest[0].(*int)
		*valPtr = ttl
		ivalPtr, _ := dest[1].(*int64)
		*ivalPtr = secs * int64(time.Second/time.Microsecond)
		return nil
	}
	mockquery.On("Scan", mock.AnythingOfType("*int"),
		mock.AnythingOfType("*int64")).Return(readMockData)

	info, err := s.bls.GetExtKeyInfo(unitTestEtherCtx, []byte(testKey))
	s.Require().NoError(err, "GetExtKeyInfo returned error %s on failure", err)
	s.Require().Equal(ttl, info.TTL, "TTL mismatch")
	s.Require().Equal(secs, info.WriteTime.Unix(), "WriteTime mismatch")
}

func (s *storeTests) TestGetExtKeyInfoErr() {
	mocksession := s.bls.store.session.(*MockSession)
	mockquery := &MockQuery{}
	qstr := fmt.Sprintf(`SELECT ttl(value), writetime(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, []byte(testKey)).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*int"),
		mock.AnythingOfType("*int64")).Return(gocql.ErrNotFound)

	_, err := s.bls.GetExtKeyInfo(unitTestEtherCtx, []byte(testKey))
	s.Require().Error(err, "GetExtKeyInfo did not return error")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from GetExtKeyInfo is of type %T", err))
	s.Require().Equal(blobstore.ErrKeyNotFound, verr.Code,
		"Invalid Error Code %d from GetExtKeyInfo", verr.Code)
}

func TestStore(t *testing.T) {
	suite.Run(t, &storeTests{})
}

// The TearDownTest method will be run after every test in the suite.
func (s *storeTests) TearDownTest() {
	resetCqlStore()
}

// The TearDownSuite method will be run after Suite is done
func (s *storeTests) TearDownSuite() {
}
