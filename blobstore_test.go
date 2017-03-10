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

	"github.com/aristanetworks/ether/blobstore"
	"github.com/aristanetworks/ether/utils/stats/inmem"
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
	store, err := initCqlStore(mockcc)
	s.Require().NoError(err, "initCqlStore Failed: ", err)

	s.bls = &cqlBlobStore{
		store:       &store,
		keyspace:    "ether",
		insertStats: inmem.NewOpStatsInMem("insertBlobStore"),
		getStats:    inmem.NewOpStatsInMem("getBlobStore"),
	}
	s.Require().NoError(err)

	_, ok := s.bls.store.session.(*MockSession)
	s.Require().Equal(true, ok,
		fmt.Sprintf("SetupTest has wrong type of session %T", s.bls.store.session))
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
	keyVal := []interface{}{testKey, []byte(testValue)}
	mocksession.On("Query", qstr, keyVal).Return(mockquery)

	mockquery.On("Exec").Return(nil)
	mocksession.On("Close").Return()
	err := s.bls.Insert(testKey, []byte(testValue),
		map[string]string{TimeToLive: "0"})

	s.Require().NoError(err, "Insert returned an error")
}

func (s *storeTests) TestInsertFailure() {
	mocksession := s.bls.store.session.(*MockSession)
	qstr := fmt.Sprintf(`INSERT
INTO %s.blobStore (key, value)
VALUES (?, ?)
USING TTL %s`, s.bls.keyspace, "0")
	keyVal := []interface{}{testKey, []byte(testValue)}

	mockquery := &MockQuery{}
	mocksession.On("Query", qstr, keyVal).Return(mockquery)
	mocksession.On("Close").Return()
	errVal := errors.New("Some random error")
	mockquery.On("Exec").Return(errVal)

	err := s.bls.Insert(testKey, []byte(testValue),
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
	key := []interface{}{testKey}
	qstr := fmt.Sprintf(`SELECT value, ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*[]uint8"),
		mock.AnythingOfType("*int")).Return(nil)

	_, metadata, err := s.bls.Get(testKey)
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
	key := []interface{}{unknownKey}
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*[]uint8"),
		mock.AnythingOfType("*int")).Return(gocql.ErrNotFound)

	// Verify return value for a non existent key
	value, metadata, err := s.bls.Get(unknownKey)
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
	key := []interface{}{unknownKey}
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*[]uint8"),
		mock.AnythingOfType("*int")).Return(gocql.ErrUnavailable)

	// Verify return value for a non existent key
	value, metadata, err := s.bls.Get(unknownKey)
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
	key := []interface{}{testKey}
	qstr := fmt.Sprintf(`SELECT value, ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()

	readMockTTL := func(dest ...interface{}) error {
		valPtr, _ := dest[1].(*int)
		*valPtr = 1122
		return nil
	}
	mockquery.On("Scan", mock.AnythingOfType("*[]uint8"),
		mock.AnythingOfType("*int")).Return(readMockTTL)

	_, metadata, err := s.bls.Get(testKey)
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
	key := []interface{}{testKey}
	qstr := fmt.Sprintf(`SELECT ttl(value)
FROM %s.blobStore
WHERE key = ?`, s.bls.keyspace)
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()

	readMockTTL := func(dest ...interface{}) error {
		valPtr, _ := dest[0].(*int)
		*valPtr = 1122
		return nil
	}
	mockquery.On("Scan", mock.AnythingOfType("*int")).Return(readMockTTL)

	metadata, err := s.bls.Metadata(testKey)
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
	key := []interface{}{unknownKey}
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*int")).Return(gocql.ErrNotFound)

	// Verify return value for a non existent key
	metadata, err := s.bls.Metadata(unknownKey)
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
	key := []interface{}{unknownKey}
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	mockquery.On("Scan", mock.AnythingOfType("*int")).Return(gocql.ErrUnavailable)

	metadata, err := s.bls.Metadata(unknownKey)
	s.Require().Error(err, "Metadata returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Metadata is of type %T", err))
	s.Require().Equal(blobstore.ErrOperationFailed, verr.Code,
		"Invalid Error Code from Metadata")
	s.Require().Nil(metadata, "metadata was not Nil when error is ErrUnavailable")
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
