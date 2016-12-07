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
	"github.com/gocql/gocql"
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
		store:    &store,
		keyspace: "ether",
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

	var mocksession *MockSession
	if v, ok := s.bls.store.session.(*MockSession); ok {
		mocksession = v
	}
	mockquery := &MockQuery{}
	qstr := "INSERT into ether.blobStore (key, value) VALUES (?, ?)"
	keyVal := []interface{}{testKey, []byte(testValue)}
	mocksession.On("Query", qstr, keyVal).Return(mockquery)

	mockquery.On("Exec").Return(nil)
	mocksession.On("Close").Return()
	err := s.bls.Insert(testKey, []byte(testValue), nil)

	s.Require().NoError(err, "Insert returned an error")
}

func (s *storeTests) TestInsertFailure() {
	var mocksession *MockSession
	if v, ok := s.bls.store.session.(*MockSession); ok {
		mocksession = v
	}

	qstr := "INSERT into ether.blobStore (key, value) VALUES (?, ?)"
	keyVal := []interface{}{testKey, []byte(testValue)}

	mockquery := &MockQuery{}
	mocksession.On("Query", qstr, keyVal).Return(mockquery)
	mocksession.On("Close").Return()
	errVal := errors.New("Some random error")
	mockquery.On("Exec").Return(errVal)

	err := s.bls.Insert(testKey, []byte(testValue), nil)
	s.Require().Error(err, "Insert returned incorrect ErrorCode")

	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Insert is of type %T", err))
	s.Require().Equal(blobstore.ErrOperationFailed, verr.Code,
		"Invalid Error Code from Insert")
}

func (s *storeTests) TestGetNoErr() {
	var mocksession *MockSession
	if v, ok := s.bls.store.session.(*MockSession); ok {
		mocksession = v
	}
	mockquery := &MockQuery{}
	key := []interface{}{testKey}
	qstr := "SELECT value FROM ether.blobStore WHERE key = ?"
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	var valuef []byte
	mockquery.On("Scan", &valuef).Return(nil)

	_, metadata, err := s.bls.Get(testKey)
	s.Require().NoError(err, "Get returned an error")
	s.Require().Equal(map[string]string(nil), metadata,
		"Get returned incorrect metadata")
}

func (s *storeTests) TestGetFailureNoKey() {
	var mocksession *MockSession
	if v, ok := s.bls.store.session.(*MockSession); ok {
		mocksession = v
	}
	mockquery := &MockQuery{}
	qstr := "SELECT value FROM ether.blobStore WHERE key = ?"
	key := []interface{}{unknownKey}
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	var valuef []byte
	mockquery.On("Scan", &valuef).Return(gocql.ErrNotFound)

	// Verify return value for a non existent key
	value, metadata, err := s.bls.Get(unknownKey)
	s.Require().Error(err, "Get returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Get is of type %T", err))
	s.Require().Equal(blobstore.ErrKeyNotFound, verr.Code, "Invalid Error Code from get")
	s.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	s.Require().Nil(metadata, "value was not Nil when error is ErrKeyNotFound")

}

func (s *storeTests) TestGetFailureGeneric() {
	var mocksession *MockSession
	if v, ok := s.bls.store.session.(*MockSession); ok {
		mocksession = v
	}
	mockquery := &MockQuery{}
	qstr := "SELECT value FROM ether.blobStore WHERE key = ?"
	key := []interface{}{unknownKey}
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	var valuef []byte
	mockquery.On("Scan", &valuef).Return(gocql.ErrUnavailable)

	// Verify return value for a non existent key
	value, metadata, err := s.bls.Get(unknownKey)
	s.Require().Error(err, "Get returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Get is of type %T", err))
	s.Require().Equal(blobstore.ErrOperationFailed, verr.Code,
		"Invalid Error Code from Get")
	s.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	s.Require().Nil(metadata, "value was not Nil when error is ErrKeyNotFound")
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
