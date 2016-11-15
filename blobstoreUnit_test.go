// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
// The set of tests in this file are for unit testing
// These tests talk to a mock implementation of the gocql
// library.

package cql

import (
	"errors"
	"testing"

	"github.com/aristanetworks/ether"
	"github.com/gocql/gocql"
	"github.com/stretchr/testify/suite"
)

var mocking = true

type BlobStoreUnitTestSuite struct {
	suite.Suite
	bls *cqlBlobStore
}

func (suite *BlobStoreUnitTestSuite) SetupSuite() {
}

func (suite *BlobStoreUnitTestSuite) SetupTest() {
	var etherr ether.ErrorResponse

	// Set expectations
	mockcc := new(MockCluster)
	mocksession := new(MockSession)
	mocksession.On("Close").Return()
	mockcc.On("CreateSession").Return(mocksession, nil)
	store, err := initCqlStore(mockcc, mocking)
	suite.Require().Equal(err, nil, "initCqlStore Failed: ", err)

	suite.bls = &cqlBlobStore{
		store: &store,
	}
	suite.Require().Equal(
		ether.ErrorResponse{ErrorCode: 0, ErrorMessage: "", Internal: error(nil)},
		etherr, "NewCqlStore returned an error:: "+etherr.Error())
}

func (suite *BlobStoreUnitTestSuite) TestNewCqlStoreFailure() {
	var etherr ether.ErrorResponse

	// Since this is a setup Test
	resetCqlStore()
	// Set expectations
	mockcc := new(MockCluster)
	mocksession := new(MockSession)
	mocksession.On("Close").Return()
	mockcc.On("CreateSession").Return(mocksession, errors.New("initFailed"))
	_, err := initCqlStore(mockcc, mocking)

	suite.Require().NotEqual(err, nil, "initCqlStore should have Failed: ", err)

	suite.Require().Equal(
		ether.ErrorResponse{ErrorCode: 0, ErrorMessage: "", Internal: error(nil)},
		etherr, "NewCqlStore returned an error:: "+etherr.Error())
}

func (suite *BlobStoreUnitTestSuite) TestInsert() {

	var mocksession *MockSession
	if v, ok := suite.bls.store.session.(*MockSession); ok {
		mocksession = v
	}
	mockquery := new(MockQuery)
	qstr := "INSERT into blobStore (key, value) VALUES (?, ?)"
	keyVal := []interface{}{testKey, []byte(testValue)}
	mocksession.On("Query", qstr, keyVal).Return(mockquery)

	mockquery.On("Exec").Return(nil)
	mocksession.On("Close").Return()
	err := suite.bls.Insert(testKey, []byte(testValue), nil)

	suite.Require().Equal(ether.ErrOk, err.ErrorCode,
		"Insert returned an error:: "+err.Error())
}

func (suite *BlobStoreUnitTestSuite) TestInsertFailure() {
	var mocksession *MockSession
	if v, ok := suite.bls.store.session.(*MockSession); ok {
		mocksession = v
	}

	qstr := "INSERT into blobStore (key, value) VALUES (?, ?)"
	keyVal := []interface{}{testKey, []byte(testValue)}

	mockquery := new(MockQuery)
	mocksession.On("Query", qstr, keyVal).Return(mockquery)
	mocksession.On("Close").Return()
	errVal := errors.New("Some random error")
	mockquery.On("Exec").Return(errVal)
	err := suite.bls.Insert(testKey, []byte(testValue), nil)
	suite.Require().Equal(ether.ErrOperationFailed, err.ErrorCode,
		"Insert returned incorrect ErrorCode")
	suite.Require().Equal(errVal, err.Internal, "Insert returned incorrect Err")

}

func (suite *BlobStoreUnitTestSuite) TestGetNoErr() {
	var mocksession *MockSession
	if v, ok := suite.bls.store.session.(*MockSession); ok {
		mocksession = v
	}
	mockquery := new(MockQuery)
	key := []interface{}{testKey}
	qstr := "SELECT value FROM blobStore WHERE key = ?"
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	var valuef []byte
	val := []interface{}{&valuef}
	mockquery.On("Scan", val).Return(nil)

	_, metadata, err := suite.bls.Get(testKey)
	suite.Require().Equal(ether.ErrOk, err.ErrorCode,
		"Get returned an error::  "+err.Error())
	suite.Require().Equal(map[string]string(nil), metadata,
		"Get returned incorrect metadata")
}

func (suite *BlobStoreUnitTestSuite) TestGetFailureNoKey() {
	var mocksession *MockSession
	if v, ok := suite.bls.store.session.(*MockSession); ok {
		mocksession = v
	}
	mockquery := new(MockQuery)
	qstr := "SELECT value FROM blobStore WHERE key = ?"
	key := []interface{}{unknownKey}
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	var valuef []byte
	val := []interface{}{&valuef}
	mockquery.On("Scan", val).Return(gocql.ErrNotFound)

	// Verify return value for a non existent key
	value, metadata, err := suite.bls.Get(unknownKey)
	suite.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	suite.Require().Nil(metadata, "value was not Nil when error is ErrKeyNotFound")
	suite.Require().Equal(ether.ErrKeyNotFound, err.ErrorCode,
		"Get returned incorrect error")
}

func (suite *BlobStoreUnitTestSuite) TestGetFailureGeneric() {
	var mocksession *MockSession
	if v, ok := suite.bls.store.session.(*MockSession); ok {
		mocksession = v
	}
	mockquery := new(MockQuery)
	qstr := "SELECT value FROM blobStore WHERE key = ?"
	key := []interface{}{unknownKey}
	mocksession.On("Query", qstr, key).Return(mockquery)
	mocksession.On("Close").Return()
	var valuef []byte
	val := []interface{}{&valuef}
	mockquery.On("Scan", val).Return(gocql.ErrUnavailable)

	// Verify return value for a non existent key
	value, metadata, err := suite.bls.Get(unknownKey)
	suite.Require().Nil(value, "value was not Nil when error is ErrUnavailable")
	suite.Require().Nil(metadata, "value was not Nil when error is ErrKeyUnavailable")
	suite.Require().Equal(ether.ErrOperationFailed, err.ErrorCode,
		"Get returned incorrect error")

}

func TestBlobStoreUnitTestSuite(t *testing.T) {
	suite.Run(t, new(BlobStoreUnitTestSuite))
}

// The TearDownTest method will be run after every test in the suite.
func (suite *BlobStoreUnitTestSuite) TearDownTest() {
	resetCqlStore()
}

// The TearDownSuite method will be run after Suite is done
func (suite *BlobStoreUnitTestSuite) TearDownSuite() {
}
