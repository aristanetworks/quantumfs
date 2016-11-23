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

var mocking = true

type BlobStoreUnitTestSuite struct {
	suite.Suite
	bls *cqlBlobStore
}

func (suite *BlobStoreUnitTestSuite) SetupSuite() {
}

func (suite *BlobStoreUnitTestSuite) SetupTest() {

	// Set expectations
	mockcc := new(MockCluster)
	mocksession := new(MockSession)
	mocksession.On("Close").Return()
	mockcc.On("CreateSession").Return(mocksession, nil)
	store, err := initCqlStore(mockcc, mocking)
	suite.Require().NoError(err, "initCqlStore Failed: ", err)

	suite.bls = &cqlBlobStore{
		store: &store,
	}
	suite.Require().NoError(err)
}

func (suite *BlobStoreUnitTestSuite) TestNewCqlStoreFailure() {

	// Since this is a setup Test
	resetCqlStore()
	// Set expectations
	mockcc := new(MockCluster)
	mocksession := new(MockSession)
	mocksession.On("Close").Return()
	mockcc.On("CreateSession").Return(mocksession, errors.New("initFailed"))
	_, err := initCqlStore(mockcc, mocking)

	suite.Require().Error(err, "initCqlStore should have Failed")
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

	suite.Require().NoError(err, "Insert returned an error")
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
	suite.Require().Error(err, "Insert returned incorrect ErrorCode")

	verr, ok := err.(*blobstore.Error)
	suite.Require().Equal(true, ok, fmt.Sprintf("Error from Insert is of type %T", err))
	suite.Require().Equal(blobstore.ErrOperationFailed, verr.Code,
		"Invalid Error Code from Insert")
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
	suite.Require().NoError(err, "Get returned an error")
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
	suite.Require().Error(err, "Get returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	suite.Require().Equal(true, ok, fmt.Sprintf("Error from Get is of type %T", err))
	suite.Require().Equal(blobstore.ErrKeyNotFound, verr.Code, "Invalid Error Code from get")
	suite.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	suite.Require().Nil(metadata, "value was not Nil when error is ErrKeyNotFound")

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
	suite.Require().Error(err, "Get returned nil error on failure")
	verr, ok := err.(*blobstore.Error)
	suite.Require().Equal(true, ok, fmt.Sprintf("Error from Get is of type %T", err))
	suite.Require().Equal(blobstore.ErrOperationFailed, verr.Code,
		"Invalid Error Code from Get")
	suite.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	suite.Require().Nil(metadata, "value was not Nil when error is ErrKeyNotFound")
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
