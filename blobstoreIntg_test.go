// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
// The set of tests in this file are for integration testing
// of ether with scyllabd and gocql library.

// +build integration

package cql

import (
	"testing"

	"github.com/aristanetworks/ether"
	"github.com/stretchr/testify/suite"
)

func checkSetupIntg(suite *BlobStoreIntgTestSuite) {
	if suite.bls == nil {
		suite.T().Skip("Blobstore was not setup")
	}
}

type BlobStoreIntgTestSuite struct {
	suite.Suite
	bls ether.BlobStore
}

// TODO:Currently the test targets a "shared" cluster
// and expects that the keyspace and table are already setup.
// Once the integration tests are managed using jenkins and
// containers, the test should be modified to run against its i
// "private" cluster instance
func (suite *BlobStoreIntgTestSuite) SetupSuite() {

	var err ether.ErrorResponse
	suite.bls, err = NewCqlBlobStore(hwConfFile)

	suite.Require().Equal(ether.ErrOk, err.ErrorCode,
		"NewCqlBlobStore returned an error:: "+err.Error())

	suite.Require().NotEqual(nil, suite.bls,
		"NewCqlBlobStore returned nil")
}

func (suite *BlobStoreIntgTestSuite) SetupTest() {
	checkSetupIntg(suite)
}

func (suite *BlobStoreIntgTestSuite) TestInsert() {

	err := suite.bls.Insert(testKey, []byte(testValue), nil)
	suite.Require().Equal(ether.ErrOk, err.ErrorCode,
		"Insert returned an error:: "+err.Error())
}

func (suite *BlobStoreIntgTestSuite) TestGet() {

	value, metadata, err := suite.bls.Get(testKey)
	suite.Require().Equal(ether.ErrOk, err.ErrorCode,
		"Get returned an error::  "+err.Error())
	suite.Require().Equal(testValue, string(value), "Get returned incorrect value")
	suite.Require().Equal(map[string]string(nil), metadata,
		"Get returned incorrect metadata")
}

func (suite *BlobStoreIntgTestSuite) TestGetInvalidKey() {

	value, metadata, err := suite.bls.Get(unknownKey)
	suite.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	suite.Require().Nil(metadata, "value was not Nil when error is ErrKeyNotFound")
	suite.Require().Equal(ether.ErrKeyNotFound, err.ErrorCode,
		"Get returned incorrect error")

}

func TestBlobStoreIntgtTestSuite(t *testing.T) {
	suite.Run(t, new(BlobStoreIntgTestSuite))
}

// The TearDownTest method will be run after every test in the suite.
func (suite *BlobStoreIntgTestSuite) TearDownTest() {
	// nop
}

// The TearDownSuite method will be run after Suite is done
func (suite *BlobStoreIntgTestSuite) TearDownSuite() {
	resetCqlStore()
}
