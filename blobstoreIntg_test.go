// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.
// The set of tests in this file are for integration testing
// of ether with scyllabd and gocql library.

// +build integration

package cql

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/aristanetworks/ether/blobstore"
	"github.com/stretchr/testify/suite"
	"golang.org/x/sync/errgroup"
)

func checkSetupIntg(suite *BlobStoreIntgTestSuite) {
	if suite.bls == nil {
		suite.T().Skip("Blobstore was not setup")
	}
}

type BlobStoreIntgTestSuite struct {
	suite.Suite
	bls blobstore.BlobStore
}

// TODO:Currently the test targets a "shared" cluster
// and expects that the keyspace and table are already setup.
// Once the integration tests are managed using jenkins and
// containers, the test should be modified to run against its
// "private" cluster instance
func (suite *BlobStoreIntgTestSuite) SetupSuite() {

	var err error
	suite.bls, err = NewCqlBlobStore(hwConfFile)
	suite.Require().NoError(err, "NewCqlBlobStore returned an error")
	suite.Require().NotNil(suite.bls, "NewCqlBlobStore returned nil")
}

func (suite *BlobStoreIntgTestSuite) SetupTest() {
	checkSetupIntg(suite)
}

func (suite *BlobStoreIntgTestSuite) TestInsert() {

	err := suite.bls.Insert(testKey, []byte(testValue), nil)
	suite.Require().NoError(err, "Insert returned an error")
}

func (suite *BlobStoreIntgTestSuite) TestInsertParallel() {

	ctx := context.Background()
	Wg, _ := errgroup.WithContext(ctx)

	for count := 0; count < 2; count++ {
		countl := count
		Wg.Go(func() error {

			return suite.bls.Insert(testKey+strconv.Itoa(countl), []byte(testValue), nil)
		})
	}
	err := Wg.Wait()
	suite.Require().NoError(err, "Insert returned an error")

	// Check
	for count := 0; count < 2; count++ {
		value, _, err := suite.bls.Get(testKey + strconv.Itoa(count))
		suite.Require().NoError(err, "Insert returned an error")
		suite.Require().Equal(testValue, string(value), "Get returned in correct value")
	}
}

func (suite *BlobStoreIntgTestSuite) TestGet() {

	value, metadata, err := suite.bls.Get(testKey)
	suite.Require().NoError(err, "Get returned an error")
	suite.Require().Equal(testValue, string(value), "Get returned incorrect value")
	suite.Require().Equal(map[string]string(nil), metadata,
		"Get returned incorrect metadata")
}

func (suite *BlobStoreIntgTestSuite) TestGetInvalidKey() {

	value, metadata, err := suite.bls.Get(unknownKey)
	suite.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	suite.Require().Nil(metadata, "value was not Nil when error is ErrKeyNotFound")
	suite.Require().Error(err, "Get returned incorrect error")
	verr, ok := err.(*blobstore.Error)
	suite.Require().Equal(true, ok, fmt.Sprintf("Error from Get is of type %T", err))
	suite.Require().Equal(blobstore.ErrKeyNotFound, verr.Code, "Invalid Error Code from Get")
}

func TestBlobStoreIntgTestSuite(t *testing.T) {
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
