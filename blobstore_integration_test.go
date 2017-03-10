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

type storeIntegrationTests struct {
	suite.Suite
	bls blobstore.BlobStore
}

func checkSetupIntg(s *storeIntegrationTests) {
	if s.bls == nil {
		s.T().Skip("Blobstore was not setup")
	}
}

func (s *storeIntegrationTests) SetupSuite() {
	confFile, err := EtherConfFile()
	s.Require().NoError(err, "error in getting ether configuration file")

	err = SetupTestSchema(confFile)
	s.Require().NoError(err, "SetupSchema returned an error")

	// Establish connection with the cluster
	s.bls, err = NewCqlBlobStore(confFile)
	s.Require().NoError(err, "NewCqlBlobStore returned an error")
	s.Require().NotNil(s.bls, "NewCqlBlobStore returned nil")
}

func (s *storeIntegrationTests) SetupTest() {
	checkSetupIntg(s)
}

func (s *storeIntegrationTests) TestInsert() {
	err := s.bls.Insert(testKey, []byte(testValue),
		map[string]string{TimeToLive: "0"})
	s.Require().NoError(err, "Insert returned an error")
}

func (s *storeIntegrationTests) TestInsertParallel() {
	ctx := context.Background()
	Wg, _ := errgroup.WithContext(ctx)

	for count := 0; count < 2; count++ {
		countl := count
		Wg.Go(func() error {

			return s.bls.Insert(testKey+strconv.Itoa(countl), []byte(testValue),
				map[string]string{TimeToLive: "0"})
		})
	}
	err := Wg.Wait()
	s.Require().NoError(err, "Insert returned an error")

	// Check
	for count := 0; count < 2; count++ {
		value, _, err := s.bls.Get(testKey + strconv.Itoa(count))
		s.Require().NoError(err, "Insert returned an error")
		s.Require().Equal(testValue, string(value), "Get returned in correct value")
	}
}

func (s *storeIntegrationTests) TestGet() {
	err := s.bls.Insert(testKey, []byte(testValue),
		map[string]string{TimeToLive: "0"})
	s.Require().NoError(err, "Insert returned an error")

	value, metadata, err := s.bls.Get(testKey)
	s.Require().NoError(err, "Get returned an error")
	s.Require().Equal(testValue, string(value), "Get returned incorrect value")
	s.Require().NotNil(metadata, "Get returned incorrect metadata")
	s.Require().Contains(metadata, TimeToLive,
		"Metadata doesn't contain expected key TimeToLive")
	s.Require().Equal("0", metadata[TimeToLive],
		"Metadata contains unexpected value for TimeToLive")
}

func (s *storeIntegrationTests) TestGetUnknownKey() {
	value, metadata, err := s.bls.Get(unknownKey)
	s.Require().Nil(value, "value was not Nil when error is ErrKeyNotFound")
	s.Require().Nil(metadata, "metadata was not Nil when error is ErrKeyNotFound")
	s.Require().Error(err, "Get returned incorrect error")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Get is of type %T", err))
	s.Require().Equal(blobstore.ErrKeyNotFound, verr.Code, "Invalid Error Code from Get")
}

func (s *storeIntegrationTests) TestGetNonZeroTTL() {
	err := s.bls.Insert(testKey, []byte(testValue),
		map[string]string{TimeToLive: "1234"})
	s.Require().NoError(err, "Insert returned an error")

	value, metadata, err := s.bls.Get(testKey)
	s.Require().NoError(err, "Get returned an error")
	s.Require().Equal(testValue, string(value), "Get returned incorrect value")
	s.Require().NotNil(metadata, "Get returned incorrect metadata")
	s.Require().Contains(metadata, TimeToLive,
		"Metadata doesn't contain expected key TimeToLive")
	s.Require().Condition(func() bool {
		i, err := strconv.Atoi(metadata[TimeToLive])
		if err != nil || i <= 0 || i > 1234 {
			return false
		}
		return true
	},
		"Metadata contains expected:%s actual:%s for TimeToLive",
		"0<TTL<=1234", metadata[TimeToLive])
}

func (s *storeIntegrationTests) TestMetadataOK() {
	err := s.bls.Insert(testKey, []byte(testValue),
		map[string]string{TimeToLive: "1234"})
	s.Require().NoError(err, "Insert returned an error")

	metadata, err := s.bls.Metadata(testKey)
	s.Require().NoError(err, "Metadata returned an error")
	s.Require().NotNil(metadata, "Metadata returned incorrect metadata")
	s.Require().Contains(metadata, TimeToLive,
		"Metadata doesn't contain expected key TimeToLive")
	s.Require().Condition(func() bool {
		i, err := strconv.Atoi(metadata[TimeToLive])
		if err != nil || i <= 0 || i > 1234 {
			return false
		}
		return true
	},
		"Metadata contains expected:%s actual:%s for TimeToLive",
		"0<TTL<=1234", metadata[TimeToLive])
}

func (s *storeIntegrationTests) TestMetadataUnknownKey() {
	metadata, err := s.bls.Metadata(unknownKey)
	s.Require().Error(err, "Metadata didn't return error")
	s.Require().Nil(metadata, "metadata was not Nil when error is ErrKeyNotFound")
	verr, ok := err.(*blobstore.Error)
	s.Require().Equal(true, ok, fmt.Sprintf("Error from Metadata is of type %T", err))
	s.Require().Equal(blobstore.ErrKeyNotFound, verr.Code, "Invalid Error Code from Metadata")
}

func TestStoreIntg(t *testing.T) {
	suite.Run(t, &storeIntegrationTests{})
}

// The TearDownTest method will be run after every test in the suite.
func (s *storeIntegrationTests) TearDownTest() {
}

// The TearDownSuite method will be run after Suite is done
func (s *storeIntegrationTests) TearDownSuite() {

	confFile, err := EtherConfFile()
	s.Require().NoError(err, "error in getting ether configuration file")

	resetCqlStore()
	_ = TearDownTestSchema(confFile)
}
