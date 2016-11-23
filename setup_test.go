// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type CqlStoreUnitTestSuite struct {
	suite.Suite
	s rand.Source
	r *rand.Rand
}

func (suite *CqlStoreUnitTestSuite) SetupSuite() {
	suite.s = rand.NewSource(time.Now().UnixNano())
	suite.r = rand.New(suite.s)
}

func (suite *CqlStoreUnitTestSuite) SetupTest() {
	//nop
}

func (suite *CqlStoreUnitTestSuite) TestInvalidConfigFilePath() {
	var config Config

	config.Nodes = []string{"node1", "node2"}

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	suite.Require().NoError(err, "Tempfile creation failed")
	name := file.Name()
	file.Close()
	defer os.Remove(name)

	err = writeCqlConfig(name, &config)
	suite.Require().NoError(err, "CQL config file write failed")

	// Garble the config file name
	name += strconv.Itoa(suite.r.Int())
	bls, err := NewCqlBlobStore(name)
	suite.Require().Error(err)
	suite.Require().Equal(bls, nil, "bls should be nil but is not")
}

func (suite *CqlStoreUnitTestSuite) TestInvalidConfigFilePerms() {
	var config Config

	config.Nodes = []string{"node1", "node2"}

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	suite.Require().NoError(err, "Tempfile creation failed")
	name := file.Name()
	file.Close()
	defer os.Remove(name)

	err = writeCqlConfig(name, &config)
	suite.Require().NoError(err, "CQL config file write failed")

	// Modify config file Perms
	err = os.Chmod(name, 0000)
	suite.Require().NoError(err, "Error in changing file perms")
	defer os.Chmod(name, 0666) //-rw-rw-rw-

	bls, err := NewCqlBlobStore(name)
	suite.Require().Error(err)
	suite.Require().Equal(bls, nil, "bls should be nil but is not")
}

func (suite *CqlStoreUnitTestSuite) TestInvalidConfigFormat() {
	var config Config

	config.Nodes = []string{"node1", "node2"}

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	suite.Require().NoError(err, "Tempfile creation failed")
	name := file.Name()
	file.Close()
	defer os.Remove(name)

	err = writeCqlConfig(name, &config)
	suite.Require().NoError(err, "CQL config file write failed")

	// Write some small garbage to the file
	file, err = os.OpenFile(name, os.O_RDWR, 0777)
	suite.Require().NoError(err, "CQL config file open failed")

	var length int
	garbage := []byte("boo")
	length, err = file.Write(garbage)
	suite.Require().NoError(err, "CQL config file write failed")
	suite.Require().Equal(length, len(garbage), "CQL config file write incorrect")

	bls, err := NewCqlBlobStore(name)
	suite.Require().Error(err)
	suite.Require().Equal(bls, nil, "bls should be nil but is not")
}

func TestCqlStoreUnitTestSuite(t *testing.T) {
	suite.Run(t, new(CqlStoreUnitTestSuite))
}

func (suite *CqlStoreUnitTestSuite) TearDownTest() {
	resetCqlStore()
}

func (suite *CqlStoreUnitTestSuite) TearDownSuite() {
	//nop
}
