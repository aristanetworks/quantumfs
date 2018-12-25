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

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/suite"
)

type setupTests struct {
	suite.Suite
	src rand.Source
	r   *rand.Rand
}

func (s *setupTests) SetupSuite() {
	s.src = rand.NewSource(time.Now().UnixNano())
	s.r = rand.New(s.src)
}

func (s *setupTests) SetupTest() {
	//nop
}

func (s *setupTests) TestCFNamePrefix() {
	tests := []struct {
		name  string
		value string
		pass  bool
	}{
		{"empty val", "", false},
		{"val with special chars", "a$#c", false},
		{"correct val", "a9A0_1", true},
		{"val with > 30 chars", "99999999999999999999999999999999", false},
	}

	for _, test := range tests {
		actual := true
		if err := checkCfNamePrefix(test.value); err != nil {
			actual = false
		}
		s.Require().Equal(test.pass, actual, "Failed test %q", test.name)
	}
}

func (s *setupTests) TestValidConfig() {
	var config Config
	var config2 *Config

	config.Cluster.Nodes = []string{"node1", "node2"}
	config.WsDB.CacheTimeoutSecs = 99

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	s.Require().NoError(err, "Tempfile creation failed")
	name := file.Name()
	file.Close()
	defer os.Remove(name)

	err = writeCqlConfig(name, &config)
	s.Require().NoError(err, "CQL config file write failed")

	config2, err = readCqlConfig(name)
	s.Require().NoError(err, "CQL config file read failed")

	s.Require().Equal(config, *config2,
		"The config read was not the same as the config written")
}

func (s *setupTests) TestInvalidConfigFilePath() {
	var config Config

	config.Cluster.Nodes = []string{"node1", "node2"}

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	s.Require().NoError(err, "Tempfile creation failed")
	name := file.Name()
	file.Close()
	defer os.Remove(name)

	err = writeCqlConfig(name, &config)
	s.Require().NoError(err, "CQL config file write failed")

	// Garble the config file name
	name += strconv.Itoa(s.r.Int())
	bls, err := NewCqlBlobStore(name)
	s.Require().Error(err)
	s.Require().Equal(bls, nil, "bls should be nil but is not")
}

func (s *setupTests) TestInvalidConfigFilePerms() {
	var config Config

	config.Cluster.Nodes = []string{"node1", "node2"}

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	s.Require().NoError(err, "Tempfile creation failed")
	name := file.Name()
	file.Close()
	defer os.Remove(name)

	err = writeCqlConfig(name, &config)
	s.Require().NoError(err, "CQL config file write failed")

	// Modify config file Perms
	err = os.Chmod(name, 0000)
	s.Require().NoError(err, "Error in changing file perms")
	defer os.Chmod(name, 0666) //-rw-rw-rw-

	bls, err := NewCqlBlobStore(name)
	s.Require().Error(err)
	s.Require().Equal(bls, nil, "bls should be nil but is not")
}

func (s *setupTests) TestInvalidConfigFormat() {
	var config Config

	config.Cluster.Nodes = []string{"node1", "node2"}

	file, err := ioutil.TempFile(os.TempDir(), "ether")
	s.Require().NoError(err, "Tempfile creation failed")
	name := file.Name()
	file.Close()
	defer os.Remove(name)

	err = writeCqlConfig(name, &config)
	s.Require().NoError(err, "CQL config file write failed")

	// Write some small garbage to the file
	file, err = os.OpenFile(name, os.O_RDWR, 0777)
	s.Require().NoError(err, "CQL config file open failed")

	var length int
	garbage := []byte("boo")
	length, err = file.Write(garbage)
	s.Require().NoError(err, "CQL config file write failed")
	s.Require().Equal(length, len(garbage), "CQL config file write incorrect")

	bls, err := NewCqlBlobStore(name)
	s.Require().Error(err)
	s.Require().Equal(bls, nil, "bls should be nil but is not")
}

// When host is present and up in the single host policy,
// same host be returned for each query retry attempt
func (s *setupTests) TestSingleHostPolicyOK() {
	policy := newSingleHostPolicy()
	host := &gocql.HostInfo{}
	policy.AddHost(host)

	maxRetries := 5
	rt := &gocql.SimpleRetryPolicy{NumRetries: maxRetries}
	query := &gocql.Query{}
	query.RetryPolicy(rt)

	nextHostFunc := policy.Pick(query)
	// Following loop mimics the sequence in GoCQL core driver
	//
	// The driver will invoke nextHostFunc() for each
	// failure in query. The single host policy will continue
	// to provide the same host for each of these calls
	for retry := 1; retry < maxRetries; retry++ {
		nextHost := nextHostFunc()
		s.Require().NotNil(nextHost)
		s.Require().Equal(host, nextHost.Info())
	}
}

func (s *setupTests) TestSingleHostPolicyHostDown() {
	query := &gocql.Query{}
	host := &gocql.HostInfo{}

	policy := newSingleHostPolicy()
	// policy is not managing any hosts at this point

	nextHostFunc := policy.Pick(query)
	nextHost := nextHostFunc()
	s.Require().Nil(nextHost, "nextHost is non-nil when policy has no hosts")

	policy.AddHost(host)
	nextHostFunc = policy.Pick(query)
	nextHost = nextHostFunc()
	s.Require().NotNil(nextHost, "nextHost is nil even when host was added")

	policy.RemoveHost(host)
	nextHostFunc = policy.Pick(query)
	nextHost = nextHostFunc()
	s.Require().Nil(nextHost, "nextHost is non-nil after removing the host")

	policy.HostUp(host)
	nextHostFunc = policy.Pick(query)
	nextHost = nextHostFunc()
	s.Require().NotNil(nextHost, "nextHost is nil after host Up")

	policy.HostDown(host)
	nextHostFunc = policy.Pick(query)
	nextHost = nextHostFunc()
	s.Require().Nil(nextHost, "nextHost is non-nil host Down")
}

func TestSetup(t *testing.T) {
	suite.Run(t, &setupTests{})
}

func (s *setupTests) TearDownTest() {
	resetCqlStore()
}

func (s *setupTests) TearDownSuite() {
	//nop
}
