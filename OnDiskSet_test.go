// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

type odsTests struct {
	suite.Suite
	dbFileName string
	ods        *OnDiskSet
}

func checkSetup(s *odsTests) {
	if s.ods == nil {
		s.T().Skip("OnDiskSet was not setup")
	}
}

func (s *odsTests) SetupSuite() {
	dbFile, err := ioutil.TempFile("/tmp", "onDiskSetUnitTest")
	s.Require().NoError(err, "Failure to create temp file")
	defer dbFile.Close()

	s.dbFileName = dbFile.Name()

	s.ods, err = NewOnDiskSet(s.dbFileName)
	s.Require().NoError(err, "NewOnDiskSet returned an error")
	s.Require().NotNil(s.ods, "NewOnDiskSet returned nil")
}

func (s *odsTests) SetupTest() {
	checkSetup(s)
}

func (s *odsTests) TestOdsInsert() {
	element := "Insert"
	present, err := s.ods.Insert(element)
	s.Require().NoError(err, "Failure in Insert")
	s.Require().Equal(present, false, "Element present")
}

func (s *odsTests) TestOdsInsertAgain() {
	element := "InsertAgain"
	present, err := s.ods.Insert(element)
	s.Require().Equal(present, false, "Element present")
	s.Require().NoError(err, "Failure in Insert")
	present, err = s.ods.Insert(element)
	s.Require().NoError(err, "Failure in Insert Again")
	s.Require().Equal(present, true, "Element not present")
}

func (s *odsTests) TestOdsIsPresent() {
	element := "Present"
	present, err := s.ods.Insert(element)
	s.Require().NoError(err, "Failure in Insert")
	s.Require().Equal(present, false, "Element present")

	present = s.ods.IsPresent(element)
	s.Require().Equal(present, true, "Element not present")
}

func (s *odsTests) TestOdsIsPresentFalse() {
	element := "PresentFalse"
	present := s.ods.IsPresent(element)
	s.Require().Equal(present, false, "Element found in the Set")
}

func (s *odsTests) TestOdsCount() {
	element := "Count"

	count, err := s.ods.Count()
	s.Require().NoError(err, "Error in Count")
	s.Require().Equal(count, 0, "Incorrect Count")

	present, err := s.ods.Insert(element)
	s.Require().NoError(err, "Failure in Insert")
	s.Require().Equal(present, false, "Element present")

	count, err = s.ods.Count()
	s.Require().NoError(err, "Error in Count")
	s.Require().Equal(count, 1, "Incorrect Count")
}

func (s *odsTests) TestOdsDelete() {
	element := "Delete"

	orgCount, err := s.ods.Count()
	s.Require().NoError(err, "Error in Count")
	s.Require().Equal(1, orgCount, "Incorrect Count")

	present, err := s.ods.Insert(element)
	s.Require().NoError(err, "Failure in Insert")
	s.Require().Equal(present, false, "Element present")

	present = s.ods.IsPresent(element)
	s.Require().Equal(present, true, "Element not present")

	err = s.ods.Delete(element)
	s.Require().NoError(err, "Failure in Delete")
	present = s.ods.IsPresent(element)
	s.Require().Equal(present, false, "Element present")

	count, err := s.ods.Count()
	s.Require().NoError(err, "Error in Count")
	s.Require().Equal(orgCount, count, "Incorrect Count")

	err = s.ods.Delete(element)
	s.Require().NoError(err, "Failure in Delete")
	present = s.ods.IsPresent(element)
	s.Require().Equal(present, false, "Element present")
}

func TestOds(t *testing.T) {
	suite.Run(t, &odsTests{})
}

// The TearDownTest method will be run after every test in the suite.
func (s *odsTests) TearDownTest() {
}

// The TearDownSuite method will be run after Suite is done
func (s *odsTests) TearDownSuite() {
	defer os.Remove(s.dbFileName)
	s.ods.Close()
}
