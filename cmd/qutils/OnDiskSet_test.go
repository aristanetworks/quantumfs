// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"io/ioutil"
	"os"
	"sync"
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

func (s *odsTests) TestOdsInsertElem() {
	element := "Insert"
	present, err := s.ods.InsertElem(element)
	s.Require().NoError(err, "Failure in InsertElem")
	s.Require().Equal(false, present, "Element present")
}

func (s *odsTests) TestOdsInsertElemAgain() {
	element := "InsertAgain"
	present, err := s.ods.InsertElem(element)
	s.Require().Equal(false, present, "Element present")
	s.Require().NoError(err, "Failure in InsertElem")

	present, err = s.ods.InsertElem(element)
	s.Require().NoError(err, "Failure in InsertElem Again")
	s.Require().Equal(true, present, "Element absent")
}

func (s *odsTests) TestOdsInsertElemAgainParallel() {
	element := "InsertAgainParallel"
	var wg sync.WaitGroup
	countBefore, err := s.ods.CountElem(element)
	s.Require().NoError(err, "Failure in TotalUniqueElems")

	insertFunc := func() {
		defer wg.Done()
		_, err := s.ods.InsertElem(element)
		s.Require().NoError(err, "Failure in InsertElem")
	}

	numRoutines := 50
	for i := 0; i < numRoutines; i++ {
		wg.Add(1)
		go insertFunc()
	}

	wg.Wait()
	countAfter, err := s.ods.CountElem(element)
	s.Require().NoError(err, "Failure in TotalUniqueElems")
	s.Require().Equal(countBefore+uint64(numRoutines), countAfter,
		"Incorrect count value present for InsertElem")
}

func (s *odsTests) TestOdsCountElem() {
	element := "Present"
	present, err := s.ods.InsertElem(element)
	s.Require().NoError(err, "Failure in InsertElem")
	s.Require().Equal(false, present, "Element present")

	count, err := s.ods.CountElem(element)
	s.Require().NoError(err, "Failure in CountElem")
	s.Require().Equal(uint64(1), count, "Count Mismatch")
}

func (s *odsTests) TestOdsCountElemFalse() {
	element := "PresentFalse"
	count, err := s.ods.CountElem(element)
	s.Require().NoError(err, "Failure in CountElem")
	s.Require().Equal(uint64(0), count, "Count Mismatch")
}

func (s *odsTests) TestOdsCount() {
	element := "Count"

	keyCount, err := s.ods.TotalUniqueElems()
	s.Require().NoError(err, "Error in TotalUniqueElems")
	s.Require().Equal(0, keyCount, "Incorrect TotalUniqueElems")

	present, err := s.ods.InsertElem(element)
	s.Require().NoError(err, "Failure in InsertElem")
	s.Require().Equal(false, present, "Element present")

	keyCount, err = s.ods.TotalUniqueElems()
	s.Require().NoError(err, "Error in TotalUniqueElems")
	s.Require().Equal(1, keyCount, "Incorrect TotalUniqueElems")
}

func (s *odsTests) TestOdsDeleteElem() {
	element := "Delete"

	orgTotalCount, err := s.ods.TotalUniqueElems()
	s.Require().NoError(err, "Error in TotalUniqueElems")

	present, err := s.ods.InsertElem(element)
	s.Require().NoError(err, "Failure in InsertElem")
	s.Require().Equal(false, present, "Element present")

	count, err := s.ods.CountElem(element)
	s.Require().NoError(err, "Failure in CountElem")
	s.Require().Equal(uint64(1), count, "Count Mismatch")

	err = s.ods.DeleteElem(element)
	s.Require().NoError(err, "Failure in DeleteElem")

	count, err = s.ods.CountElem(element)
	s.Require().NoError(err, "Failure in CountElem")
	s.Require().Equal(uint64(0), count, "Count Mismatch")

	totalCount, err := s.ods.TotalUniqueElems()
	s.Require().NoError(err, "Error in TotalUniqueElems")
	s.Require().Equal(orgTotalCount, totalCount, "Incorrect TotalUniqueElems")

	// Delete Again
	err = s.ods.DeleteElem(element)
	s.Require().NoError(err, "Failure in DeleteElem")

	count, err = s.ods.CountElem(element)
	s.Require().NoError(err, "Failure in CountElem")
	s.Require().Equal(uint64(0), count, "Count Mismatch")

	totalCount, err = s.ods.TotalUniqueElems()
	s.Require().NoError(err, "Error in TotalUniqueElems")
	s.Require().Equal(orgTotalCount, totalCount, "Incorrect TotalUniqueElems")

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
