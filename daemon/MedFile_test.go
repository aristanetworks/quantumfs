// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test the small/medium file transitions and medium file operations

import "bytes"
import "io/ioutil"
import "os"
import "testing"
import "strconv"
import "syscall"

func genFibonacci(maxLen int) []byte {
	rtn := make([]byte, maxLen)
	copy(rtn, "11")
	end := 2
	lastFib := 1
	lastLastFib := 1

	for len(rtn) < maxLen {
		fibNum := lastFib + lastLastFib
		lastLastFib = lastFib
		lastFib = fibNum

		copy(rtn[end:], strconv.Itoa(fibNum))
	}

	return rtn[:maxLen]
}

func TestFileExpansion_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		api := test.getApi()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		// Write the fibonacci sequence to the file continually past what
		// a single block could hold.
		data := genFibonacci(4 * 1024 * 1024)
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing 4MB fibonacci to new fd: %v",
			err)

		// Read it back
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 4MB fibonacci back from file")
		test.assert(len(data) == len(output),
			"Data length mismatch, %d vs %d", len(data), len(output))
		if !bytes.Equal(data, output) {
			for i := 0; i < len(data); i++ {
				test.assert(data[i] == output[i],
					"Data readback mismatch at idx %d, %s vs %s",
					i, data[i], output[i])
			}
		}

		// Test that we can truncate this
		newLen := 2500000
		os.Truncate(testFilename, int64(newLen))

		// Ensure that the data remaining is what we expect
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 2.5MB fibonacci from file")
		test.assert(len(output) == newLen, "Truncated length incorrect")
		test.assert(bytes.Equal(data[:newLen], output),
			"Post-truncation mismatch")

		// Let's re-expand it using SetAttr
		const truncLen = 5 * 1024 * 1024
		os.Truncate(testFilename, truncLen)

		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading fib+hole back from file")
		test.assert(bytes.Equal(data[:newLen], output[:newLen]),
			"Data readback mismatch")
		delta := truncLen - newLen
		test.assert(bytes.Equal(make([]byte, delta), output[newLen:]),
			"File hole not filled with zeros")

		// Branch the workspace
		dst := "largeattrsparse/test"
		err = api.Branch(test.relPath(workspace), dst)
		test.assert(err == nil, "Unable to branch")

		test.checkSparse(test.absPath(dst+"/test"), testFilename, 25000,
			10)
	})
}

func TestMedFileAttr_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		api := test.getApi()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		// Create a small file
		fd, _ := syscall.Creat(testFilename, 0124)
		syscall.Close(fd)

		// Then expand it via SetAttr to medium file size
		newSize := int64(2 * 1024 * 1024)
		os.Truncate(testFilename, newSize)

		// Check that the size increase worked
		var stat syscall.Stat_t
		err := syscall.Stat(testFilename, &stat)
		test.assert(err == nil, "Unable to stat file: %v", err)
		test.assert(stat.Size == newSize, "File size incorrect, %d",
			stat.Size)

		// Read what should be 2MB of zeros
		var output []byte
		output, err = ioutil.ReadFile(testFilename)
		test.assert(err == nil, "Error reading 4MB hole from file")

		for i := 0; i < len(output); i++ {
			test.assert(output[i] == 0, "Data not zeroed in file, %s",
				output[i])
		}

		// Ensure that we can write data into the hole
		testString := []byte("testData")
		var file *os.File
		var count int
		dataOffset := 1500000
		file, err = os.OpenFile(testFilename, os.O_RDWR, 0777)
		test.assert(err == nil, "Unable to open file for rdwr: %v", err)
		count, err = file.WriteAt(testString, int64(dataOffset))
		test.assert(err == nil, "Unable to write at offset: %v", err)
		test.assert(count == len(testString),
			"Unable to write data all at once")
		err = file.Close()
		test.assert(err == nil, "Unable to close file handle")

		// Branch the workspace
		dst := "medattrsparse/test"
		err = api.Branch(test.relPath(workspace), dst)
		test.assert(err == nil, "Unable to branch")

		test.checkSparse(test.absPath(dst+"/test"), testFilename, 25000,
			10)
	})
}

func TestMedFileZero_test(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.startDefaultQuantumFs()

		workspace := test.nullWorkspace()
		testFilename := workspace + "/test"

		data := genFibonacci(10 * 1024)
		err := printToFile(testFilename, string(data))
		test.assert(err == nil, "Error writing tiny fib to new fd")
		// expand this to be the desired file type
		os.Truncate(testFilename, 2*1048576)

		os.Truncate(testFilename, 0)
		test.assert(test.fileSize(testFilename) == 0, "Unable to zero file")

		output, err := ioutil.ReadFile(testFilename)
		test.assert(len(output) == 0, "Empty file not really empty")
		test.assert(err == nil, "Unable to read from empty file")
	})
}
