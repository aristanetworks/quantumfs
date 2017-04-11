// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import "os"

var recordMutex DeferableMutex

// Leave a record of the temperary directory's name
func WriteRecords(fileName, dirPath string) error {
	var errorInfo error = nil

	defer recordMutex.Lock().Unlock()
	for i := 0; i < 100; i++ {
		errorInfo = func() error {
			filePath := os.Getenv("GOPATH") + "/src/github.com" +
				"/aristanetworks/quantumfs/History/" + fileName
			file, err := os.OpenFile(filePath,
				os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				return err
			}

			defer file.Close()
			_, err = file.Seek(0, os.SEEK_END)
			if err != nil {
				return err
			}

			dirName := []byte(dirPath + "\n")
			size, err := file.Write(dirName)
			if err != nil && size != len(dirName) {
				return err
			}
			return nil
		}()

		if errorInfo == nil {
			break
		}
	}

	return errorInfo
}
