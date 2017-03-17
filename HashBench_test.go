// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import "crypto/md5"
import "crypto/sha1"
import "testing"

func genData(length int) []byte {
	rtn := make([]byte, length)
	for i := 0; i < len(rtn); i++ {
		rtn[i] = byte(i)
	}
	return rtn
}

const shortLength = 12
const mediumLength = 1024
const longLength = 1048576

func BenchmarkSha1Short(test *testing.B) {
	data := genData(shortLength)

	for i := 0; i < test.N; i++ {
		sha1.Sum(data)
	}
}

func BenchmarkSha1Medium(test *testing.B) {
	data := genData(mediumLength)

	for i := 0; i < test.N; i++ {
		sha1.Sum(data)
	}
}

func BenchmarkSha1Long(test *testing.B) {
	data := genData(longLength)

	for i := 0; i < test.N; i++ {
		sha1.Sum(data)
	}
}

func BenchmarkCity128Short(test *testing.B) {
	data := genData(shortLength)

	for i := 0; i < test.N; i++ {
		CityHash128(data)
	}
}

func BenchmarkCity128Medium(test *testing.B) {
	data := genData(mediumLength)

	for i := 0; i < test.N; i++ {
		CityHash128(data)
	}
}

func BenchmarkCity128Long(test *testing.B) {
	data := genData(longLength)

	for i := 0; i < test.N; i++ {
		CityHash128(data)
	}
}

func BenchmarkCity256CrcShort(test *testing.B) {
	data := genData(shortLength)

	for i := 0; i < test.N; i++ {
		CityHash256(data)
	}
}

func BenchmarkCity256CrcMedium(test *testing.B) {
	data := genData(mediumLength)

	for i := 0; i < test.N; i++ {
		CityHash256(data)
	}
}

func BenchmarkCity256CrcLong(test *testing.B) {
	data := genData(longLength)

	for i := 0; i < test.N; i++ {
		CityHash256(data)
	}
}

func BenchmarkMd5Short(test *testing.B) {
	data := genData(shortLength)

	for i := 0; i < test.N; i++ {
		md5.Sum(data)
	}
}

func BenchmarkMd5Medium(test *testing.B) {
	data := genData(mediumLength)

	for i := 0; i < test.N; i++ {
		md5.Sum(data)
	}
}

func BenchmarkMd5Long(test *testing.B) {
	data := genData(longLength)

	for i := 0; i < test.N; i++ {
		md5.Sum(data)
	}
}
