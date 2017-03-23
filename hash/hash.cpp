// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

#include "citycrc.h"
#include <stdint.h>

extern "C" {

	void cCityHashCrc256(const char *s, size_t len, uint64_t *result) {
		CityHashCrc256(s, len, result);
	}

	void cCityHash128(const char *s, size_t len, uint64_t *lower,
		uint64_t *upper) {

		uint128 rtn = CityHash128(s, len);
		*lower = Uint128Low64(rtn);
		*upper = Uint128High64(rtn);
	}
}


