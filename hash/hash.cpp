// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

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


