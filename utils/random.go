// Copyright (c) 2017 Arista Networks, Inc.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

package utils

import (
	"math/rand"
	"time"
)

// We are not using golang's default Random Number Generator (RNG) as
// we do not need a perfect, reproducible sequence of psudo-random
// numbers guaraneed by a locked source. An unlocked source would
// suffice.
// Moreover, issues like BUG/205036 have been observed about
// inconsistent state of the random module's internal lock.

var RandomSeed int64
var RandomNumberGenerator *rand.Rand

func init() {
	RandomSeed = time.Now().UnixNano()
	RandomNumberGenerator = rand.New(rand.NewSource(RandomSeed))
}
