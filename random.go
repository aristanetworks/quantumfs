// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package quantumfs

import (
	"math/rand"
	"time"
)

var RandomSeed int64
var RandomNumberGenerator *rand.Rand

func init() {
	RandomSeed = time.Now().UnixNano()
	RandomNumberGenerator = rand.New(rand.NewSource(RandomSeed))
}
