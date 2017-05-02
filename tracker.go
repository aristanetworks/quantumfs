// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

// track keys, count, size and optiionally path information
// associated with keys

import "encoding/hex"
import "fmt"

import "github.com/aristanetworks/quantumfs"

// a set of paths
type pathSet map[string]bool

// sharing information for a key
type sharedInfo struct {
	// size shared with other paths
	size uint64
	// count of keys shared with other paths
	count uint64
	// content is shared with these set of paths
	otherPaths pathSet
}

type tracker struct {
	// track path information only when explicitly requested
	trackPathInfo bool
	keys          map[string]*sharedInfo
}

func newTracker(trackPathInfo bool) *tracker {
	return &tracker{
		trackPathInfo: trackPathInfo,
		keys:          make(map[string]*sharedInfo),
	}
}

func (t *tracker) addKey(key string, path string, size uint64) {
	if _, found := t.keys[key]; !found {
		t.keys[key] = &sharedInfo{
			count: 1,
			size:  size,
		}
		if t.trackPathInfo {
			t.keys[key].otherPaths = pathSet{path: true}
		}
		return
	}
	s := t.keys[key]
	s.count += 1
	s.size += size
	if t.trackPathInfo {
		s.otherPaths[path] = true
	}
}

// printDedupeReport outputs in following format
// X1 X2 shared-size X3 shared-count X4 X5
//
// X1: key encoded as string
// X2: key as ObjectKey.String()
// X3: total shared size in bytes
// X4: total shared count
// X5: list of paths which share this key
func (t *tracker) printDedupeReport() {
	if !t.trackPathInfo {
		return
	}
	fmt.Println("Dedupe report:")
	fmt.Println("----------------------")
	for k, info := range t.keys {
		if info.count > 1 {
			paths := make([]string, 0)
			for p := range info.otherPaths {
				paths = append(paths, p)
			}
			data, err := hex.DecodeString(k)
			if err != nil {
				fmt.Println("%s KeyDecodeError: %v\n",
					k, err)
				continue
			}
			qKey := quantumfs.NewObjectKeyFromBytes(data)
			fmt.Printf("%s %s shared-size %d shared-count %d %v\n",
				k, qKey.String(), info.size, info.count, paths)
		}
	}
}

func (t *tracker) printKeyPathInfo(keys []string) {
	if !t.trackPathInfo {
		return
	}
	for _, k := range keys {
		data, err := hex.DecodeString(k)
		if err != nil {
			fmt.Printf("%s KeyDecodeError: %v\n",
				k, err)
			continue
		}
		qKey := quantumfs.NewObjectKeyFromBytes(data)
		paths := make([]string, 0)
		for p := range t.keys[k].otherPaths {
			paths = append(paths, p)
		}
		fmt.Printf("%s %s %v\n", k, qKey.Text(), paths)
	}
}

// returns list of keys and size unique to t
func (t *tracker) trackerKeyDiff(t1 *tracker) ([]string, uint64) {
	uniq := make([]string, 0)
	var usize uint64
	for k, _ := range t.keys {
		if _, exists := t1.keys[k]; !exists {
			uniq = append(uniq, k)
			usize += t.keys[k].size
		}
	}
	return uniq, usize
}

func (t *tracker) uniqueKeys() int {
	var uniq int
	for _, info := range t.keys {
		if info.count == 1 {
			uniq++
		}
	}
	return uniq
}

func (t *tracker) uniqueSize() uint64 {
	var size uint64
	for _, info := range t.keys {
		if info.count == 1 {
			size += info.size
		}
	}
	return size
}
func (t *tracker) totalKeys() int {
	return len(t.keys)
}

func (t *tracker) totalSize() uint64 {
	var size uint64
	for _, info := range t.keys {
		size += info.size
	}
	return size
}
