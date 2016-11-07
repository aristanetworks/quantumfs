// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package qlog

import "fmt"
import "math"
import "strconv"

var wildcardStr string


func init() {
	// The wildcard character / string needs to be something that would never
	// show in a log so that we can use strings as map keys
	wildcardStr = string([]byte{7})

}

func collectData(wildcards []bool, seq []LogOutput,
	sequences []SequenceData) SequenceData {

	var rtn SequenceData
	rtn.Seq = make([]LogOutput, len(seq), len(seq))
	copy(rtn.Seq, seq)

	for i := 0; i < len(sequences); i++ {
		if PatternMatches(seq, wildcards, sequences[i].Seq) {
			rtn.Times = append(rtn.Times, sequences[i].Times...)
		}
	}

	return rtn
}

// countConsecutive is false if we should count consecutive wildcards as one
func CountWildcards(mask []bool, countConsecutive bool) int {
	count := 0
	inWildcard := false
	for i := 0; i < len(mask); i++ {
		if mask[i] {
			if countConsecutive || !inWildcard {
				count++
			}
			inWildcard = true
		} else {
			inWildcard = false
		}
	}
	return count
}

// Returns true if a is a superset of b
func Superset(a []TimeData, b []TimeData) bool {
	union := make(map[int]bool)

	if len(b) > len(a) {
		return false
	}

	for i := 0; i < len(a); i++ {
		union[a[i].LogIdxLoc] = true
	}

	for i := 0; i < len(b); i++ {
		union[b[i].LogIdxLoc] = true
	}

	return len(union) == len(a)
}

type wildcardedSequence struct {
	sequence  []LogOutput
	wildcards []bool
}

func newWildcardedSeq(seq []LogOutput, wc []bool) wildcardedSequence {

	var rtn wildcardedSequence
	rtn.sequence = make([]LogOutput, len(seq), len(seq))
	rtn.wildcards = make([]bool, len(wc), len(wc))
	copy(rtn.sequence, seq)
	copy(rtn.wildcards, wc)

	return rtn
}

type PatternMap struct {
	dataByList map[string]*wildcardedSequence
	strToList  map[string]string
}

func (l *PatternMap) StrExists(key string) bool {
	_, exists := l.strToList[key]
	return exists
}

func (l *PatternMap) SetStr(key string, listKey string) {
	l.strToList[key] = listKey
}

func (l *PatternMap) Listed(listAsStr string) *wildcardedSequence {
	entry, exists := l.dataByList[listAsStr]
	if exists {
		return entry
	}

	return nil
}

func (l *PatternMap) Set(newKey string, newListKey string,
	newData *wildcardedSequence) {

	l.dataByList[newListKey] = newData
	l.strToList[newKey] = newListKey
}

func (l *PatternMap) recurseGenPatterns(seq []LogOutput,
	sequences []SequenceData, maxLenWildcards int) {

	if len(seq) > maxLenWildcards {
		return
	}

	// Start with all wildcards
	wildcardMask := make([]bool, len(seq), len(seq))
	for j := 1; j < len(wildcardMask)-1; j++ {
		wildcardMask[j] = true
	}

	l.recurseGenPatterns_(wildcardMask, 1, seq, sequences)
}

// curMask is a map of indices into sequences which should be ignored when gathering
// data (wildcards). We check how many sequences match with the current wildcard mask
// and if only one sequence matches then we know that no others will (since as we
// recurse deeper, we remove wildcards and only become more specific) and escape
func (l *PatternMap) recurseGenPatterns_(curMask []bool, wildcardStartIdx int,
	seq []LogOutput, sequences []SequenceData) {

	// If we've already got a result for this sequence, then this has
	// been generated and we can skip it. This would not be safe if we didn't
	// include multiple consecutive wildcards because small sequences would
	// occlude longer versions.
	expandedStr := genSeqStrExt(seq, curMask, true)
	if l.StrExists(expandedStr) {
		return
	}

	// Count how many unique sequences match this sequence (with wildcards)
	matches := 0
	matchStr := ""
	for i := 0; i < len(sequences); i++ {
		if PatternMatches(seq, curMask, sequences[i].Seq) {
			matches++
			matchStr += strconv.Itoa(i) + "|"
		} else if len(seq) == len(sequences[i].Seq) {
			for j := 0; j < len(seq); j++ {
				if seq[j].Format != sequences[i].Seq[j].Format {
					break
				}
			}
		}
	}
	if matches == 0 {
		panic(fmt.Sprintf("Sequence doesn't even match against itself"))
	}

	// Make sure that we mark that we've visited this expanded sequence
	l.SetStr(expandedStr, matchStr)

	// If there's already an identical mapping on PatternMap, there's no point
	// in returning both. Only choose the one with more effective wildcards
	oldEntry := l.Listed(matchStr)

	if matches <= 1 && oldEntry != nil {
		// There are no interesting sequence / wildcard combos deeper and
		// we already have an entry for this data
		return
	}

	setEntry := false
	if oldEntry == nil || (CountWildcards(curMask, false) <
		CountWildcards(oldEntry.wildcards, false)) {

		setEntry = true
	}

	if setEntry {
		newEntry := newWildcardedSeq(seq, curMask)
		l.Set(expandedStr, matchStr, &newEntry)
	}

	// If there are exactly two matches, then we don't need to recurse because
	// we've already accounted for the most wildcarded version of this sequence
	// and getting more specific with the same number of wildcards isn't useful
	if matches > 2 {
		// we need to remove a new wildcard. Note: we don't allow the first /
		// last logs to be wildcarded since we *know* they're functions and
		// they define the function we're trying to analyze across variants
		for i := wildcardStartIdx; i < len(seq)-1; i++ {
			if curMask[i] {
				// This spot hasn't lost its wildcard yet.
				curMask[i] = false
				l.recurseGenPatterns_(curMask, i+1, seq, sequences)
				// Make sure to fix the entry for the next loop
				curMask[i] = true
			}
		}
	}
}

func GetStatPatterns(logs []LogOutput, maxLenWildcards int) []PatternData {
	sequenceMap := ExtractSequences(logs)
	sequences := make([]SequenceData, len(sequenceMap), len(sequenceMap))
	idx := 0
	for _, v := range sequenceMap {
		sequences[idx] = v
		idx++
	}

	status := NewLogStatus(50)

	// Now generate all combinations of the sequences with wildcards in them, but
	// start with an almost completely wildcarded sequence and recurse towards
	// a less-so one. We only care about subsequences that match more than one
	// other sequence, because they are uninteresting if they only belong to one.
	// By starting at the most wildcarded sequence, we can disregard entire
	// branches as we recurse to save time!
	fmt.Printf("Generating all log patterns from %d unique sequences...\n",
		len(sequences))
	patterns := PatternMap{
		dataByList: make(map[string]*wildcardedSequence),
		strToList:  make(map[string]string),
	}

	for i := 0; i < len(sequences); i++ {
		// Update the status bar
		status.Process(float32(i) / float32(len(sequences)))

		seq := sequences[i].Seq

		patterns.recurseGenPatterns(seq, sequences, maxLenWildcards)

		matchStr := strconv.Itoa(i)
		// recurseGenPatterns will overlook the "no wildcards" sequence,
		// so we must add that ourselves
		newEntry := newWildcardedSeq(seq, []bool{})
		patterns.Set(genSeqStr(seq), matchStr, &newEntry)

		// make sure that we at least call the "wrapper" pattern
		wildcardFilled := make([]bool, len(seq), len(seq))
		for j := 1; j < len(seq)-1; j++ {
			wildcardFilled[j] = true
		}
		patterns.recurseGenPatterns_(wildcardFilled, len(seq)-1, seq,
			sequences)
	}
	status.Process(1)

	// we MUST preallocate here. I had been using append with just a size hint,
	// and for some reason it resulted in a number of zero entries here. Go bug?
	rawResults := make([]PatternData, len(patterns.dataByList),
		len(patterns.dataByList))

	// Now collect all the data for the sequences, allowing wildcards
	fmt.Printf("Collecting data for each of %d wildcard-ed subsequences...\n",
		len(patterns.dataByList))
	status = NewLogStatus(50)
	mapIdx := 0
	for _, wcseq := range patterns.dataByList {
		status.Process(float32(mapIdx) / float32(len(patterns.dataByList)))

		var newResult PatternData
		newResult.SeqStrRaw = genSeqStr(wcseq.sequence)
		newResult.Wildcards = wcseq.wildcards
		newResult.Data = collectData(wcseq.wildcards, wcseq.sequence,
			sequences)

		// Precompute more useful stats
		var avgSum int64
		for i := 0; i < len(newResult.Data.Times); i++ {
			avgSum += newResult.Data.Times[i].Delta
		}
		newResult.Sum = avgSum
		newResult.Avg = avgSum / int64(len(newResult.Data.Times))

		// Now we can compute the standard deviation of the data. This is
		// used to determine whether a pattern's average time is probably
		// caused by exactly that pattern's sequence, and not a subsequence
		var deviationSum float64
		for i := 0; i < len(newResult.Data.Times); i++ {
			deviation := newResult.Data.Times[i].Delta - newResult.Avg
			deviationSum += float64(deviation * deviation)
		}
		newResult.Stddev = int64(math.Sqrt(deviationSum))
		newResult.Id = mapIdx

		rawResults[mapIdx] = newResult
		mapIdx++
	}
	status.Process(1)

	return rawResults
}

