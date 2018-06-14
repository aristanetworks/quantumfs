// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/aristanetworks/quantumfs/qlog"
)

const defaultChunkSize = 4

func collectData(wildcards []bool, seq []qlog.LogOutput,
	sequences []sequenceData) sequenceData {

	var rtn sequenceData
	rtn.Seq = make([]qlog.LogOutput, len(seq), len(seq))
	copy(rtn.Seq, seq)

	for i := 0; i < len(sequences); i++ {
		if patternMatches(seq, wildcards, sequences[i].Seq) {
			rtn.Times = append(rtn.Times, sequences[i].Times...)
		}
	}

	return rtn
}

// countConsecutive is false if we should count consecutive wildcards as one
func countWildcards(mask []bool, countConsecutive bool) int {
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
func superset(a []timeData, b []timeData) bool {
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
	sequence  []qlog.LogOutput
	wildcards []bool
}

func newWildcardedSeq(seq []qlog.LogOutput, wc []bool) wildcardedSequence {
	var rtn wildcardedSequence
	rtn.sequence = make([]qlog.LogOutput, len(seq), len(seq))
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

func (l *PatternMap) recurseGenPatterns(seq []qlog.LogOutput,
	sequences []sequenceData, maxLenWildcards int) {

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
	seq []qlog.LogOutput, sequences []sequenceData) {

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
		if patternMatches(seq, curMask, sequences[i].Seq) {
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
	if oldEntry == nil || (countWildcards(curMask, false) <
		countWildcards(oldEntry.wildcards, false)) {

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

func SeqMapToList(sequenceMap map[string]sequenceData) []sequenceData {
	sequences := make([]sequenceData, len(sequenceMap), len(sequenceMap))
	idx := 0
	for _, v := range sequenceMap {
		sequences[idx] = v
		idx++
	}

	return sequences
}

// Because Golang is a horrible language and doesn't support maps with slice keys,
// we need to construct long string keys and save the slices in the value for later
func extractTrackerMap(logs []qlog.LogOutput, maxThreads int) (count int,
	rtnMap map[uint64][]sequenceTracker) {

	trackerMap := make(map[uint64][]sequenceTracker)
	trackerCount := 0
	var outputMutex sync.Mutex

	// we need one channel per thread. Each thread accepts a disjoint subset of
	// all possible request IDs, so we can use channels to preserve log order
	var wg sync.WaitGroup
	wg.Add(maxThreads)

	var jobChannels []chan int
	for i := 0; i < maxThreads; i++ {
		jobChannels = append(jobChannels, make(chan int, 2))
	}

	for i := 0; i < maxThreads; i++ {
		go processTrackers(jobChannels[i], &wg, logs, trackerMap,
			&trackerCount, &outputMutex)
	}

	fmt.Printf("Extracting sub-sequences from %d logs...\n", len(logs))
	status := qlog.NewLogStatus(50)

	// Go through all the logs in one pass, constructing all subsequences
	for i := 0; i < len(logs); i++ {
		status.Process(float32(i) / float32(len(logs)))

		reqId := logs[i].ReqId

		// Skip it if its a special id since they're not self contained
		if reqId >= qlog.MinSpecialReqId {
			continue
		}

		channelIdx := reqId % uint64(maxThreads)
		jobChannels[channelIdx] <- i
	}
	for i := 0; i < maxThreads; i++ {
		close(jobChannels[i])
	}
	wg.Wait()
	status.Process(1)

	return trackerCount, trackerMap
}

func getTrackerMap(inFile string, maxThreads int) (int,
	map[uint64][]sequenceTracker) {

	var logs []qlog.LogOutput
	{
		pastEndIdx, dataArray, strMap := qlog.ExtractFields(inFile)
		if len(dataArray) == 0 {
			panic("qlog file doesn't contain headers")
		}
		logs = qlog.OutputLogsExt(pastEndIdx, dataArray, strMap,
			maxThreads, true)

		dataArray = nil
		strMap = nil
	}
	fmt.Println("Garbage Collecting...")
	runtime.GC()

	var trackerMap map[uint64][]sequenceTracker
	var trackerCount int
	{
		trackerCount, trackerMap = extractTrackerMap(logs, maxThreads)
		logs = nil
	}
	fmt.Println("Garbage Collecting...")
	runtime.GC()

	return trackerCount, trackerMap
}

type timeData struct {
	// how long the sequence took
	Delta int64
	// when it started in Unix time
	StartTime int64
	// where it started in the logs
	LogIdxLoc int
}

type sequenceData struct {
	Times []timeData
	Seq   []qlog.LogOutput
}

type patternData struct {
	SeqStrRaw string // No wildcards in this string, just seq
	Data      sequenceData
	Wildcards []bool
	Avg       int64
	Sum       int64
	Stddev    int64
	Id        uint64
}

func extractSeqMap(trackerCount int,
	trackerMap map[uint64][]sequenceTracker) map[string]sequenceData {

	fmt.Printf("Collating subsequence time data into map with %d entries...\n",
		trackerCount)
	status := qlog.NewLogStatus(50)

	// After going through the logs, add all our sequences to the rtn map
	rtn := make(map[string]sequenceData)
	trackerIdx := 0
	for reqId, trackers := range trackerMap {
		// Skip any requests marked as bad
		if len(trackers) == 0 {
			continue
		}

		// Go through each tracker
		for k := 0; k < len(trackers); k++ {
			status.Process(float32(trackerIdx) / float32(trackerCount))
			trackerIdx++

			// If the tracker isn't ready, that means there was a fnIn
			// that missed its fnOut. That's an error
			if trackers[k].ready == false {
				fmt.Printf("Error: Mismatched '%s' in requestId %d"+
					" log\n", qlog.FnEnterStr, reqId)
				break
			}

			rawSeq := trackers[k].seq
			seq := genSeqStr(rawSeq)
			data := rtn[seq]
			// For this sequence, append the time it took
			if data.Seq == nil {
				data.Seq = rawSeq
			}
			data.Times = append(data.Times,
				timeData{
					Delta: rawSeq[len(rawSeq)-1].T -
						rawSeq[0].T,
					StartTime: rawSeq[0].T,
					LogIdxLoc: trackers[k].startLogIdx,
				})
			rtn[seq] = data
		}
	}
	status.Process(1)

	return rtn
}

func getStatPatterns(inFile string, maxThreads int,
	maxLenWildcards int) []patternData {

	// Structures during this process can be massive. Throw them away asap
	trackerCount, trackerMap := getTrackerMap(inFile, maxThreads)

	var sequenceMap map[string]sequenceData
	{
		sequenceMap = extractSeqMap(trackerCount, trackerMap)
		trackerMap = nil
	}
	fmt.Println("Garbage Collecting...")
	runtime.GC()

	var sequences []sequenceData
	{
		sequences = SeqMapToList(sequenceMap)
		sequenceMap = nil
	}
	fmt.Println("Garbage Collecting...")
	runtime.GC()

	patterns := SeqToPatterns(sequences, maxLenWildcards)
	sequences = nil

	return patterns
}

func SeqToPatterns(sequences []sequenceData, maxLenWildcards int) []patternData {
	status := qlog.NewLogStatus(50)

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
	rawResults := make([]patternData, len(patterns.dataByList),
		len(patterns.dataByList))

	// Now collect all the data for the sequences, allowing wildcards
	fmt.Printf("Collecting data for each of %d wildcard-ed subsequences...\n",
		len(patterns.dataByList))
	status = qlog.NewLogStatus(50)
	mapIdx := 0
	for _, wcseq := range patterns.dataByList {
		status.Process(float32(mapIdx) / float32(len(patterns.dataByList)))

		var newResult patternData
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
		// We need an ID that's unique but deterministic
		hash := md5.Sum([]byte(newResult.SeqStrRaw))
		hashTrimmed := hash[:8]
		newResult.Id = binary.BigEndian.Uint64(hashTrimmed[:])

		rawResults[mapIdx] = newResult
		mapIdx++
	}
	status.Process(1)

	return rawResults
}

// What the current patternIdx we're looking for is, and whether we can ignore
// mismatched strings while looking for it
type matchState struct {
	patternIdx  int
	matchAnyStr bool
	endIdx      int
}

func newMatchState(p int, m bool) matchState {
	return matchState{
		patternIdx:  p,
		matchAnyStr: m,
		endIdx:      -1,
	}
}

// Determine whether pattern, using wildcards, exists within data. We do this instead
// of regex because we have a simple enough case and regex is super slow
func patternMatches(pattern []qlog.LogOutput, wildcards []bool,
	data []qlog.LogOutput) bool {

	stateStack := make([]matchState, 0, len(pattern))
	stateStack = append(stateStack, newMatchState(0, false))

	for i := 0; i < len(data); i++ {
		curState := stateStack[len(stateStack)-1]
		rewindPatternIdx := false

		if pattern[curState.patternIdx].Format != data[i].Format {
			if !curState.matchAnyStr {
				rewindPatternIdx = true
			}
			// otherwise there's no issue just continue on
		} else {
			// The pattern matches, so we need to advance the patternIdx
			matchAnyStr := false
			patternIdx := curState.patternIdx
			for {
				pushState := false
				patternIdx++

				if patternIdx >= len(pattern) {
					// we've reached the end of the pattern at
					// the right time, so we have a match
					if i == len(data)-1 {
						return true
					} else {
						//it's too early, so this mismatches
						rewindPatternIdx = true
						pushState = true
					}
				}

				if patternIdx < len(wildcards) &&
					wildcards[patternIdx] {

					matchAnyStr = true
				} else {
					pushState = true
				}

				if pushState {
					// record where we ended in data
					curState.endIdx = i
					stateStack[len(stateStack)-1] = curState

					// we've found a new pattern token that isn't
					// a wildcard. We're done advancing.
					stateStack = append(stateStack,
						newMatchState(patternIdx,
							matchAnyStr))
					break
				}
			}
		}

		if rewindPatternIdx {
			// We saw a mismatch and weren't allowed to, so this can't be
			// the right subsequence and we must go back to the last
			// valid state that allowed wildcards
			for j := len(stateStack) - 1; j >= 0; j-- {
				stateStack = stateStack[:j]

				if len(stateStack) == 0 {
					return false
				}

				if stateStack[j-1].matchAnyStr {
					if stateStack[j-1].endIdx == -1 {
						panic("Unset endIdx used")
					}
					// we have to rewind our search in data also
					i = stateStack[j-1].endIdx
					stateStack[j-1].endIdx = -1
					break
				}
			}
		}
	}

	// We reached the end of the data and never reached the end of the pattern,
	// so there's no match
	return false
}

func processTrackers(jobs <-chan int, wg *sync.WaitGroup, logs []qlog.LogOutput,
	trackerMap map[uint64][]sequenceTracker, trackerCount *int,
	mutex *sync.Mutex) {

	for i := range jobs {
		log := logs[i]
		reqId := log.ReqId

		// Grab the sequenceTracker list for this request.
		// Note: it is safe for us to not lock the entire region only
		// because we're guaranteed that we're the only thread for this
		// request Id, and hence this tracker is only ours
		var trackers []sequenceTracker
		mutex.Lock()
		trackers, exists := trackerMap[reqId]
		mutex.Unlock()

		// If there's an empty entry that already exists, that means
		// this request had an error and was aborted. Leave it alone.
		if len(trackers) == 0 && exists {
			continue
		}

		// Start a new subsequence if we need to
		if isFunctionIn(log.Format) {
			trackers = append(trackers, newSequenceTracker(i))
		}

		abortRequest := false
		// Inform all the trackers of the new token
		for k := 0; k < len(trackers); k++ {
			err := trackers[k].Process(log)
			if err != nil {
				fmt.Println(err)
				abortRequest = true
				break
			}
		}

		if abortRequest {
			// Mark the request as bad
			mutex.Lock()
			trackerMap[reqId] = make([]sequenceTracker, 0)
			mutex.Unlock()
			continue
		}

		// Only update entry if it won't be empty
		if exists || len(trackers) > 0 {
			mutex.Lock()
			if len(trackers) != len(trackerMap[reqId]) {
				*trackerCount++
			}
			trackerMap[reqId] = trackers
			mutex.Unlock()
		}
	}
	wg.Done()
}

type sequenceTracker struct {
	stack logStack

	ready       bool
	seq         []qlog.LogOutput
	startLogIdx int
}

func (s *sequenceTracker) Ready() bool {
	return s.ready
}

func (s *sequenceTracker) Seq() []qlog.LogOutput {
	return s.seq
}

func newSequenceTracker(startIdx int) sequenceTracker {
	return sequenceTracker{
		stack:       newLogStack(),
		ready:       false,
		seq:         make([]qlog.LogOutput, 0),
		startLogIdx: startIdx,
	}
}

func (s *sequenceTracker) Process(log qlog.LogOutput) error {
	// Nothing more to do
	if s.ready {
		return nil
	}

	top, err := s.stack.Peek()
	if len(s.stack) == 1 && qlog.IsLogFnPair(top.Format, log.Format) {
		// We've found our pair, and have our sequence. Finalize
		s.ready = true
	} else if isFunctionIn(log.Format) {
		s.stack.Push(log)
	} else if isFunctionOut(log.Format) {
		if err != nil || !qlog.IsLogFnPair(top.Format, log.Format) {
			return errors.New(fmt.Sprintf("Error: Mismatched '%s' in "+
				"requestId %d log. ||%s|||%s||%s\n", qlog.FnExitStr,
				log.ReqId, top.Format, log.Format, err))
		}
		s.stack.Pop()
	}

	// Add to the sequence we're tracking
	s.seq = append(s.seq, log)
	return nil
}

type logStack []qlog.LogOutput

func newLogStack() logStack {
	return make([]qlog.LogOutput, 0)
}

func (s *logStack) Push(n qlog.LogOutput) {
	*s = append(*s, n)
}

func (s *logStack) Pop() {
	if len(*s) > 0 {
		*s = (*s)[:len(*s)-1]
	}
}

func (s *logStack) Peek() (qlog.LogOutput, error) {
	if len(*s) == 0 {
		return qlog.LogOutput{},
			errors.New("Cannot peek on an empty logStack")
	}

	return (*s)[len(*s)-1], nil
}

func saveToStat(file *os.File, patterns []patternData) {
	encoder := gob.NewEncoder(file)

	// Encode the length so we can have a progress bar on load
	patternLen := int(len(patterns))
	encoder.Encode(patternLen)

	// The gob package has an annoying and poorly thought out const cap on
	// Encode data max length. So, we have to encode in chunks we a new encoder
	// each time
	status := qlog.NewLogStatus(50)
	for i := 0; i < len(patterns); {
		// if a chunk is too large, Encode() will take disproportionally long
		for chunkSize := defaultChunkSize; chunkSize >= 1; chunkSize /= 2 {
			chunkPastEnd := i + chunkSize
			if chunkPastEnd > len(patterns) {
				chunkPastEnd = len(patterns)
			}

			err := encoder.Encode(patterns[i:chunkPastEnd])
			if err == nil {
				i = chunkPastEnd
				break
			}

			if chunkSize <= 1 {
				fmt.Printf("Unable to encode stat data into file: "+
					"%s\n", err)
				os.Exit(1)
			}
		}

		status.Process(float32(i) / float32(len(patterns)))
	}
}

func loadFromStat(file *os.File) []patternData {
	decoder := gob.NewDecoder(file)
	var rtn []patternData

	var patternLen int
	decoder.Decode(&patternLen)
	totalDecoded := 0

	status := qlog.NewLogStatus(50)
	// We have to encode in chunks, so keep going until we're out of data
	for {
		var chunk []patternData
		err := decoder.Decode(&chunk)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("Unable to decode stat contents: %s\n", err)
			os.Exit(1)
		}
		rtn = append(rtn, chunk...)

		totalDecoded += len(chunk)
		status.Process(float32(totalDecoded) / float32(patternLen))
	}
	status.Process(1)
	if totalDecoded != patternLen {
		panic(fmt.Sprintf("Statistics length mismatch in file: %d vs %d\n",
			totalDecoded, patternLen))
	}

	fmt.Printf("Loaded %d pattern results\n", len(rtn))
	return rtn
}

func genSeqStr(seq []qlog.LogOutput) string {
	return genSeqStrExt(seq, []bool{}, false)
}

// consecutiveWc specifies whether the output should contain more than one wildcard
// in sequence when they are back to back in wildcardMsdk
func genSeqStrExt(seq []qlog.LogOutput, wildcardMask []bool,
	consecutiveWc bool) string {

	rtn := ""
	outputWildcard := false
	for n := 0; n < len(seq); n++ {
		if n < len(wildcardMask) && wildcardMask[n] {
			// This is a wildcard in the sequence, but ensure that we
			// include consecutive wildcards if we need to
			if consecutiveWc || !outputWildcard {
				rtn += string([]byte{7})
				outputWildcard = true
			}
		} else {
			rtn += seq[n].Format
			outputWildcard = false
		}
	}

	return rtn
}

func isFunctionIn(test string) bool {
	return strings.Index(test, qlog.FnEnterStr) == 0
}

func isFunctionOut(test string) bool {
	return strings.Index(test, qlog.FnExitStr) == 0
}
