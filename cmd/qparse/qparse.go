// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import "encoding/gob"
import "errors"
import "flag"
import "fmt"
import "io"
import "math"
import "os"
import "sort"
import "sync"
import "strconv"
import "strings"
import "time"

import "github.com/aristanetworks/quantumfs/qlog"

var file *string
var statFile *string
var tabSpaces *int
var logOut *bool
var stats *bool
var topTotal *bool
var stdDevMin *float64
var stdDevMax *float64
var wildMin *int
var wildMax *int
var maxThreads *int
var maxLenWildcards *int
var maxLen *int

var wildcardStr string

// -- worker structures

type SequenceData struct {
	Times	[]int64
	Seq	[]qlog.LogOutput
}

type PatternData struct {
	SeqStrRaw	string	// No wildcards in this string, just seq
	Data		SequenceData
	Wildcards	[]bool
	Avg		int64
	Sum		int64
	Stddev		int64
}

type sequenceTracker struct {
	stack		logStack

	ready		bool
	seq		[]qlog.LogOutput
}

func newSequenceTracker() sequenceTracker {
	return sequenceTracker {
		stack:	newLogStack(),
		ready:	false,
		seq:	make([]qlog.LogOutput, 0),
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
	} else if qlog.IsFnIn(log.Format) {
		s.stack.Push(log)
	} else if qlog.IsFnOut(log.Format) {
		if err != nil || !qlog.IsLogFnPair(top.Format, log.Format) {
			return errors.New(fmt.Sprintf("Error: Mismatched '%s' in "+
				"requestId %d log\n",
				qlog.FnExitStr, log.ReqId))
		}
		s.stack.Pop()
	}

	// Add to the sequence we're tracking
	s.seq = append(s.seq, log)
	return nil
}

func collectData(wildcards []bool, seq []qlog.LogOutput,
	sequences []SequenceData) SequenceData {

	var rtn SequenceData
	rtn.Seq = make([]qlog.LogOutput, len(seq), len(seq))
	copy(rtn.Seq, seq)

	for i := 0; i < len(sequences); i++ {
		if qlog.PatternMatches(seq, wildcards, sequences[i].Seq) {
			rtn.Times = append(rtn.Times, sequences[i].Times...)
		}
	}

	return rtn
}

// Because Golang is a horrible language and doesn't support maps with slice keys,
// we need to construct long string keys and save the slices in the value for later
func extractSequences(logs []qlog.LogOutput) map[string]SequenceData {
	trackerMap := make(map[uint64][]sequenceTracker)
	trackerCount := 0

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

		// Grab the sequenceTracker list for this request
		trackers, exists := trackerMap[reqId]
		if len(trackers) == 0 && exists {
			// If there's an empty entry that already exists, that means
			// this request had an error and was aborted. Leave it alone.
			continue
		}

		// Start a new subsequence if we need to
		if qlog.IsFnIn(logs[i].Format) {
			trackers = append(trackers, newSequenceTracker())
		}

		abortRequest := false
		// Inform all the trackers of the new token
		for k := 0; k < len(trackers); k++ {
			err := trackers[k].Process(logs[i])
			if err != nil {
				fmt.Println(err)
				abortRequest = true
				break
			}
		}

		if abortRequest {
			// Mark the request as bad
			trackerMap[reqId] = make([]sequenceTracker, 0)
			continue
		}

		// Only update entry if it won't be empty
		if exists || len(trackers) > 0 {
			if len(trackers) != len(trackerMap[reqId]) {
				trackerCount++
			}
			trackerMap[reqId] = trackers
		}
	}
	status.Process(1)

	fmt.Printf("Collating subsequence time data into map with %d entries...\n",
		trackerCount)
	status = qlog.NewLogStatus(50)

	// After going through the logs, add all our sequences to the rtn map
	rtn := make(map[string]SequenceData)
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
				rawSeq[len(rawSeq)-1].T-rawSeq[0].T)
			rtn[seq] = data
		}
	}
	status.Process(1)

	return rtn
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

type SortResultsTotal []PatternData

func (s SortResultsTotal) Len() int {
	return len(s)
}

func (s SortResultsTotal) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortResultsTotal) Less(i, j int) bool {
	if s[i].Sum == s[j].Sum {
		return (s[i].SeqStrRaw < s[j].SeqStrRaw)
	} else {
		return (s[i].Sum < s[j].Sum)
	}
}

type SortReqs []uint64

func (s SortReqs) Len() int {
	return len(s)
}

func (s SortReqs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortReqs) Less(i, j int) bool {
	return s[i] < s[j]
}

// -- end

func init() {
	// The wildcard character / string needs to be something that would never
	// show in a log so that we can use strings as map keys
	wildcardStr = string([]byte { 7 })

	file = flag.String("f", "", "Specify a log file")
	statFile = flag.String("fstat", "", "Specify a statistics file")
	tabSpaces = flag.Int("tab", 0,
		"Indent function logs with n spaces, when using -log")
	logOut = flag.Bool("log", false, "Parse a log file (-f) and print to stdout")
	stats = flag.Bool("stat", false, "Parse a log file (-f) and output to a "+
		"stats file (-fstat). Default stats filename is logfile.stats")
	topTotal = flag.Bool("bytotal", false, "Parse a stat file (-fstat) and "+
		"print top functions by total time usage in logs")
	stdDevMin = flag.Float64("smin", 0, "Filter results, requiring minimum "+
		"standard deviation of <stdmin>. Float units of microseconds")
	stdDevMax = flag.Float64("smax", 1000000, "Like smin, but setting a maximum")
	wildMin = flag.Int("wmin", 0, "Filter results, requiring minimum number "+
		"of wildcards in function pattern.")
	wildMax = flag.Int("wmax", 100, "Same as wmin, but setting a maximum")
	maxThreads = flag.Int("threads", 30, "Max threads to use (default 30)")
	maxLenWildcards = flag.Int("maxwc", 16,
		"Max sequence length to wildcard during -stat (default 16)")
	maxLen = flag.Int("maxlen", 10000,
		"Max sequence length to return in results")

	flag.Usage = func() {
		fmt.Printf("Usage: %s -f <filepath> [flags]\n\n", os.Args[0])
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
}

func saveToStat(filename string, patterns []PatternData) {
	fmt.Println("Saving to stat file...")

	file, err := os.Create(filename)
	if err != nil {
		fmt.Printf("Unable to create %s for new data: %s\n", filename, err)
		os.Exit(1)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)

	// The gob package has an annoying and poorly thought out const cap on
	// Encode data max length. So, we have to encode in chunks we a new encoder
	// each time
	const chunkSize = 10
	for i := 0; i < len(patterns); i += chunkSize {
		chunkPastEnd := i + chunkSize
		if chunkPastEnd > len(patterns) {
			chunkPastEnd = len(patterns)
		}

		err = encoder.Encode(patterns[i:chunkPastEnd])
		if err != nil {
			fmt.Printf("Unable to encode stat data into file: %s\n", err)
			os.Exit(1)
		}
	}
}

func loadFromStat(filename string) []PatternData {
	fmt.Println("Loading file...")

	file, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Unable to open stat file %s: %s\n", filename, err)
		os.Exit(1)
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var rtn []PatternData

	// We have to encode in chunks, so keep going until we're out of data
	for {
		var chunk []PatternData
		err = decoder.Decode(&chunk)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("Unable to decode stat contents: %s\n", err)
			os.Exit(1)
		}
		rtn = append(rtn, chunk...)
	}

	fmt.Printf("Loaded %d pattern results\n", len(rtn))
	return rtn
}

func main() {
	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		return
	}

	if *logOut {
		if *file == "" {
			fmt.Println("To -log, you must specify a log file with -f")
			os.Exit(1)
		}

		// Log parse mode only
		fmt.Println(qlog.ParseLogsExt(*file, *tabSpaces,
			*maxThreads))
	} else if *stats {
		if *file == "" {
			fmt.Println("To -stat, you must specify a log file with -f")
			os.Exit(1)
		}
		outFile := *file + ".stats"
		if *statFile != "" {
			outFile = *statFile
		}

		pastEndIdx, dataArray, strMap := qlog.ExtractFields(*file)
		logs := qlog.OutputLogsExt(pastEndIdx, dataArray, strMap,
			*maxThreads, true)

		patterns := getStatPatterns(logs)
		saveToStat(outFile, patterns)
		fmt.Printf("Stats file created: %s\n", outFile)
	} else if *topTotal {
		if *statFile == "" {
			fmt.Println("To -topTotal, you must specify a stat file "+
				"with -fstat")
			os.Exit(1)
		}

		patterns := loadFromStat(*statFile)
		showTopTotalStats(patterns, *stdDevMin, *stdDevMax, *wildMin,
			*wildMax, *maxLen)
	} else {
		fmt.Println("No action flags (-log, -stat) specified.")
		os.Exit(1)
	}
}

func showHelp() {
	fmt.Println("Commands:")
	fmt.Println("topTotal <stddevmin> <stddevmax> <wmax>    "+
		"List top function patterns by avg total time used where patterns")
	fmt.Println("                                           "+
		"contain < wmax wildcards, and stddevmin < sigma < stddevmax. Units")
	fmt.Println("                                           "+
		"for stddev are microseconds.")
	fmt.Println("topFn <stddevmin> <stddevmax> <wmax>       "+
		"Like topTotal, except using times per call")
	fmt.Println("ids                                        "+
		"List all request ids in log")
	fmt.Println("log <id>                                   "+
		"Show log sequence for request <id>")
	fmt.Println("exit                                       "+
		"Exit and return to the shell")
	fmt.Println("")
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
				rtn += wildcardStr
				outputWildcard = true
			}
		} else {
			rtn += seq[n].Format
			outputWildcard = false
		}
	}

	return rtn
}

type wildcardedSequence struct {
	sequence	[]qlog.LogOutput
	wildcards	[]bool
}

func newWildcardedSeq(seq []qlog.LogOutput, wc []bool) wildcardedSequence {

	var rtn wildcardedSequence
	rtn.sequence = make([]qlog.LogOutput, len(seq), len(seq))
	rtn.wildcards = make([]bool, len(wc), len(wc))
	copy(rtn.sequence, seq)
	copy(rtn.wildcards, wc)

	return rtn
}

type ConcurrentMap struct {
	mutex		sync.RWMutex
	dataByList	map[string]*wildcardedSequence
	strToList	map[string]string
}

func (l *ConcurrentMap) Exists(key string) bool {
//	l.mutex.RLock()
//	defer l.mutex.RUnlock()

	_, exists := l.strToList[key]
	return exists
}

func (l *ConcurrentMap) Listed(listAsStr string) *wildcardedSequence {
	entry, exists := l.dataByList[listAsStr]
	if exists {
		return entry
	}

	return nil
}

func (l *ConcurrentMap) Set(newKey string, newListKey string,
	newData *wildcardedSequence) {

//	l.mutex.Lock()
//	defer l.mutex.Unlock()

	l.dataByList[newListKey] = newData
	l.strToList[newKey] = newListKey
}

func recurseGenPatterns(seq []qlog.LogOutput, sequences []SequenceData,
	out *ConcurrentMap /*out*/) {

	if len(seq) > *maxLenWildcards {
		return
	}

	// Start with all wildcards
	wildcardMask := make([]bool, len(seq), len(seq))
	for j := 1; j < len(wildcardMask)-1; j++ {
		wildcardMask[j] = true
	}

	recurseGenPatterns_(wildcardMask, 1, seq, sequences, out)
}

// curMask is a map of indices into sequences which should be ignored when gathering
// data (wildcards). We check how many sequences match with the current wildcard mask
// and if only one sequence matches then we know that no others will (since as we
// recurse deeper, we remove wildcards and only become more specific) and escape
func recurseGenPatterns_(curMask []bool, wildcardStartIdx int,
	seq []qlog.LogOutput, sequences []SequenceData, out *ConcurrentMap /*out*/) {

	// If we've already got a result for this sequence, then this has
	// been generated and we can skip it. This would not be safe if we didn't
	// include multiple consecutive wildcards because small sequences would
	// occlude longer versions.
	if out.Exists(genSeqStrExt(seq, curMask, true)) {
		return
	}

	// Count how many unique sequences match this sequence (with wildcards)
	matches := 0
	matchStr := ""
	for i := 0; i < len(sequences); i++ {
		if qlog.PatternMatches(seq, curMask, sequences[i].Seq) {
			matches++
			matchStr += strconv.Itoa(i) + "|"
		}
	}

	if matches <= 1 {
		// There are no interesting sequence / wildcard combos deeper
		return
	}

	// If there's already an identical mapping on ConcurrentMap, there's no point
	// in returning both. Only choose the one with more effective wildcards
	oldEntry := out.Listed(matchStr)
	setEntry := false
	if oldEntry == nil || (countWildcards(curMask, false) <
		countWildcards(oldEntry.wildcards, false)) {

		setEntry = true
	}

	if setEntry {
		// we compress wildcards when adding an entry,
		// but can't do so when recursing
		seqStr := genSeqStrExt(seq, curMask, false)

		newEntry := newWildcardedSeq(seq, curMask)
		out.Set(seqStr, matchStr, &newEntry)
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
				recurseGenPatterns_(curMask, i+1, seq, sequences,
					out)
				// Make sure to fix the entry for the next loop
				curMask[i] = true
			}
		}
	}
}

func getStatPatterns(logs []qlog.LogOutput) []PatternData {
	sequenceMap := extractSequences(logs)
	sequences := make([]SequenceData, len(sequenceMap), len(sequenceMap))
	idx := 0
	for _, v := range sequenceMap {
		sequences[idx] = v
		idx++
	}

	status := qlog.NewLogStatus(50)

	// Now generate all combinations of the sequences with wildcards in them, but
	// start with an almost completely wildcarded sequence and recurse towards
	// a less-so one. We only care about subsequences that match more than one
	// other sequence, because they are uninteresting if they only belong to one.
	// By starting at the most wildcarded sequence, we can disregard entire
	// branches as we recurse to save time!
	fmt.Printf("Generating all log patterns from %d unique sequences...\n",
		len(sequences))
	patterns := ConcurrentMap {
		dataByList:	make(map[string]*wildcardedSequence),
		strToList:	make(map[string]string),
	}
	for i := 0; i < len(sequences); i++ {
		// Update the status bar
		status.Process(float32(i) / float32(len(sequences)))

		recurseGenPatterns(sequences[i].Seq, sequences, &patterns /*out*/)

		matchStr := strconv.Itoa(i)
		// recurseGenPatterns will overlook the "no wildcards" sequence,
		// so we must add that ourselves
		newEntry := newWildcardedSeq(sequences[i].Seq, []bool{})
		patterns.Set(genSeqStr(sequences[i].Seq), matchStr, &newEntry)
	}
	status.Process(1)

	rawResults := make([]PatternData, len(patterns.dataByList))

	// Now collect all the data for the sequences, allowing wildcards
	fmt.Printf("Collecting data for each of %d wildcard-ed subsequences...\n",
		len(patterns.dataByList))
	status = qlog.NewLogStatus(50)
	mapIdx := 0
	for _, wcseq := range patterns.dataByList {
		status.Process(float32(mapIdx) / float32(len(patterns.dataByList)))
		mapIdx++

		var newResult PatternData
		newResult.SeqStrRaw = genSeqStr(wcseq.sequence)
		newResult.Wildcards = wcseq.wildcards
		newResult.Data = collectData(wcseq.wildcards, wcseq.sequence,
			sequences)

		// Precompute more useful stats
		var avgSum int64
		for i := 0; i < len(newResult.Data.Times); i++ {
			avgSum += newResult.Data.Times[i]
		}
		newResult.Sum = avgSum
		newResult.Avg = avgSum / int64(len(newResult.Data.Times))

		// Now we can compute the standard deviation of the data. This is
		// used to determine whether a pattern's average time is probably
		// caused by exactly that pattern's sequence, and not a subsequence
		var deviationSum float64
		for i := 0; i < len(newResult.Data.Times); i++ {
			deviation := newResult.Data.Times[i] - newResult.Avg
			deviationSum += float64(deviation * deviation)
		}
		newResult.Stddev = int64(math.Sqrt(deviationSum))

		rawResults = append(rawResults, newResult)
	}
	status.Process(1)

	// Now sort by total time usage
	sort.Sort(SortResultsTotal(rawResults))
if false {
	fmt.Println("Filtering out duplicate entries...")
	status = qlog.NewLogStatus(50)
	// Filter out any duplicates (resulting from ineffectual wildcards)
	currentData := PatternData{}
	filteredResults := make([]PatternData, 0)
	for i := 0; i <= len(rawResults); i++ {
		status.Process(float32(i) / float32(len(rawResults)))

		var result PatternData
		if i < len(rawResults) {
			result = rawResults[i]
		}

		// If the sequences are the same, but have different stats then
		// they should be considered different
		matchingSeqWildcarded := (qlog.PatternMatches(result.Data.Seq,
			result.Wildcards, currentData.Data.Seq) ||
			qlog.PatternMatches(currentData.Data.Seq,
			currentData.Wildcards, result.Data.Seq))

		if i == 0 ||
			result.Sum != currentData.Sum ||
			result.Avg != currentData.Avg ||
			result.Stddev != currentData.Stddev ||
			!matchingSeqWildcarded {

			// We've finished going through a group of duplicates, so
			// add their most wildcarded member
			if i > 0 {
				filteredResults = append(filteredResults,
					currentData)
			}
			currentData = result
		} else {
			// We have a duplicate
			if countWildcards(result.Wildcards, true) >
				countWildcards(currentData.Wildcards, true) {

				currentData = result
			}
		}
	}
	fmt.Printf("Number of results now: %d\n", len(filteredResults))
}
	return rawResults
}

// stddev units are microseconds
func showTopTotalStats(patterns []PatternData, minStdDev float64, maxStdDev float64,
	minWildcards int, maxWildcards int, maxLen int) {

	minStdDevNano := int64(minStdDev * 1000)
	maxStdDevNano := int64(maxStdDev * 1000)

	funcResults := make([]PatternData, 0)
	// Go through all the patterns and collect ones within stddev
	for i := 0; i < len(patterns); i++ {
		wildcards := countWildcards(patterns[i].Wildcards, false)
		if wildcards > maxWildcards || wildcards < minWildcards {
			continue
		}

		if len(patterns[i].Data.Seq) - (countWildcards(patterns[i].Wildcards,
			true) - wildcards) > maxLen {

			continue
		}

		// time package's units are nanoseconds. So we need to convert our
		// microsecond stddev bounds to nanoseconds so we can compare
		if minStdDevNano <= patterns[i].Stddev &&
			patterns[i].Stddev <= maxStdDevNano {

			funcResults = append(funcResults, patterns[i])
		}
	}

	fmt.Println("Top function patterns by total time used:")
	count := 0
	for i := len(funcResults)-1; i >= 0; i-- {
		result := funcResults[i]

		fmt.Printf("=================%2d===================\n", count+1)
		outputWildcard := false
		for j := 0; j < len(result.Data.Seq); j++ {
			if j < len(result.Wildcards) && result.Wildcards[j] {
				// Don't show consecutive wildcards
				if !outputWildcard {
					fmt.Println("***Wildcards***")
					outputWildcard = true
				}
			} else {
				fmt.Printf("%s\n",
					strings.TrimSpace(result.Data.Seq[j].Format))
				outputWildcard = false
			}
		}
		fmt.Println("--------------------------------------")
		fmt.Printf("Total sequence time: %12s\n",
			time.Duration(result.Sum).String())
		fmt.Printf("Average sequence time: %12s\n",
			time.Duration(result.Avg).String())
		fmt.Printf("Number of samples: %d\n", len(result.Data.Times))
		fmt.Printf("Standard Deviation: %12s\n",
			time.Duration(result.Stddev).String())
		fmt.Println("")
		count++

		// Stop when we've output enough of the top
		if count >= 30 {
			break;
		}
	}
}

func extractRequestIds(logs []qlog.LogOutput) []uint64 {
	idMap := make(map[uint64]bool)

	for i := 0; i < len(logs); i++ {
		idMap[logs[i].ReqId] = true
	}

	keys := make([]uint64, 0)
	for k, _ := range idMap {
		keys = append(keys, k)
	}
	sort.Sort(SortReqs(keys))

	return keys
}

func showRequestIds(logs []qlog.LogOutput) {
	keys := extractRequestIds(logs)

	// Get the max length we're going to output
	maxReqStr := fmt.Sprintf("%d", keys[len(keys)-1])
	padLen := strconv.Itoa(len(maxReqStr))

	fmt.Println("Request IDs in log:")
	counter := 0
	for i := 0; i < len(keys); i++ {
		fmt.Printf("%" + padLen + "d ", keys[i])

		counter++
		if counter == 5 {
			fmt.Println("")
			counter = 0
		}
	}
	fmt.Println("")
}

func getReqLogs(reqId uint64, logs []qlog.LogOutput) []qlog.LogOutput {
	filteredLogs := make([]qlog.LogOutput, 0)
	for i := 0; i < len(logs); i++ {
		if logs[i].ReqId == reqId {
			filteredLogs = append(filteredLogs, logs[i])
		}
	}

	return filteredLogs
}

func showLogs(reqId uint64, logs []qlog.LogOutput) {
	filteredLogs := getReqLogs(reqId, logs)

	if len(filteredLogs) == 0 {
		fmt.Printf("No logs present for request id %d\n", reqId)
		return
	}

	fmt.Println(qlog.FormatLogs(filteredLogs, *tabSpaces))
}
