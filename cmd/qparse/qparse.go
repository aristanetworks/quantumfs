// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import "flag"
import "fmt"
import "math"
import "os"
import "sort"
import "sync"
import "strconv"
import "strings"
import "time"

import "github.com/aristanetworks/quantumfs/qlog"

var inFile *string
var outFile *string
var tabSpaces *int
var logOut *bool
var stats *bool
var topTotal *int
var showClose *bool
var stdDevMin *float64
var stdDevMax *float64
var wildMin *int
var wildMax *int
var maxThreads *int
var maxLenWildcards *int
var maxLen *int
var patternOut *bool

var wildcardStr string

// -- worker structures
func collectData(wildcards []bool, seq []qlog.LogOutput,
	sequences []qlog.SequenceData) qlog.SequenceData {

	var rtn qlog.SequenceData
	rtn.Seq = make([]qlog.LogOutput, len(seq), len(seq))
	copy(rtn.Seq, seq)

	for i := 0; i < len(sequences); i++ {
		if qlog.PatternMatches(seq, wildcards, sequences[i].Seq) {
			rtn.Times = append(rtn.Times, sequences[i].Times...)
		}
	}

	return rtn
}

type SortResultsTotal []qlog.PatternData

func (s SortResultsTotal) Len() int {
	return len(s)
}

func (s SortResultsTotal) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortResultsTotal) Less(i, j int) bool {
	if s[i].Sum == s[j].Sum {
		return (s[i].SeqStrRaw > s[j].SeqStrRaw)
	} else {
		return (s[i].Sum > s[j].Sum)
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

	inFile = flag.String("in", "", "Specify an input file")
	outFile = flag.String("out", "", "Specify an output file")
	tabSpaces = flag.Int("tab", 0,
		"Indent function logs with n spaces, when using -log")
	logOut = flag.Bool("log", false,
		"Parse a log file (-in) and print to stdout")
	patternOut = flag.Bool("patt", false,
		"Show patterns given in a stat file")
	stats = flag.Bool("stat", false, "Parse a log file (-in) and output to a "+
		"stats file (-out). Default stats filename is logfile.stats")
	topTotal = flag.Int("bytotal", 0, "Parse a stat file (-in) and "+
		"print top <bytotal> functions by total time usage in logs")
	showClose = flag.Bool("sims", false,
		"Don't hide similar sequences when using -bytotal")
	stdDevMin = flag.Float64("smin", 0, "Filter results, requiring minimum "+
		"standard deviation of <stdmin>. Float units of microseconds")
	stdDevMax = flag.Float64("smax", 1000000000,
		"Like smin, but setting a maximum")
	wildMin = flag.Int("wmin", 0, "Filter results, requiring minimum number "+
		"of wildcards in function pattern.")
	wildMax = flag.Int("wmax", 100, "Same as wmin, but setting a maximum")
	maxThreads = flag.Int("threads", 30, "Max threads to use (default 30)")
	maxLenWildcards = flag.Int("maxwc", 16,
		"Max sequence length to wildcard during -stat (default 16)")
	maxLen = flag.Int("maxlen", 10000,
		"Max sequence length to return in results")

	flag.Usage = func() {
		fmt.Printf("Usage: %s -in <filepath> [flags]\n\n", os.Args[0])
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
}

func printIndexedLog(idx int, sequence []qlog.LogOutput, wildcards []bool) {

	printIndexedLogExt(idx, sequence, wildcards, false)
}

func printIndexedLogExt(idx int, sequence []qlog.LogOutput, wildcards []bool,
	collapseWildcards bool) {

	fmt.Printf("=================%2d===================\n", idx)
	outputWildcard := false
	for j := 0; j < len(sequence); j++ {
		if j < len(wildcards) && wildcards[j] {
			// Don't show consecutive wildcards
			if !outputWildcard {
				fmt.Println("***Wildcards***")
				if collapseWildcards {
					outputWildcard = true
				}
			}
		} else {
			fmt.Printf("%s\n",
				strings.TrimSpace(sequence[j].Format))
			outputWildcard = false
		}
	}
}

func main() {
	flag.Parse()

	if len(os.Args) == 1 {
		flag.Usage()
		return
	}

	if *logOut {
		if *inFile == "" {
			fmt.Println("To -log, you must specify a log file with -in")
			os.Exit(1)
		}

		// Log parse mode only
		fmt.Println(qlog.ParseLogsExt(*inFile, *tabSpaces,
			*maxThreads))
	} else if *patternOut {
		if *inFile == "" {
			fmt.Println("To -patt, you must specify a stat " +
				"file with -in")
			os.Exit(1)
		}

		fmt.Println("Loading file...")

		file, err := os.Open(*inFile)
		if err != nil {
			fmt.Printf("Unable to open stat file %s: %s\n", *inFile, err)
			os.Exit(1)
		}
		defer file.Close()
		patterns := qlog.LoadFromStat(file)
		for i := 0; i < len(patterns); i++ {
			printIndexedLogExt(i, patterns[i].Data.Seq,
				patterns[i].Wildcards, true)
		}
	} else if *stats {
		if *inFile == "" {
			fmt.Println("To -stat, you must specify a log file with -in")
			os.Exit(1)
		}
		outFilename := *inFile + ".stats"
		if *outFile != "" {
			outFilename = *outFile
		}

		pastEndIdx, dataArray, strMap := qlog.ExtractFields(*inFile)
		logs := qlog.OutputLogsExt(pastEndIdx, dataArray, strMap,
			*maxThreads, true)

		patterns := getStatPatterns(logs)

		fmt.Println("Saving to stat file...")
		file, err := os.Create(outFilename)
		if err != nil {
			fmt.Printf("Unable to create %s for new data: %s\n",
				outFilename, err)
			os.Exit(1)
		}
		defer file.Close()
		qlog.SaveToStat(file, patterns)
		fmt.Printf("Stats file created: %s\n", outFilename)
	} else if *topTotal != 0 {
		if *inFile == "" {
			fmt.Println("To -topTotal, you must specify a stat file "+
				"with -in")
			os.Exit(1)
		}

		fmt.Println("Loading file...")
		file, err := os.Open(*inFile)
		if err != nil {
			fmt.Printf("Unable to open stat file %s: %s\n", *inFile, err)
			os.Exit(1)
		}
		defer file.Close()
		patterns := qlog.LoadFromStat(file)
		showTopTotalStats(patterns, *stdDevMin, *stdDevMax, *wildMin,
			*wildMax, *maxLen)
	} else {
		fmt.Println("No action flags (-log, -stat) specified.")
		os.Exit(1)
	}
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

// Returns true if a is a superset of b
func superset(a []qlog.TimeData, b []qlog.TimeData) bool {
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

func (l *ConcurrentMap) StrExists(key string) bool {
//	l.mutex.RLock()
//	defer l.mutex.RUnlock()

	_, exists := l.strToList[key]
	return exists
}

func (l *ConcurrentMap) SetStr(key string, listKey string) {
	l.strToList[key] = listKey
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

func recurseGenPatterns(seq []qlog.LogOutput, sequences []qlog.SequenceData,
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
	seq []qlog.LogOutput, sequences []qlog.SequenceData, out *ConcurrentMap /*out*/) {

	// If we've already got a result for this sequence, then this has
	// been generated and we can skip it. This would not be safe if we didn't
	// include multiple consecutive wildcards because small sequences would
	// occlude longer versions.
	expandedStr := genSeqStrExt(seq, curMask, true)
	if out.StrExists(expandedStr) {
		return
	}

	// Count how many unique sequences match this sequence (with wildcards)
	matches := 0
	matchStr := ""
	for i := 0; i < len(sequences); i++ {
		if qlog.PatternMatches(seq, curMask, sequences[i].Seq) {
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
		printIndexedLog(0, seq, curMask)
		panic(fmt.Sprintf("Sequence doesn't even match against itself"))
	}

	// Make sure that we mark that we've visited this expanded sequence
	out.SetStr(expandedStr, matchStr)

	// If there's already an identical mapping on ConcurrentMap, there's no point
	// in returning both. Only choose the one with more effective wildcards
	oldEntry := out.Listed(matchStr)

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
		out.Set(expandedStr, matchStr, &newEntry)
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

func getStatPatterns(logs []qlog.LogOutput) []qlog.PatternData {
	sequenceMap := qlog.ExtractSequences(logs)
	sequences := make([]qlog.SequenceData, len(sequenceMap), len(sequenceMap))
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

		seq := sequences[i].Seq

		recurseGenPatterns(seq, sequences, &patterns /*out*/)

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
		recurseGenPatterns_(wildcardFilled, len(seq)-1, seq, sequences,
			&patterns)
	}
	status.Process(1)

	// we MUST preallocate here. I had been using append with just a size hint,
	// and for some reason it resulted in a number of zero entries here. Go bug?
	rawResults := make([]qlog.PatternData, len(patterns.dataByList),
		len(patterns.dataByList))

	// Now collect all the data for the sequences, allowing wildcards
	fmt.Printf("Collecting data for each of %d wildcard-ed subsequences...\n",
		len(patterns.dataByList))
	status = qlog.NewLogStatus(50)
	mapIdx := 0
	for _, wcseq := range patterns.dataByList {
		status.Process(float32(mapIdx) / float32(len(patterns.dataByList)))

		var newResult qlog.PatternData
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

		rawResults[mapIdx] = newResult
		mapIdx++
	}
	status.Process(1)

	return rawResults
}

// stddev units are microseconds
func showTopTotalStats(patterns []qlog.PatternData, minStdDev float64, maxStdDev float64,
	minWildcards int, maxWildcards int, maxLen int) {

	// Now sort by total time usage
	fmt.Println("Sorting data by total time usage...")
	sort.Sort(SortResultsTotal(patterns))

	minStdDevNano := int64(minStdDev * 1000)
	maxStdDevNano := int64(maxStdDev * 1000)

	funcResults := make([]qlog.PatternData, 0)
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

		if len(funcResults) >= *topTotal {
			break
		}
	}

	fmt.Println("Top function patterns by total time used:")
	var lastTimes []qlog.TimeData
	count := 0
	for i := 0; i < len(funcResults); i++ {
		result := funcResults[i]

		if !(*showClose) {
			// If this dataset is a subset of the last, then we've
			// already output the most wildcarded version of this
			// sequence so let's not print redundant information
			if superset(lastTimes, result.Data.Times) {
				continue
			}
		}
		lastTimes = result.Data.Times

		printIndexedLogExt(count+1, result.Data.Seq, result.Wildcards, true)
		fmt.Println("--------------------------------------")
		fmt.Printf("Total sequence time: %12s\n",
			time.Duration(result.Sum).String())
		fmt.Printf("Average sequence time: %12s\n",
			time.Duration(result.Avg).String())
		fmt.Printf("Number of samples: %d\n", len(result.Data.Times))
		fmt.Printf("Sequence Index: %d\n", i)
		fmt.Printf("Standard Deviation: %12s\n",
			time.Duration(result.Stddev).String())
		fmt.Println("")
		count++
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
