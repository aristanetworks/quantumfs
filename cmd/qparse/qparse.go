// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import "bufio"
import "errors"
import "flag"
import "fmt"
import "math"
import "os"
import "regexp"
import "sort"
import "sync"
import "strconv"
import "strings"
import "time"

import "github.com/aristanetworks/quantumfs/qlog"

var tabSpaces *int
var file *string
var stats *bool
var maxThreads *int

var wildcardStr string
// All possible combinations of wildcards becomes exponentially large. Given a
// sequence length, here's the number of wildcards that can be used to keep
// nunber of iterations <= around 700k. Sequences > 30 in length won't be wildcarded.
var maxCombinations map[int]int
var longCombinationsStart int

// -- worker structures

type sequenceData struct {
	times	[]int64
	seq	[]qlog.LogOutput
}

type patternData struct {
	data		sequenceData
	wildcards	[]bool
	avg		int64
	sum		int64
	stddev		float64
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
	sequences []sequenceData) sequenceData {

	var rtn sequenceData
	rtn.seq = make([]qlog.LogOutput, len(seq), len(seq))
	copy(rtn.seq, seq)

	regex := regexp.MustCompile(genSeqRegex(seq, wildcards))

	for i := 0; i < len(sequences); i++ {
		toMatch := genSeqRegex(sequences[i].seq, []bool{})
		if matches := regex.FindAllStringSubmatch(toMatch,
			1); len(matches) > 0 {

			rtn.times = append(rtn.times, sequences[i].times...)
		}
	}

	return rtn
}

// Because Golang is a horrible language and doesn't support maps with slice keys,
// we need to construct long string keys and save the slices in the value for later
func extractSequences(logs []qlog.LogOutput) map[string]sequenceData {
	trackerMap := make(map[uint64][]sequenceTracker)

	fmt.Println("Extracing sub-sequences from logs...")
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
			trackerMap[reqId] = trackers
		}
	}
	status.Process(1)

	fmt.Println("Collating subsequence time data into map...")
	status = qlog.NewLogStatus(50)

	// After going through the logs, add all our sequences to the rtn map
	rtn := make(map[string]sequenceData)
	mapIdx := 0
	for reqId, trackers := range trackerMap {
		status.Process(float32(mapIdx) / float32(len(trackerMap)))
		mapIdx++

		// Skip any requests marked as bad
		if len(trackers) == 0 {
			continue
		}

		// Go through each tracker
		for k := 0; k < len(trackers); k++ {
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
			if data.seq == nil {
				data.seq = rawSeq
			}
			data.times = append(data.times,
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

type SortResultsTotal []patternData

func (s SortResultsTotal) Len() int {
	return len(s)
}

func (s SortResultsTotal) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortResultsTotal) Less(i, j int) bool {
	return s[i].sum < s[j].sum
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
	maxCombinations = map[int]int {
		30:	6,
		29:	6,
		28:	6,
		27:	6,
		26:	6,
		25:	7,
		24:	7,
		23:	7,
		22:	8,
		21:	9,
		20:	11,
	}
	longCombinationsStart = 20

	tabSpaces = flag.Int("tab", 0, "Indent function logs with n spaces")
	file = flag.String("f", "", "Log file to parse (required)")
	stats = flag.Bool("stats", false, "Enter interactive mode to read stats")
	maxThreads = flag.Int("threads", 16, "Max threads to use")

	flag.Usage = func() {
		fmt.Printf("Usage: %s -f <filepath> [flags]\n\n", os.Args[0])
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if len(*file) == 0 {
		flag.Usage()
		return
	}

	if !(*stats) {
		// Log parse mode only
		fmt.Println(qlog.ParseLogsExt(*file, *tabSpaces, *maxThreads))
		return
	} else {
		interactiveMode(*file)
	}
}

func interactiveMode(filepath string) {
	fmt.Println(">>> Entered interactive log parse mode")
	reader := bufio.NewReader(os.Stdin)

	// Parse the logs into log structs
	pastEndIdx, dataArray, strMap := qlog.ExtractFields(filepath)
	logs := qlog.OutputLogsExt(pastEndIdx, dataArray, strMap, *maxThreads, true)

	for {
		fmt.Printf(">> ")
		text, _ := reader.ReadString('\n')

		// Strip off the newline
		text = text[:len(text)-1]

		menuProcess(text, logs)
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

func countWildcards(mask []bool) int {
	count := 0
	for i := 0; i < len(mask); i++ {
		if mask[i] {
			count++
		}
	}
	return count
}

func genSeqStr(seq []qlog.LogOutput) string {
	return genSeqStrExt(seq, []bool{})
}

func genSeqStrExt(seq []qlog.LogOutput, wildcardMask []bool) string {
	rtn := ""
	for n := 0; n < len(seq); n++ {
		if n < len(wildcardMask) && wildcardMask[n] {
			// This is a wildcard in the sequence, but skip consecutive
			// wildcards
			if rtn[len(rtn)-1:] == wildcardStr {
				continue
			}

			rtn += wildcardStr
			continue
		}

		rtn += seq[n].Format
	}

	return rtn
}

// This function is used to generate regex strings for checking for matches
func genSeqRegex(seq []qlog.LogOutput, wildcards []bool) string {

	rtn := "?s:"
	for n := 0; n < len(seq); n++ {
		if n < len(wildcards) && wildcards[n] {
			rtn += ".*"
		} else {
			rtn += seq[n].Format
		}
	}

	return "("+rtn+")"
}

type wildcardedSequence struct {
	sequence	[]qlog.LogOutput
	wildcards	[]bool
}

type ConcurrentMap struct {
	mutex	sync.RWMutex
	data	map[string]wildcardedSequence
}

func (l *ConcurrentMap) Exists(key string) bool {
//	l.mutex.RLock()
//	defer l.mutex.RUnlock()

	_, exists := l.data[key]
	return exists
}

func (l *ConcurrentMap) Set(newKey string, newData wildcardedSequence) {
//	l.mutex.Lock()
//	defer l.mutex.Unlock()

	l.data[newKey] = newData
}

// curMask is a map of indices into sequences which should be ignored when gathering
// data (wildcards). The base cases of this function are when its called with zero
// remaining in wildcardNum - that's how it knows to use curMask and collect the
// data
func recurseGenPatterns(curMask []bool, wildcardNum int, wildcardStartIdx int,
	seq []qlog.LogOutput, sequences []sequenceData, out *ConcurrentMap /*out*/) {

	if wildcardNum > 0 {
		// we need to place a new wildcard. Note: we don't allow the first /
		// last logs to be wildcarded since we *know* they're functions and
		// they define the function we're trying to analyze across variants
		for i := wildcardStartIdx; i < len(seq)-1; i++ {
			if !curMask[i] {
				// This spot doesn't have a wildcard yet.
				curMask[i] = true
				recurseGenPatterns(curMask, wildcardNum-1, i+1, seq,
					sequences, out)
				// Make sure to remove the entry for the next loop
				curMask[i] = false
			}
		}
	} else {
		seqStr := genSeqStrExt(seq, curMask)

		var newPattern wildcardedSequence
		newPattern.sequence = make([]qlog.LogOutput, len(seq), len(seq))
		newPattern.wildcards = make([]bool, len(curMask), len(curMask))
		copy(newPattern.sequence, seq)
		copy(newPattern.wildcards, curMask)
		out.Set(seqStr, newPattern)
	}
}

func getStatPatterns(logs []qlog.LogOutput) []patternData {
	sequenceMap := extractSequences(logs)
	sequences := make([]sequenceData, len(sequenceMap), len(sequenceMap))
	idx := 0
	for _, v := range sequenceMap {
		sequences[idx] = v
		idx++
	}

	status := qlog.NewLogStatus(50)

	// Now generate all combinations of the sequences with wildcards in them,
	// and collect stats for each generated sequence
	fmt.Println("Generating all log sequence patterns...")
	patterns := ConcurrentMap {
		data:	make(map[string]wildcardedSequence),
	}
	for i := 0; i < len(sequences); i++ {
		// Update the status bar
		status.Process(float32(i) / float32(len(sequences)))

		curSeq := sequences[i].seq

		// If we've already got a result for this sequence, then this has
		// been generated and we can skip it.
		if patterns.Exists(genSeqStr(curSeq)) {
			continue
		}

		maxWildcards := len(curSeq) - 2
		if maxWildcards >= longCombinationsStart {
			wildcardsSupported, exists := maxCombinations[maxWildcards]
			if exists {
				maxWildcards = wildcardsSupported
			} else {
				// if the sequence is too long, then do no wildcards
				maxWildcards = 0
			}
		}

		for wildcards := 0; wildcards < maxWildcards; wildcards++ {
			wildcardMask := make([]bool, len(curSeq), len(curSeq))
			recurseGenPatterns(wildcardMask, wildcards, 1, curSeq,
				sequences, &patterns /*out*/)
		}
	}
	status.Process(1)

	rtn := make([]patternData, len(patterns.data))

	// Now collect all the data for the sequences, allowing wildcards
	fmt.Println("Collecting data for each wildcard-ed subsequence...", len(patterns.data))
	status = qlog.NewLogStatus(50)
	mapIdx := 0
	for _, wcseq := range patterns.data {
		status.Process(float32(mapIdx) / float32(len(patterns.data)))
		mapIdx++

		var newResult patternData
		newResult.wildcards = wcseq.wildcards
		newResult.data = collectData(wcseq.wildcards, wcseq.sequence,
			sequences)

		// Precompute more useful stats
		var avgSum int64
		for i := 0; i < len(newResult.data.times); i++ {
			avgSum += newResult.data.times[i]
		}
		newResult.sum = avgSum
		newResult.avg = avgSum / int64(len(newResult.data.times))

		// Now we can compute the standard deviation of the data. This is
		// used to determine whether a pattern's average time is probably
		// caused by exactly that pattern's sequence, and not a subsequence
		var deviationSum float64
		for i := 0; i < len(newResult.data.times); i++ {
			deviation := newResult.data.times[i] - newResult.avg
			deviationSum += float64(deviation * deviation)
		}
		newResult.stddev = math.Sqrt(deviationSum)

		rtn = append(rtn, newResult)
	}
	status.Process(1)

	return rtn
}

// stddev units are microseconds
func showTopTotalStats(patterns []patternData, minStdDev float64, maxStdDev float64,
	maxWildcards int) {

	minStdDevNano := minStdDev * 1000
	maxStdDevNano := maxStdDev * 1000

	funcResults := make([]patternData, 0)
	// Go through all the patterns and collect ones within stddev
	for i := 0; i < len(patterns); i++ {
		wildcards := 0
		for i := 0; i < len(patterns[i].wildcards); i++ {
			if patterns[i].wildcards[i] {
				wildcards++
			}
		}
		if wildcards > maxWildcards {
			continue
		}

		// time package's units are nanoseconds. So we need to convert our
		// microsecond stddev bounds to nanoseconds so we can compare
		if minStdDevNano <= patterns[i].stddev &&
			patterns[i].stddev <= maxStdDevNano {

			funcResults = append(funcResults, patterns[i])
		}
	}

	// Now sort by total time usage
	sort.Sort(SortResultsTotal(funcResults))

	// Filter out any duplicates (resulting from ineffectual wildcards)
	currentSeq := ""
	currentData := patternData{}
	filteredResults := make([]patternData, 0)
	for i := 0; i <= len(funcResults); i++ {
		var result patternData
		if i < len(funcResults) {
			result = funcResults[i]
		}

		seqStr := genSeqStr(result.data.seq)
		
		if seqStr != currentSeq {
			// We've finished going through a group of duplicates, so
			// add their most wildcarded member
			if i > 0 {
				filteredResults = append(filteredResults,
					currentData)
			}
			currentSeq = seqStr
			currentData = result
		} else {
			// We have a duplicate
			if countWildcards(result.wildcards) >
				countWildcards(currentData.wildcards) {

				currentData = result
			}
		}
	}

	fmt.Println("Top function patterns by total time used:")
	count := 0
	for i := len(filteredResults)-1; i >= 0; i-- {
		result := filteredResults[i]

		fmt.Printf("=================%2d===================\n", count+1)
		for j := 0; j < len(result.data.seq); j++ {
			if result.wildcards[j] {
				fmt.Println("***Wildcards***")
			} else {
				fmt.Printf("%s\n",
					strings.TrimSpace(result.data.seq[j].Format))
			}
		}
		fmt.Println("--------------------------------------")
		fmt.Printf("Total sequence time: %12s\n",
			time.Duration(result.sum).String())
		fmt.Printf("Average sequence time: %12s\n",
			time.Duration(result.avg).String())
		fmt.Printf("Standard Deviation: %12s\n",
			time.Duration(int64(result.stddev)).String())
		fmt.Println("")
		count++

		// Stop when we've output enough of the top
		if count >= 30 {
			break;
		}
	}

	//debug now
/*
	//debug
	for k, v := range sequences {
		fmt.Printf("%s\n", k)
		for i := 0; i < len(v.times); i++ {
			fmt.Printf("%d ", v.times[i])
		}
		fmt.Printf("\n\n")
	}*/
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

var patternStats []patternData
func menuProcess(command string, logs []qlog.LogOutput) {
	tokens := strings.Split(command, " ")

	if len(command) == 0 || tokens[0] == "help" {
		showHelp()
		return
	}

	switch tokens[0] {
	case "topTotal":
		if len(tokens) < 4 {
			fmt.Println("Error: log requires 3 parameters. See 'help'.")
			return
		}

		minStdDev, err := strconv.ParseFloat(tokens[1], 64)
		if err != nil {
			fmt.Printf("Error: '%s' is not a valid float\n",
				tokens[1])
			return
		}

		maxStdDev, err := strconv.ParseFloat(tokens[2], 64)
		if err != nil {
			fmt.Printf("Error: '%s' is not a valid float\n",
				tokens[2])
			return
		}

		maxWcs, err := strconv.ParseInt(tokens[3], 10, 32)
		if err != nil {
			fmt.Printf("Error: '%s' is not a valid int\n",
				tokens[3])
			return
		}

		if patternStats == nil {
			patternStats = getStatPatterns(logs)
		}

		showTopTotalStats(patternStats, minStdDev, maxStdDev, int(maxWcs))
	case "ids":
		showRequestIds(logs)
	case "log":
		if len(tokens) < 2 {
			fmt.Println("Error: log requires 1 parameter. See 'help'.")
			return
		}

		reqId, err := strconv.ParseUint(tokens[1], 10, 64)
		if err != nil {
			fmt.Printf("Error: '%s' is not a valid request id\n",
				tokens[1])
			return
		}
		showLogs(reqId, logs)
	case "exit":
		os.Exit(0)
	default:
		fmt.Printf("Error: Unrecognized command '%s'. See 'help'.\n",
			tokens[0])
	}
}
