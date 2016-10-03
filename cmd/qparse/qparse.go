// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import "bufio"
import "errors"
import "flag"
import "fmt"
import "os"
import "regexp"
import "sort"
import "strconv"
import "strings"

import "github.com/aristanetworks/quantumfs/qlog"

var tabSpaces *int
var file *string
var stats *bool

// To limit the computational cost of pattern matching, we set the max number of
// wildcards in a sequence to 30
const maxSeqWildcards = 30
var wildcardStr string

// -- worker structures

type sequenceData struct {
	times	[]int64
	seq	[]qlog.LogOutput
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
	reqIds := extractRequestIds(logs)

	rtn := make(map[string]sequenceData)

	fmt.Println("Extracing sequences from logs...")
	status := qlog.NewLogStatus(50)

	// Go through all the logs per request
	for i := 0; i < len(reqIds); i++ {
		status.Process(float32(i) / float32(len(reqIds)))

		abortRequest := false
		reqLogs := getReqLogs(reqIds[i], logs)

		// Skip it if its a special id since they're not self contained
		if reqIds[i] >= qlog.MinSpecialReqId {
			continue
		}

		// Iterate through the request's logs, constructing all subsequences
		trackers := make([]sequenceTracker, 0)
		for j := 0; j < len(reqLogs); j++ {

			// Start a new subsequence
			if qlog.IsFnIn(reqLogs[j].Format) {
				trackers = append(trackers, newSequenceTracker())
			}

			// Inform all the trackers of the new token
			for k := 0; k < len(trackers); k++ {
				err := trackers[k].Process(reqLogs[j])
				if err != nil {
					fmt.Println(err)
					abortRequest = true
					break
				}
			}

			if abortRequest {
				break
			}
		}

		if abortRequest {
			continue
		}

		// After going through the logs, add all our sequences to the rtn map
		for k := 0; k < len(trackers); k++ {
			// If the tracker isn't ready, that means there was a fnIn
			// that missed its fnOut. That's an error
			if trackers[k].ready == false {
				fmt.Printf("Error: Mismatched '%s' in requestId %d"+
					" log\n", qlog.FnEnterStr, reqIds[i])
				abortRequest = true
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

		if abortRequest {
			continue
		}
	}

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

	tabSpaces = flag.Int("tab", 0, "Indent function logs with n spaces")
	file = flag.String("f", "", "Log file to parse (required)")
	stats = flag.Bool("stats", false, "Enter interactive mode to read stats.")

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
		fmt.Println(qlog.ParseLogsExt(*file, *tabSpaces))
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
	logs := qlog.OutputLogsExt(pastEndIdx, dataArray, strMap, true)

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
	fmt.Println("overall          Show overall statistics")
	fmt.Println("ids              List all request ids in log")
	fmt.Println("log <id>         Show log sequence for request <id>")
	fmt.Println("exit             Exit and return to the shell")
	fmt.Println("")
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

// curMask is a map of indices into sequences which should be ignored when gathering
// data (wildcards). The base cases of this function are when its called with zero
// remaining in wildcardNum - that's how it knows to use curMask and collect the
// data
func recurseCalcTimes(curMask []bool, wildcardNum int, seq []qlog.LogOutput,
	sequences []sequenceData, out map[string]sequenceData /*out*/) {

	// If we've already collected times for this wildcard + sequence,
	// then we don't need to do anything since other wildcard combinations will
	// have already been covered
	seqStr := genSeqStrExt(seq, curMask)
	if _, exists := out[seqStr]; exists {
		return
	}

	if wildcardNum > 0 {
		// we need to place a new wildcard. Note: we don't allow the first /
		// last logs to be wildcarded since we *know* they're functions and
		// they define the function we're trying to analyze across variants
		for i := 1; i < len(sequences)-1; i++ {
			if !curMask[i] {
				// This spot doesn't have a wildcard yet.
				curMask[i] = true
				recurseCalcTimes(curMask, wildcardNum-1, seq,
					sequences, out)
				// Make sure to remove the entry for the next loop
				curMask[i] = false
			}
		}
	} else {
		out[seqStr] = collectData(curMask, seq, sequences)
	}
}

// Given a set of logs, collect deltas within and between function in/out pairs
func showOverallStats(logs []qlog.LogOutput) {
	sequenceMap := extractSequences(logs)
	sequences := make([]sequenceData, 0, len(sequenceMap))
	for _, v := range sequenceMap {
		sequences = append(sequences, v)
	}

	// Map of wildcard containing string to times and example logs
	rtn := make(map[string]sequenceData)

	status := qlog.NewLogStatus(50)

	// Now generate all combinations of the sequences with wildcards in them,
	// and collect stats for each generated sequence
	for i := 0; i < len(sequences); i++ {
		// Update the status bar
		status.Process(float32(i) / float32(len(sequences)))

		// If we've already got a result for this sequence, then this has
		// been counted and we can skip it.
		if _, exists := rtn[genSeqStr(sequences[i].seq)]; exists {
			continue
		}

		maxWildcards := len(sequences) - 2
		if maxWildcards > maxSeqWildcards {
			maxWildcards = maxSeqWildcards
		}

		for wildcards := 0; wildcards < maxWildcards; wildcards++ {
			wildcardMask := make([]bool, len(sequences[i].seq))
			recurseCalcTimes(wildcardMask, wildcards, sequences[i].seq,
				sequences, rtn /*out*/)
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

func menuProcess(command string, logs []qlog.LogOutput) {
	tokens := strings.Split(command, " ")

	if len(command) == 0 || tokens[0] == "help" {
		showHelp()
		return
	}

	switch tokens[0] {
	case "overall":
		showOverallStats(logs)
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
