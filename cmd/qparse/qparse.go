// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import "flag"
import "fmt"
import "io/ioutil"
import "os"
import "sort"
import "strconv"
import "strings"
import "time"

import "github.com/aristanetworks/quantumfs/qlog"

var version string

type intSlice []uint64

func (i *intSlice) String() string {
	return fmt.Sprintf("%d", *i)
}

func (i *intSlice) Set(value string) error {
	token, err := strconv.ParseInt(value, 16, 64)
	if err != nil {
		fmt.Printf("Error: %s is not a valid 8 byte hex id\n", value)
		os.Exit(1)
	} else {
		*i = append(*i, uint64(token))
	}

	return nil
}

var inFile string
var outFile string
var tabSpaces int
var logOut bool
var patternsOut bool
var stats bool
var patternFile string
var sizeStats bool
var topTotal int
var topAvg int
var minTimeslicePct int
var filterId intSlice
var bucketWidthMs int
var bucketWidthNs int64
var showClose bool
var stdDevMin float64
var stdDevMax float64
var wildMin int
var wildMax int
var sampleMin int
var maxThreads int
var maxLenWildcards int
var maxLen int

var wildcardStr string

// -- worker structures
type SortResultsAverage []qlog.PatternData

func (s SortResultsAverage) Len() int {
	return len(s)
}

func (s SortResultsAverage) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortResultsAverage) Less(i, j int) bool {
	if s[i].Avg == s[j].Avg {
		return (s[i].SeqStrRaw > s[j].SeqStrRaw)
	} else {
		return (s[i].Avg > s[j].Avg)
	}
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
	flag.StringVar(&inFile, "in", "", "Specify an input file")
	flag.StringVar(&outFile, "out", "", "Specify an output file")
	flag.IntVar(&tabSpaces, "tab", 0,
		"Indent function logs with n spaces, when using -log")
	flag.BoolVar(&logOut, "log", false,
		"Parse a log file (-in) and print to stdout or a file with -out")
	flag.BoolVar(&patternsOut, "pattern", false,
		"Print patterns given in a stat file. Works with -id.")
	flag.BoolVar(&stats, "stat", false, "Parse a log file (-in) and output to "+
		"a stats file (-out). Default stats filename is logfile.stats")
	flag.StringVar(&patternFile, "logpatt", "", "Filter logs by requests which "+
		"match the pattern given in the file <logpatt>. Try -byTotal or "+
		"-byAvg to get patterns.")
	flag.BoolVar(&sizeStats, "sizes", false, "Parse a log file and output "+
		"a histogram of log packet sizes. Works with -out.")
	flag.IntVar(&topTotal, "byTotal", 0, "Parse a stat file (-in) and "+
		"print top <byTotal> functions by total time usage in logs")
	flag.IntVar(&topAvg, "byAvg", 0, "Parse a stat file (-in) and "+
		"print top <byAvg> functions by total time usage in logs")
	flag.IntVar(&minTimeslicePct, "minTimeslicePct", -1, "Output csv wall time "+
		"consumed in bucket t per SequenceId. To be output needs "+
		"<minTimeslicePct>/100 in any bucket or -id")
	flag.Var(&filterId, "id", "Filter certain output to include only given "+
		"8 byte Sequence Id Hash. Multiple -id flags are supported.")
	flag.IntVar(&bucketWidthMs, "bucketMs", 1000, "Bucket width for -csv in Ms")
	flag.BoolVar(&showClose, "similars", false,
		"Don't hide similar sequences when using -byTotal or -byAvg")
	flag.Float64Var(&stdDevMin, "stdDevMin", 0, "Filter results, requiring "+
		"a standard deviation of at least <stdDevMin>. Float units of "+
		"microseconds")
	flag.Float64Var(&stdDevMax, "stdDevMax", 1000000000,
		"Like stdDevMin, but setting a maximum")
	flag.IntVar(&wildMin, "wcMin", 0, "Filter results, requiring minimum "+
		"number of wildcards in function pattern.")
	flag.IntVar(&wildMax, "wcMax", 100, "Same as wmin, but setting a maximum")
	flag.IntVar(&sampleMin, "smMin", 1, "Filter results, requiring minimum "+
		"number of samples in function pattern.")
	flag.IntVar(&maxThreads, "threads", 30, "Max threads to use")
	flag.IntVar(&maxLenWildcards, "maxWc", 16,
		"Max sequence length to wildcard during -stat")
	flag.IntVar(&maxLen, "maxLen", 10000,
		"Max sequence length to return in results")

	flag.Usage = func() {
		fmt.Printf("Usage: %s -in <filepath> [flags]\n\n", os.Args[0])
		fmt.Println("Note: If the oom killer is killing qparse, export " +
			"GOGC=80 should help.")
		fmt.Println("Lower values help more, but make qparse slower.\n")

		fmt.Println("Flags:")
		flag.PrintDefaults()
	}

	wildcardStr = "***Wildcard***"
}

func printIndexedLog(idx int, sequence []qlog.LogOutput, wildcards []bool) {

	printIndexedLogExt(idx, sequence, wildcards, false)
}

func printIndexedLogExt(idx int, sequence []qlog.LogOutput, wildcards []bool,
	collapseWildcards bool) {

	fmt.Printf("=====%2d=====PATTERN SECTION BELOW=====\n", idx)
	outputWildcard := false
	for j := 0; j < len(sequence); j++ {
		if j < len(wildcards) && wildcards[j] {
			// Don't show consecutive wildcards
			if !outputWildcard {
				fmt.Println(wildcardStr)
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
	bucketWidthNs = 1000000 * int64(bucketWidthMs)

	if len(os.Args) == 1 {
		fmt.Println("qparse version", version)

		flag.Usage()
		return
	}

	switch {
	case minTimeslicePct != -1:
		if inFile == "" {
			fmt.Println("To -cover, you must specify a stat file " +
				"with -in")
			os.Exit(1)
		}
		if outFile == "" {
			fmt.Println("To -cover, you must specify an output filename")
			os.Exit(1)
		}
		if minTimeslicePct < 0 || minTimeslicePct > 100 {
			fmt.Println("To -cover, you must specify a threshold " +
				"[0, 100]")
			os.Exit(1)
		}

		fmt.Println("Loading file for -cover...")
		file, err := os.Open(inFile)
		if err != nil {
			fmt.Printf("Unable to open stat file %s: %s\n", inFile, err)
			os.Exit(1)
		}
		defer file.Close()
		patterns := qlog.LoadFromStat(file)

		outputCsvCover(patterns)
	case logOut:
		if inFile == "" {
			fmt.Println("To -log, you must specify a log file with -in")
			os.Exit(1)
		}

		if patternFile != "" {
			filterLogOut(inFile, patternFile, true, tabSpaces)
		} else if outFile == "" {
			// Log parse mode only
			qlog.ParseLogsExt(inFile, tabSpaces,
				maxThreads, false, fmt.Printf)
		} else {
			outFh, err := os.Create(outFile)
			if err != nil {
				fmt.Printf("Unable to create output file: %s\n", err)
				os.Exit(1)
			}
			defer outFh.Close()

			qlog.ParseLogsExt(inFile, tabSpaces, maxThreads,
				true, func(format string, args ...interface{}) (int,
					error) {

					return fmt.Fprintf(outFh, format, args...)
				})
		}
	case patternsOut:
		if inFile == "" {
			fmt.Println("To -patt, you must specify a stat " +
				"file with -in")
			os.Exit(1)
		}

		fmt.Println("Loading file...")

		file, err := os.Open(inFile)
		if err != nil {
			fmt.Printf("Unable to open stat file %s: %s\n", inFile, err)
			os.Exit(1)
		}
		defer file.Close()
		patterns := qlog.LoadFromStat(file)

		filterMap := make(map[uint64]bool)
		for i := 0; i < len(filterId); i++ {
			filterMap[filterId[i]] = true
		}

		printAll := (len(filterId) == 0)
		count := 0
		for i := 0; i < len(patterns); i++ {
			if !printAll {
				if _, exists := filterMap[patterns[i].Id]; !exists {
					continue
				}
			}

			printIndexedLogExt(i, patterns[i].Data.Seq,
				patterns[i].Wildcards, true)
			printPatternData(patterns[i])
			count++
		}
	case stats:
		if inFile == "" {
			fmt.Println("To -stat, you must specify a log file with -in")
			os.Exit(1)
		}
		outFilename := inFile + ".stats"
		if outFile != "" {
			outFilename = outFile
		}

		patterns := qlog.GetStatPatterns(inFile, maxThreads, maxLenWildcards)

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
	case sizeStats:
		if inFile == "" {
			fmt.Println("To -sizes you must specify a log file with -in")
			os.Exit(1)
		}

		if outFile == "" {
			// Print to stdout, no status bar
			qlog.PacketStats(inFile, false, fmt.Printf)
		} else {
			outFh, err := os.Create(outFile)
			if err != nil {
				fmt.Printf("Unable to create output file: %s\n", err)
				os.Exit(1)
			}
			defer outFh.Close()

			qlog.PacketStats(inFile, true, func(format string,
				args ...interface{}) (int, error) {

				toWrite := fmt.Sprintf(format, args...)
				return outFh.WriteString(toWrite)
			})
		}
	case topTotal != 0:
		if inFile == "" {
			fmt.Println("To -topTotal, you must specify a stat file " +
				"with -in")
			os.Exit(1)
		}

		fmt.Println("Loading file for -byTotal...")
		file, err := os.Open(inFile)
		if err != nil {
			fmt.Printf("Unable to open stat file %s: %s\n", inFile, err)
			os.Exit(1)
		}
		defer file.Close()
		patterns := qlog.LoadFromStat(file)

		// Now sort by total time usage
		fmt.Println("Sorting data by total time usage...")
		sort.Sort(SortResultsTotal(patterns))

		fmt.Println("Top function patterns by total time used:")
		showStats(patterns, stdDevMin, stdDevMax, wildMin,
			wildMax, sampleMin, maxLen, topTotal)
	case topAvg != 0:
		if inFile == "" {
			fmt.Println("To -topAvg, you must specify a stat file " +
				"with -in")
			os.Exit(1)
		}

		fmt.Println("Loading file for -byAvg...")
		file, err := os.Open(inFile)
		if err != nil {
			fmt.Printf("Unable to open stat file %s: %s\n", inFile, err)
			os.Exit(1)
		}
		defer file.Close()
		patterns := qlog.LoadFromStat(file)

		// Now sort by average time usage
		fmt.Println("Sorting data by average time usage...")
		sort.Sort(SortResultsAverage(patterns))

		fmt.Println("Top function patterns by average time used:")
		showStats(patterns, stdDevMin, stdDevMax, wildMin,
			wildMax, sampleMin, maxLen, topAvg)
	default:
		fmt.Println("No action flags (-log, -stat, -csv) specified.")
		os.Exit(1)
	}
}

type bucket struct {
	timeSums map[uint64]float64
	totalSum float64
}

func fillTimeline(out map[int64]bucket, seqId uint64,
	pattern qlog.PatternData) (minStartTime int64) {

	times := pattern.Data.Times
	var minTime int64

	for k := 0; k < len(times); k++ {
		if k == 0 || times[k].StartTime < minTime {
			minTime = times[k].StartTime
		}

		bucketIdx := times[k].StartTime / bucketWidthNs
		endBucketIdx := (times[k].StartTime + times[k].Delta) / bucketWidthNs

		for n := bucketIdx; n <= endBucketIdx; n++ {
			bucketIt := out[n]
			if bucketIt.timeSums == nil {
				bucketIt.timeSums = make(map[uint64]float64)
			}

			timeDeltaStart := n * bucketWidthNs
			if n == bucketIdx {
				timeDeltaStart = times[k].StartTime
			}
			timeDeltaEnd := (n + 1) * bucketWidthNs
			if n == endBucketIdx {
				timeDeltaEnd = times[k].StartTime + times[k].Delta
			}

			newDelta := (float64(timeDeltaEnd) -
				float64(timeDeltaStart)) / float64(bucketWidthNs)
			bucketIt.timeSums[seqId] += newDelta
			bucketIt.totalSum += newDelta

			out[n] = bucketIt
		}
	}

	return minTime
}

func filterLogOut(inFile string, patternFile string, showStatus bool,
	tabSpaces int) {

	pattern := make([]qlog.LogOutput, 0)
	wildcards := make([]bool, 0)
	patternData, err := ioutil.ReadFile(patternFile)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Showing logs from requests matching:\n%s\n\n", patternData)

	patternStrings := strings.Split(string(patternData), "\n")
	for i := 0; i < len(patternStrings); i++ {
		// ignore empty lines
		if len(patternStrings[i]) == 0 {
			continue
		}

		pattern = append(pattern, qlog.LogOutput{
			Format: patternStrings[i] + "\n",
		})

		if patternStrings[i] == wildcardStr {
			wildcards = append(wildcards, true)
		} else {
			wildcards = append(wildcards, false)
		}
	}

	trackerCount, trackerMap := qlog.GetTrackerMap(inFile, maxThreads)

	// now we just need to output the contents of the tracker maps we match
	trackerIdx := 0
	fmt.Println("Filtering for relevant subsequences...")
	status := qlog.NewLogStatus(50)
	sequences := make([]*qlog.SequenceTracker, 0)
	for reqId, trackers := range trackerMap {
		if len(trackers) == 0 {
			continue
		}

		for k := 0; k < len(trackers); k++ {
			trackerIdx++
			if showStatus {
				status.Process(float32(trackerIdx) /
					float32(trackerCount))
			}
			if trackers[k].Ready() == false {
				fmt.Printf("Error: Mismatched '%s' in requestId %d"+
					" log\n", qlog.FnEnterStr, reqId)
				break
			}

			if qlog.PatternMatches(pattern, wildcards,
				trackers[k].Seq()) {

				sequences = append(sequences, &trackers[k])
			}
		}
	}
	if showStatus {
		status.Process(1)
	}

	if len(sequences) == 0 {
		fmt.Println("No log pattern match for", patternFile)
		os.Exit(0)
	}

	for _, i := range sequences {
		qlog.FormatLogs(i.Seq(), tabSpaces, false, fmt.Printf)
	}
}

func outputCsvCover(patterns []qlog.PatternData) {
	file, err := os.Create(outFile)
	if err != nil {
		fmt.Printf("Unable to create %s for new data: %s\n", outFile, err)
		os.Exit(1)
	}
	defer file.Close()

	timeline := make(map[int64]bucket)
	var minTime int64
	minSet := false
	fmt.Println("Filling timeline buckets...")
	status := qlog.NewLogStatus(50)
	if len(filterId) != 0 {
		for i := 0; i < len(patterns); i++ {
			status.Process(float32(i) / float32(len(patterns)))
			for j := 0; j < len(filterId); j++ {
				if patterns[i].Id == filterId[j] {
					minStart := fillTimeline(timeline,
						filterId[j], patterns[i])

					if !minSet || minStart < minTime {
						minTime = minStart
						minSet = true
					}
					break
				}
			}
		}
	} else {
		for i := 0; i < len(patterns); i++ {
			status.Process(float32(i) / float32(len(patterns)))
			minStart := fillTimeline(timeline, patterns[i].Id,
				patterns[i])

			if !minSet || minStart < minTime {
				minTime = minStart
				minSet = true
			}
		}
	}
	status.Process(1)

	bucketThreshold := float64(minTimeslicePct) / float64(100)
	outputIndices := make([]uint64, 0)
	if len(filterId) != 0 {
		for i := 0; i < len(filterId); i++ {
			outputIndices = append(outputIndices, filterId[i])
		}
	} else {
		status = qlog.NewLogStatus(50)
		fmt.Printf("Determining which of %d buckets to output...\n",
			len(timeline))

		outputIdxMap := make(map[uint64]bool)
		bucketKey := minTime / bucketWidthNs
		for outCount := 0; outCount < len(timeline); bucketKey++ {
			status.Process(float32(outCount) / float32(len(timeline)))
			mapBucket, exists := timeline[bucketKey]

			if !exists {
				continue
			}

			// Add any sequence ids that consume enough of the bucket
			for k, v := range mapBucket.timeSums {
				_, exists := outputIdxMap[k]
				if !exists && (v/mapBucket.totalSum) >
					bucketThreshold {

					outputIdxMap[k] = true
				}
			}
			outCount++
		}
		status.Process(1)

		for k, _ := range outputIdxMap {
			outputIndices = append(outputIndices, k)
		}
	}

	file.WriteString(fmt.Sprintf("t,"))
	for i := 0; i < len(outputIndices); i++ {
		file.WriteString(fmt.Sprintf("%d,", outputIndices[i]))
	}
	file.WriteString("\n")

	bucketKey := minTime / bucketWidthNs
	row := int64(0)
	status = qlog.NewLogStatus(50)
	fmt.Printf("Outputting %d patterns into %d buckets...\n",
		len(outputIndices), len(timeline))
	for outCount := 0; outCount < len(timeline); bucketKey++ {
		status.Process(float32(outCount) / float32(len(timeline)))
		mapBucket, exists := timeline[bucketKey]

		unixTime := minTime + (row * bucketWidthNs)
		file.WriteString(fmt.Sprintf("%s,", time.Unix(0,
			unixTime).Format("15:04:05.0000000000")))
		for i := 0; i < len(outputIndices); i++ {
			if !exists {
				file.WriteString("0,")
				continue
			}

			seqVal, _ := mapBucket.timeSums[outputIndices[i]]
			file.WriteString(fmt.Sprintf("%f,", seqVal))
		}

		if exists {
			outCount++
		}

		file.WriteString("\n")
		row++
	}
	status.Process(1)
}

func filterPatterns(patterns []qlog.PatternData, minStdDev float64,
	maxStdDev float64, minWildcards int, maxWildcards int, minSamples int,
	maxLen int, maxResults int) (filtered []qlog.PatternData, firstLog int64,
	lastLog int64) {

	minStdDevNano := int64(minStdDev * 1000)
	maxStdDevNano := int64(maxStdDev * 1000)

	earliestLog := patterns[0].Data.Times[0].StartTime
	latestLog := earliestLog

	var lastTimes []qlog.TimeData
	funcResults := make([]qlog.PatternData, 0)
	for i := 0; i < len(patterns); i++ {
		// check the times for earliest / latest time
		for j := 0; j < len(patterns[i].Data.Times); j++ {
			t := patterns[i].Data.Times[j]
			if t.StartTime < earliestLog {
				earliestLog = t.StartTime
			}

			if t.StartTime+t.Delta > latestLog {
				latestLog = t.StartTime + t.Delta
			}
		}

		if len(patterns[i].Data.Times) < minSamples {
			continue
		}

		wildcards := qlog.CountWildcards(patterns[i].Wildcards, false)
		if wildcards > maxWildcards || wildcards < minWildcards {
			continue
		}

		if len(patterns[i].Data.Seq)-
			(qlog.CountWildcards(patterns[i].Wildcards, true)-
				wildcards) > maxLen {

			continue
		}

		if !(showClose) {
			// If this dataset is a subset of the last, then we've
			// already output the most wildcarded version of this
			// sequence so let's not print redundant information
			if qlog.Superset(lastTimes, patterns[i].Data.Times) {
				continue
			}
		}

		// time package's units are nanoseconds. So we need to convert our
		// microsecond stddev bounds to nanoseconds so we can compare
		if minStdDevNano <= patterns[i].Stddev &&
			patterns[i].Stddev <= maxStdDevNano {

			funcResults = append(funcResults, patterns[i])
			lastTimes = patterns[i].Data.Times
		}

		if len(funcResults) >= maxResults {
			break
		}
	}

	return funcResults, earliestLog, latestLog
}

func printPatternDataTotal(pattern qlog.PatternData, firstLog int64, lastLog int64) {
	logTime := lastLog - firstLog
	logPct := float64(pattern.Sum) / float64(logTime)

	fmt.Println("------------PATTERN SECTION ABOVE-----")
	fmt.Printf("Total sequence time: %.12s (%.4f%% of %.10s total in logs)\n",
		time.Duration(pattern.Sum).String(), 100*logPct,
		time.Duration(logTime).String())

	printPatternCommon(pattern)
	fmt.Println("")
}

func printPatternData(pattern qlog.PatternData) {
	fmt.Println("------------PATTERN SECTION ABOVE-----")
	fmt.Printf("Total sequence time: %12s\n",
		time.Duration(pattern.Sum).String())

	printPatternCommon(pattern)
	fmt.Println("")
}

func printPatternCommon(pattern qlog.PatternData) {
	fmt.Printf("Average sequence time: %12s\n",
		time.Duration(pattern.Avg).String())
	fmt.Printf("Number of samples: %d\n", len(pattern.Data.Times))
	fmt.Printf("Sequence Id: %016x\n", pattern.Id)
	fmt.Printf("Standard Deviation: %12s\n",
		time.Duration(pattern.Stddev).String())
}

// stddev units are microseconds
func showStats(patterns []qlog.PatternData, minStdDev float64,
	maxStdDev float64, minWildcards int, maxWildcards int, minSamples int,
	maxLen int, maxResults int) {

	funcResults, firstLog, lastLog := filterPatterns(patterns, minStdDev,
		maxStdDev, minWildcards, maxWildcards, minSamples, maxLen,
		maxResults)

	count := 0
	for i := 0; i < len(funcResults); i++ {
		result := funcResults[i]

		printIndexedLogExt(count+1, result.Data.Seq, result.Wildcards, true)
		printPatternDataTotal(result, firstLog, lastLog)
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
		fmt.Printf("%"+padLen+"d ", keys[i])

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

	qlog.FormatLogs(filteredLogs, tabSpaces, false, fmt.Printf)
}
