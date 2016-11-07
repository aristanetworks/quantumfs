// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package qlog

import "bytes"
import "encoding/binary"
import "encoding/gob"
import "errors"
import "fmt"
import "io"
import "io/ioutil"
import "math"
import "os"
import "reflect"
import "sort"
import "strings"
import "sync"
import "time"
import "unsafe"

// consts
const defaultParseThreads = 30
const defaultChunkSize = 4

type LogOutput struct {
	Subsystem LogSubsystem
	ReqId     uint64
	T         int64
	Format    string
	Args      []interface{}
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
func PatternMatches(pattern []LogOutput, wildcards []bool, data []LogOutput) bool {
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

func newLog(s LogSubsystem, r uint64, t int64, f string,
	args []interface{}) LogOutput {

	return LogOutput{
		Subsystem: s,
		ReqId:     r,
		T:         t,
		Format:    f,
		Args:      args,
	}
}

type SortString []string

func (s SortString) Len() int {
	return len(s)
}

func (s SortString) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortString) Less(i, j int) bool {
	return s[i] < s[j]
}

type SortByTime []LogOutput

func (s SortByTime) Len() int {
	return len(s)
}

func (s SortByTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SortByTime) Less(i, j int) bool {
	return s[i].T < s[j].T
}

type TimeData struct {
	// how long the sequence took
	Delta int64
	// when it started in Unix time
	StartTime int64
	// where it started in the logs
	LogIdxLoc int
}

type SequenceData struct {
	Times []TimeData
	Seq   []LogOutput
}

type PatternData struct {
	SeqStrRaw string // No wildcards in this string, just seq
	Data      SequenceData
	Wildcards []bool
	Avg       int64
	Sum       int64
	Stddev    int64
	Id        int
}

type logStack []LogOutput

func newLogStack() logStack {
	return make([]LogOutput, 0)
}

func (s *logStack) Push(n LogOutput) {
	*s = append(*s, n)
}

func (s *logStack) Pop() {
	if len(*s) > 0 {
		*s = (*s)[:len(*s)-1]
	}
}

func (s *logStack) Peek() (LogOutput, error) {
	if len(*s) == 0 {
		return LogOutput{},
			errors.New("Cannot peek on an empty logStack")
	}

	return (*s)[len(*s)-1], nil
}

type sequenceTracker struct {
	stack logStack

	ready       bool
	seq         []LogOutput
	startLogIdx int
}

func newSequenceTracker(startIdx int) sequenceTracker {
	return sequenceTracker{
		stack:       newLogStack(),
		ready:       false,
		seq:         make([]LogOutput, 0),
		startLogIdx: startIdx,
	}
}

func (s *sequenceTracker) Process(log LogOutput) error {
	// Nothing more to do
	if s.ready {
		return nil
	}

	top, err := s.stack.Peek()
	if len(s.stack) == 1 && IsLogFnPair(top.Format, log.Format) {
		// We've found our pair, and have our sequence. Finalize
		s.ready = true
	} else if IsFunctionIn(log.Format) {
		s.stack.Push(log)
	} else if IsFunctionOut(log.Format) {
		if err != nil || !IsLogFnPair(top.Format, log.Format) {
			return errors.New(fmt.Sprintf("Error: Mismatched '%s' in "+
				"requestId %d log\n",
				FnExitStr, log.ReqId))
		}
		s.stack.Pop()
	}

	// Add to the sequence we're tracking
	s.seq = append(s.seq, log)
	return nil
}

func genSeqStr(seq []LogOutput) string {
	return genSeqStrExt(seq, []bool{}, false)
}

// consecutiveWc specifies whether the output should contain more than one wildcard
// in sequence when they are back to back in wildcardMsdk
func genSeqStrExt(seq []LogOutput, wildcardMask []bool,
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

// Because Golang is a horrible language and doesn't support maps with slice keys,
// we need to construct long string keys and save the slices in the value for later
func ExtractSequences(logs []LogOutput) map[string]SequenceData {
	trackerMap := make(map[uint64][]sequenceTracker)
	trackerCount := 0

	fmt.Printf("Extracting sub-sequences from %d logs...\n", len(logs))
	status := NewLogStatus(50)

	// Go through all the logs in one pass, constructing all subsequences
	for i := 0; i < len(logs); i++ {
		status.Process(float32(i) / float32(len(logs)))

		reqId := logs[i].ReqId

		// Skip it if its a special id since they're not self contained
		if reqId >= MinSpecialReqId {
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
		if IsFunctionIn(logs[i].Format) {
			trackers = append(trackers, newSequenceTracker(i))
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
	status = NewLogStatus(50)

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
					" log\n", FnEnterStr, reqId)
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
				TimeData{
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

func SaveToStat(file *os.File, patterns []PatternData) {
	encoder := gob.NewEncoder(file)

	// Encode the length so we can have a progress bar on load
	patternLen := int(len(patterns))
	encoder.Encode(patternLen)

	// The gob package has an annoying and poorly thought out const cap on
	// Encode data max length. So, we have to encode in chunks we a new encoder
	// each time
	status := NewLogStatus(50)
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

func LoadFromStat(file *os.File) []PatternData {
	decoder := gob.NewDecoder(file)
	var rtn []PatternData

	var patternLen int
	decoder.Decode(&patternLen)
	totalDecoded := 0

	status := NewLogStatus(50)
	// We have to encode in chunks, so keep going until we're out of data
	for {
		var chunk []PatternData
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

// Returns true if the log file string map given failed the logscan
func LogscanSkim(filepath string) bool {
	strMapData := extractStrMapData(filepath)

	// This takes too much time, so only count one string as failing
	if bytes.Contains(strMapData, []byte("ERROR")) {
		return true
	}

	return false
}

func ParseLogs(filepath string) string {
	return ParseLogsExt(filepath, 0, defaultParseThreads)
}

func ParseLogsExt(filepath string, tabSpaces int, maxThreads int) string {

	pastEndIdx, dataArray, strMap := ExtractFields(filepath)

	logs := OutputLogs(pastEndIdx, dataArray, strMap, maxThreads)

	return FormatLogs(logs, tabSpaces)
}

func FormatLogs(logs []LogOutput, tabSpaces int) string {
	indentMap := make(map[uint64]int)
	var rtn string
	// Now that we have the logs in correct order, we can indent them
	for i := 0; i < len(logs); i++ {
		// Add any indents necessary
		if tabSpaces > 0 {
			indents := indentMap[logs[i].ReqId]

			if strings.Index(logs[i].Format, FnExitStr) == 0 {
				indents--
			}

			// Add the spaces we need
			spaceStr := ""
			for i := 0; i < indents*tabSpaces; i++ {
				spaceStr += " "
			}

			if strings.Index(logs[i].Format, FnEnterStr) == 0 {
				indents++
			}

			logs[i].Format = spaceStr + logs[i].Format
			indentMap[logs[i].ReqId] = indents
		}

		// Convert timestamp back into something useful
		t := time.Unix(0, logs[i].T)

		rtn += formatString(logs[i].Subsystem, logs[i].ReqId, t,
			fmt.Sprintf(logs[i].Format, logs[i].Args...))
	}
	return rtn
}

func ExtractFields(filepath string) (pastEndIdx uint32, dataArray []byte,
	strMapRtn []LogStr) {

	data := grabMemory(filepath)
	header := (*MmapHeader)(unsafe.Pointer(&data[0]))

	if header.Version != QlogVersion {
		panic(fmt.Sprintf("Qlog version incompatible: got %d, need %d\n",
			header.Version, QlogVersion))
	}

	mmapHeaderSize := uint32(unsafe.Sizeof(MmapHeader{}))

	if uint32(len(data)) != (header.StrMapSize + header.CircBuf.Size +
		mmapHeaderSize) {
		fmt.Println("Data length inconsistent with expectations. %d",
			len(data))
	}

	// create a safer map to use
	strMapData := data[mmapHeaderSize+header.CircBuf.Size:]
	strMap := make([]LogStr, len(strMapData)/LogStrSize)
	idx := 0
	for i := 0; i+LogStrSize <= len(strMapData); i += LogStrSize {
		mapEntry := (*LogStr)(unsafe.Pointer(&strMapData[i]))
		strMap[idx] = *mapEntry

		idx++
	}

	return header.CircBuf.PastEndIdx,
		data[mmapHeaderSize : mmapHeaderSize+header.CircBuf.Size], strMap
}

func readOrPanic(offset int64, len int64, fd *os.File) []byte {
	raw := make([]byte, len)
	readCount := int64(0)
	for readCount < len {
		sz, err := fd.ReadAt(raw[readCount:], offset+readCount)
		if err != nil {
			panic("Unable to read log file data")
		}

		readCount += int64(sz)
	}

	return raw
}

func extractStrMapData(filepath string) []byte {
	// Extract just the strMapData, and avoid having to unecessarily read
	// the rest of the potentially giant log file
	file, err := os.Open(filepath)
	if err != nil {
		panic("Unable to open log file for strMapData read")
	}
	defer file.Close()

	headerLen := int64(unsafe.Sizeof(MmapHeader{}))
	headerRaw := readOrPanic(0, headerLen, file)
	header := (*MmapHeader)(unsafe.Pointer(&headerRaw[0]))

	return readOrPanic(headerLen+int64(header.CircBuf.Size),
		int64(header.StrMapSize), file)
}

func grabMemory(filepath string) []byte {
	if filepath == "" {
		return nil
	}

	fileData, err := ioutil.ReadFile(filepath)
	if err != nil {
		panic(fmt.Sprintf("Unable to read shared memory file data from %s\n",
			filepath))
	}

	return fileData
}

func parseArg(idx *uint32, data []byte) (interface{}, error) {
	var byteType uint16
	err := readPacket(idx, data, reflect.ValueOf(&byteType))
	if err != nil {
		return nil, err
	}

	handledWrite := true
	var rtn reflect.Value
	switch byteType {
	case TypeInt8Pointer:
		var tmp *int8
		rtn = reflect.ValueOf(&tmp)
	case TypeInt8:
		var tmp int8
		rtn = reflect.ValueOf(&tmp)
	case TypeUint8Pointer:
		var tmp *uint8
		rtn = reflect.ValueOf(&tmp)
	case TypeUint8:
		var tmp uint8
		rtn = reflect.ValueOf(&tmp)
	case TypeInt16Pointer:
		var tmp *int16
		rtn = reflect.ValueOf(&tmp)
	case TypeInt16:
		var tmp int16
		rtn = reflect.ValueOf(&tmp)
	case TypeUint16Pointer:
		var tmp *uint16
		rtn = reflect.ValueOf(&tmp)
	case TypeUint16:
		var tmp uint16
		rtn = reflect.ValueOf(&tmp)
	case TypeInt32Pointer:
		var tmp *int32
		rtn = reflect.ValueOf(&tmp)
	case TypeInt32:
		var tmp int32
		rtn = reflect.ValueOf(&tmp)
	case TypeUint32Pointer:
		var tmp *uint32
		rtn = reflect.ValueOf(&tmp)
	case TypeUint32:
		var tmp uint32
		rtn = reflect.ValueOf(&tmp)
	case TypeInt64Pointer:
		var tmp *int64
		rtn = reflect.ValueOf(&tmp)
	case TypeInt64:
		var tmp int64
		rtn = reflect.ValueOf(&tmp)
	case TypeUint64Pointer:
		var tmp *uint64
		rtn = reflect.ValueOf(&tmp)
	case TypeUint64:
		var tmp uint64
		rtn = reflect.ValueOf(&tmp)
	default:
		handledWrite = false
	}

	if handledWrite {
		err = readPacket(idx, data, rtn)
		if err != nil {
			return nil, err
		}
		return rtn.Elem().Interface(), nil
	}

	if byteType == TypeString || byteType == TypeByteArray {
		var strLen uint16
		err = readPacket(idx, data, reflect.ValueOf(&strLen))
		if err != nil {
			return nil, err
		}

		var rtnRaw [math.MaxUint16]byte
		err = readPacket(idx, data[:*idx+uint32(strLen)],
			reflect.ValueOf(&rtnRaw))
		if err != nil {
			return nil, err
		}

		if byteType == TypeString {
			return string(rtnRaw[:strLen]), nil
		}

		// Otherwise, return []byte if type is TypeByteArray
		return rtnRaw[:strLen], nil
	}

	return nil, errors.New(fmt.Sprintf("Unsupported field type %d\n", byteType))
}

func wrapRead(idx uint32, num uint32, data []byte) []byte {
	rtn := make([]byte, num)

	if idx+num > uint32(len(data)) {
		secondNum := (idx + num) - uint32(len(data))
		num -= secondNum
		copy(rtn[num:], data[0:secondNum])
	}

	copy(rtn, data[idx:idx+num])

	return rtn
}

func wrapMinusEquals(lhs *uint32, rhs uint32, bufLen int) {
	if *lhs < rhs {
		*lhs += uint32(bufLen)
	}

	*lhs -= rhs
}

// outputType is an instance of that same type as output, output *must* be a pointer
// to a variable of that type for the data to be placed into.
// PastIdx is the index of the element just *past* what we want to read
func readBack(pastIdx *uint32, data []byte, outputType interface{},
	output interface{}) {

	dataLen := uint32(reflect.TypeOf(outputType).Size())

	wrapMinusEquals(pastIdx, dataLen, len(data))
	rawData := wrapRead(*pastIdx, dataLen, data)

	buf := bytes.NewReader(rawData)
	err := binary.Read(buf, binary.LittleEndian, output)
	if err != nil {
		panic("Unable to binary read from data")
	}
}

// Returns the number of bytes read
func readPacket(idx *uint32, data []byte, output reflect.Value) error {

	dataLen := uint32(output.Elem().Type().Size())

	// If this is a string, then consume the rest of data provided
	if byteArray, ok := output.Interface().(*[math.MaxUint16]byte); ok {
		dataLen = uint32(len(data)) - *idx
		// Because binary.Read is dumb and can't read less than the given
		// array without EOFing *and* needs a fixed array, we have to do this
		var singleArray [1]byte
		for i := uint32(0); i < dataLen; i++ {
			buf := bytes.NewReader(data[*idx+i : *idx+i+1])
			err := binary.Read(buf, binary.LittleEndian, &singleArray)
			if err != nil {
				return err
			}
			(*byteArray)[i] = singleArray[0]
		}

		*idx += dataLen

		return nil
	}

	if dataLen+*idx > uint32(len(data)) {
		return errors.New(fmt.Sprintf("Packet has been clipped. (%d %d %d)",
			dataLen, *idx, len(data)))
	}

	buf := bytes.NewReader(data[*idx : *idx+dataLen])
	err := binary.Read(buf, binary.LittleEndian, output.Interface())
	if err != nil {
		return err
	}

	*idx += dataLen

	return nil
}

type LogStatus struct {
	shownHeader  bool
	pixWidth     int
	lastPixShown int
}

func NewLogStatus(displayWidth int) LogStatus {
	return LogStatus{
		shownHeader:  false,
		pixWidth:     displayWidth,
		lastPixShown: 0,
	}
}

func (l *LogStatus) Process(newPct float32) {
	if !l.shownHeader {
		leftHeader := "Processing: ||"
		nextHeader := "             |"
		fmt.Printf(leftHeader)
		for i := 0; i < l.pixWidth; i++ {
			fmt.Printf(" ")
		}
		fmt.Printf("||\n")
		fmt.Printf(nextHeader)
		l.shownHeader = true
	}

	// Calculate the amount of pixels to output in the loading bar
	pixDone := int(float32(l.pixWidth) * newPct)
	for i := l.lastPixShown + 1; i <= pixDone; i++ {
		// Each pixel in the bar is a period
		fmt.Printf(".")
	}

	if pixDone == l.pixWidth && pixDone != l.lastPixShown {
		fmt.Printf("| Done.\n")
	}

	l.lastPixShown = pixDone
}

func OutputLogs(pastEndIdx uint32, data []byte, strMap []LogStr,
	maxWorkers int) []LogOutput {

	return OutputLogsExt(pastEndIdx, data, strMap, maxWorkers, false)
}

func ProcessJobs(jobs <-chan logJob, wg *sync.WaitGroup) {
	for j := range jobs {
		packetData := j.packetData
		strMap := j.strMap
		out := j.out

		read := uint32(0)
		var numFields uint16
		var strMapId uint16
		var reqId uint64
		var timestamp int64

		var err error
		if err = readPacket(&read, packetData,
			reflect.ValueOf(&numFields)); err != nil {
		} else if err = readPacket(&read, packetData,
			reflect.ValueOf(&strMapId)); err != nil {
		} else if err = readPacket(&read, packetData,
			reflect.ValueOf(&reqId)); err != nil {
		} else if err = readPacket(&read, packetData,
			reflect.ValueOf(&timestamp)); err != nil {
		}

		args := make([]interface{}, numFields)
		for i := uint16(0); i < numFields; i++ {
			if err != nil {
				break
			}

			args[i], err = parseArg(&read, packetData)
		}

		if err != nil {
			// If the timestamp is zero, we will fill it in later with
			// the previous log's timestamp
			*out = newLog(LogQlog, QlogReqId, 0,
				"ERROR: Packet read error (%s). i"+
					"Dump of %d bytes:\n%x\n",
				[]interface{}{err, len(packetData),
					packetData})
			continue
		}

		// Grab the string and output
		if int(strMapId) > len(strMap) {
			*out = newLog(LogQlog, QlogReqId, 0,
				"Not enough entries in "+
					"string map (%d %d)\n",
				[]interface{}{strMapId,
					len(strMap) / LogStrSize})
			continue
		}
		mapEntry := strMap[strMapId]
		logSubsystem := (LogSubsystem)(mapEntry.LogSubsystem)

		// Finally, print with the front attached like normal
		mapStr := string(mapEntry.Text[:])
		firstNullTerm := strings.Index(mapStr, "\x00")
		if firstNullTerm != -1 {
			mapStr = mapStr[:firstNullTerm]
		}

		*out = newLog(logSubsystem, reqId, timestamp,
			mapStr+"\n", args)
	}
	wg.Done()
}

type logJob struct {
	packetData []byte
	strMap     []LogStr
	out        *LogOutput
}

func OutputLogsExt(pastEndIdx uint32, data []byte, strMap []LogStr, maxWorkers int,
	printStatus bool) []LogOutput {

	var logPtrs []*LogOutput
	readCount := uint32(0)

	jobs := make(chan logJob, 2)
	var wg sync.WaitGroup

	wg.Add(maxWorkers)
	for w := 0; w < maxWorkers; w++ {
		go ProcessJobs(jobs, &wg)
	}

	if printStatus {
		fmt.Printf("Parsing logs in %d threads...\n",
			maxWorkers)
	}
	status := NewLogStatus(50)

	for readCount < uint32(len(data)) {
		var packetLen uint16
		readBack(&pastEndIdx, data, packetLen, &packetLen)

		// If we read a packet of zero length, that means our buffer wasn't
		// full and we've hit the unused area
		if packetLen == 0 {
			break
		}

		// If the packet's uppermost len bit isn't set, that means it's not
		// fully written and needs to be discarded
		completeEntry := (packetLen&entryCompleteBit != 0)

		// Now clear the upper bit so it doesn't mess up our packetLen
		packetLen &= ^(uint16(entryCompleteBit))

		// Prepare the pastEndIdx and readCount variables to allow us to skip
		wrapMinusEquals(&pastEndIdx, uint32(packetLen), len(data))
		readCount += uint32(packetLen) + 2

		// Update a status bar if needed
		if printStatus {
			readCountClip := uint32(readCount)
			if readCountClip > uint32(len(data)) {
				readCountClip = uint32(len(data))
			}
			status.Process(float32(readCountClip) / float32(len(data)))
		}

		if readCount > uint32(len(data)) {
			// We've read everything, and this last packet isn't valid
			break
		}

		// At this point we know the packet is fully present, finished, and
		// the idx / readCounts have been updated in prep for the next entry
		if !completeEntry {
			newLine := newLog(LogQlog, QlogReqId, 0,
				"WARN: Dropping incomplete packet.\n", nil)
			logPtrs = append(logPtrs, &newLine)
			continue
		}

		// Read the packet data into a separate buffer
		packetData := wrapRead(pastEndIdx, uint32(packetLen), data)

		logPtrs = append(logPtrs, new(LogOutput))
		jobs <- logJob{
			packetData: packetData,
			strMap:     strMap,
			out:        logPtrs[len(logPtrs)-1],
		}
	}
	close(jobs)
	if printStatus {
		status.Process(1)
	}

	wg.Wait()

	// Go through the logs and fix any missing timestamps. Use the last entry's,
	// and de-pointer-ify them.
	rtn := make([]LogOutput, len(logPtrs))
	var lastTimestamp int64
	for i := 0; i < len(logPtrs); i++ {
		if logPtrs[i].T == 0 {
			logPtrs[i].T = lastTimestamp
		} else {
			lastTimestamp = logPtrs[i].T
		}

		rtn[i] = *logPtrs[i]
	}

	if printStatus {
		fmt.Printf("Sorting parsed logs...")
	}

	sort.Sort(SortByTime(rtn))

	if printStatus {
		fmt.Printf("done\n")
	}

	return rtn
}
