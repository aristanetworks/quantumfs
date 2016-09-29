// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qparse is the shared memory log parser for the qlog quantumfs subsystem
package main

import "bufio"
import "errors"
import "flag"
import "fmt"
import "os"
import "sort"
import "strconv"
import "strings"

import "github.com/aristanetworks/quantumfs/qlog"

var tabSpaces *int
var file *string
var stats *bool

// -- worker structures

type stack []string

func (s *stack) Push(n string) {
	*s = append(*s, n)
}

func (s *stack) Pop() {
	if len(*s) > 0 {
		*s = (*s)[:len(*s)-1]
	}
}

func (s *stack) Peek() (string, error) {
	if len(*s) == 0 {
		return "", errors.New("Cannot peek on an empty stack")
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
	logs := qlog.OutputLogs(pastEndIdx, dataArray, strMap)

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

// Given a set of logs, collect deltas within and between function in/out pairs
func showOverallStats(logs []qlog.LogOutput) {

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

func showLogs(reqId uint64, logs []qlog.LogOutput) {
	filteredLogs := make([]qlog.LogOutput, 0)
	for i := 0; i < len(logs); i++ {
		if logs[i].ReqId == reqId {
			filteredLogs = append(filteredLogs, logs[i])
		}
	}

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
