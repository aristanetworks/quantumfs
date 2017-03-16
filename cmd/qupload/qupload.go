// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qupload is a command line tool used to upload files or directories
// into a datastore and workspace DB supported by QFS. This tool
// does not require QFS instance to be available locally. The content
// uploaded by this tool can be accessed using QFS.
package main

import "flag"
import "fmt"
import "os"
import "strings"
import "time"

import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

// Various exit reasons returned to the shell as exit code
const (
	exitOk      = iota
	exitBadArgs = iota
	exitArgErr  = iota
	exitUpErr   = iota
)

func showUsage() {
	fmt.Println("usage: qupload -datastore <dsname> -datastoreconf <dsconf>" +
		" -workspaceDB <wsname> -workspaceDBconf <wsconf> " +
		" -basedir <dirname> [ -exclude <file> | dir ]")
	flag.PrintDefaults()
}

func main() {

	dsName := flag.String("datastore", "",
		"Name of the datastore to use")
	dsConf := flag.String("datastoreconf", "",
		"Options to pass to datastore")
	wsdbName := flag.String("workspaceDB", "",
		"Name of the workspace DB to use")
	wsdbConf := flag.String("workspaceDBconf", "",
		"Options to pass to workspace DB")
	ws := flag.String("workspace", "",
		"Name of workspace which'll contain uploaded data")
	baseDir := flag.String("basedir", "",
		"All directory arguments are relative to this base directory")
	excludeFile := flag.String("exclude", "",
		"Exclude the files and directories specified in this file")

	flag.Usage = showUsage
	flag.Parse()

	// TODO(krishna): check flag values and arg values
	if strings.Count(*ws, "/") != 2 {
		fmt.Println("Workspace name must contain precisely two \"/\"")
		os.Exit(exitBadArgs)
	}

	ds, dsErr := qwr.ConnectDatastore(*dsName, *dsConf)
	if dsErr != nil {
		fmt.Println(dsErr)
		os.Exit(exitArgErr)
	}

	wsdb, wsdbErr := qwr.ConnectWorkspaceDB(*wsdbName, *wsdbConf)
	if wsdbErr != nil {
		fmt.Println(wsdbErr)
		os.Exit(exitArgErr)
	}

	// TODO(krishna): exclude file and directory argument cannot be
	//                specified together
	relpath := ""
	if flag.NArg() == 0 {
		exErr := utils.LoadExcludeList(*excludeFile)
		if exErr != nil {
			fmt.Println(exErr)
			os.Exit(exitArgErr)
		}
	} else {
		relpath = flag.Arg(0)
	}

	start := time.Now()
	upErr := upload(ds, wsdb, *ws, *baseDir, relpath)
	if upErr != nil {
		fmt.Println(upErr)
		os.Exit(exitUpErr)
	}

	fmt.Printf("Uploaded in %.0f secs to %s\n",
		time.Since(start).Seconds(), *ws)
}
