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

// Various exit reasons returned to the shell as exit code
const (
	exitOk           = iota
	exitBadArgs      = iota
	exitDatastoreErr = iota
	exitWsdbErr      = iota
	exitUpErr        = iota
)

func showUsage() {
	fmt.Println("usage: qupload -datastore <dsname> -datastoreconf <dsconf>" +
		" -workspaceDB <wsname> -workspaceDBconf <wsconf>" +
		" dir1[,dir2[,...]]")
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

	flag.Usage = showUsage
	flag.Parse()

	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(exitBadArgs)
	}

	if flag.NFlag() == 0 || flag.NArg() == 0 {
		flag.Usage()
		os.Exit(exitBadArgs)
	}

	// TODO(krishna): check flag values and arg values

	if strings.Count(*ws, "/") != 2 {
		fmt.Println("Workspace name must contain precisely two \"/\"")
		os.Exit(exitBadArgs)
	}

	ds, dsErr := qwr.ConnectDatastore(*dsName, *dsConf)
	if dsErr != nil {
		fmt.Println(dsErr)
		os.Exit(exitDatastoreErr)
	}

	wsdb, wsdbErr := qwr.ConnectWorkspaceDB(*wsdbName, *wsdbConf)
	if wsdbErr != nil {
		fmt.Println(wsdbErr)
		os.Exit(exitWsdbErr)
	}

	// TODO(krishna): ensure all args are dirs
	// TODO(krishna): support mix of files and dirs later
	var dirs []string
	for d := 0; d < flag.NArg(); d++ {
		fmt.Println("Appending dir: ", flag.Arg(d))
		dirs = append(dirs, flag.Arg(d))
	}

	start := time.Now()
	upErr := upload(ds, wsdb, *baseDir, *ws, dirs)
	if upErr != nil {
		fmt.Println(upErr)
		os.Exit(exitUpErr)
	}

	fmt.Printf("Uploaded in %.0f secs to %s\n",
		time.Since(start).Seconds(), *ws)
}
