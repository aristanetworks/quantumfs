// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aristanetworks/ether/utils"
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/thirdparty_backends"
	"github.com/aristanetworks/quantumfs/walker"
)

func checkWSName(parts []string) error {

	if (len(parts) != 3) || (parts[0] == "") || (parts[1] == "") || (parts[2] == "") {
		return errors.New("WS name not in correct format")
	}
	return nil
}

func getDataStore(name string, config string) quantumfs.DataStore {
	for _, datastore := range thirdparty_backends.Datastores {
		if datastore.Name != name {
			continue
		}

		store := datastore.Constructor(config)
		if store == nil {
			fmt.Printf("Failed to construct datastore '%s:%s'\n",
				name, config)
			return nil
		}
		return store
	}

	fmt.Printf("Failed to find datastore '%s'\n", name)
	return nil
}

func getWorkspaceDB(name string, config string) quantumfs.WorkspaceDB {
	for _, db := range thirdparty_backends.WorkspaceDBs {
		if db.Name != name {
			continue
		}

		ws := db.Constructor(config)
		if ws == nil {
			fmt.Printf("WorkspaceDB Constructor failed '%s:%s'\n",
				name, config)
			return nil
		}
		return ws
	}

	fmt.Printf("Failed to find workspaceDB '%s'\n", name)
	return nil
}

func handleKeycount(ds quantumfs.DataStore,
	db quantumfs.WorkspaceDB, arg string) error {

	parts := strings.Split(arg, "/")
	err := checkWSName(parts)
	if err != nil {
		return err
	}

	rootID, err := db.Workspace(nil, parts[0], parts[1], parts[2])
	if err != nil {
		return err
	}

	var totalKeys, totalSize, uniqKeys, uniqSize uint64
	keysRecorded := make(map[string]bool)

	sizer := func(path string, key quantumfs.ObjectKey, size uint64) error {
		if _, exists := keysRecorded[key.String()]; !exists {
			keysRecorded[key.String()] = true
			uniqKeys++
			uniqSize += size
		}
		totalSize += size
		totalKeys++

		return nil
	}

	err = walker.Walk(ds, db, rootID, sizer)
	if err != nil {
		return err
	}

	fmt.Println("Unique Keys = ", uniqKeys)
	fmt.Println("Unique Size = ", utils.HumanizeBytes(uniqSize))
	fmt.Println("Total Keys = ", totalKeys)
	fmt.Println("Total Size = ", utils.HumanizeBytes(totalSize))

	return nil
}

func handleKeydiffcount(ds quantumfs.DataStore,
	db quantumfs.WorkspaceDB, arg string) error {

	args := strings.Split(arg, ":")
	root1 := strings.Split(args[0], "/")
	root2 := strings.Split(args[1], "/")

	err := checkWSName(root1)
	if err != nil {
		return err
	}

	err = checkWSName(root2)
	if err != nil {
		return err
	}

	var rootID1, rootID2 quantumfs.ObjectKey

	rootID1, err = db.Workspace(nil, root1[0], root1[1], root1[2])
	if err != nil {
		return err
	}

	rootID2, err = db.Workspace(nil, root2[0], root2[1], root2[2])
	if err != nil {
		return err
	}

	keys := make(map[string]bool)
	keyRecorder := func(path string, key quantumfs.ObjectKey, size uint64) error {
		keys[key.String()] = true
		return nil
	}

	err = walker.Walk(ds, db, rootID1, keyRecorder)
	if err != nil {
		return err
	}

	diffKeyTotal := 0
	var diffSize uint64
	keySeen := make(map[string]bool)

	diffKeyRecorder := func(path string, key quantumfs.ObjectKey, size uint64) error {
		if _, seen := keySeen[key.String()]; seen {
			return nil
		}

		if _, exists := keys[key.String()]; !exists {
			diffKeyTotal++
			diffSize = diffSize + size
		}
		return nil
	}
	err = walker.Walk(ds, db, rootID2, diffKeyRecorder)
	if err != nil {
		return err
	}

	fmt.Printf("%s has\n", root2[0]+"/"+root2[1]+"/"+root2[2])

	fmt.Printf("Different keys: %v\n", diffKeyTotal)
	fmt.Printf("Different size: %s\n", utils.HumanizeBytes(diffSize))

	return nil
}

func handleDiskUsage(ds quantumfs.DataStore,
	db quantumfs.WorkspaceDB, arg string) error {

	args := strings.Split(arg, ":")
	parts := strings.Split(args[0], "/")

	err := checkWSName(parts)
	if err != nil {
		return err
	}

	if !filepath.IsAbs(args[1]) {
		return errors.New("Invalid path provided")
	}

	fmt.Println("WS: ", parts[0]+"/"+parts[1]+"/"+parts[2], " Path: ", args[1])

	rootID, err := db.Workspace(nil, parts[0], parts[1], parts[2])
	if err != nil {
		return err
	}

	var totalSize uint64
	sizer := func(path string, key quantumfs.ObjectKey, size uint64) error {
		if !strings.HasPrefix(path, args[1]) {
			return nil
		}
		totalSize += size
		return nil
	}

	err = walker.Walk(ds, db, rootID, sizer)
	if err != nil {
		return err
	}

	fmt.Println("Total Size = ", utils.HumanizeBytes(totalSize))

	return nil
}

func main() {

	dsname := flag.String("dn", "null", "DataStore backend name")
	dsconfig := flag.String("dc", "null", "DataStore backend config")
	wsname := flag.String("wn", "", "Workspace DB backend name")
	wsconfig := flag.String("wc", "", "Workspace DB backend config")

	walksubcmd := flag.String("cmd", "",
		"Walk sub-command: keycount, keydiffcount, du")
	walksubcmdArg := flag.String("cmdargs", "", "Walk sub-command argument")

	flag.Usage = func() {
		fmt.Println(
			"This tool walks all the keys within a workspace and reports ",
			"total keys and space")
		flag.PrintDefaults()
	}

	flag.Parse()

	if *dsname == "" || *dsconfig == "" || *wsname == "" || *wsconfig == "" ||
		*walksubcmd == "" || *walksubcmdArg == "" {

		fmt.Println("Failed: mandatory options missing")
		flag.Usage()
		os.Exit(1)
	}

	db := getWorkspaceDB(*wsname, *wsconfig)
	if db == nil {
		os.Exit(1)
	}

	ds := getDataStore(*dsname, *dsconfig)
	if ds == nil {
		os.Exit(1)
	}

	var err error

	start := time.Now()
	fmt.Println("Started walk...")
	// TODO: build a progress bar to indicate tool progress

	switch *walksubcmd {
	case "keycount":
		err = handleKeycount(ds, db, *walksubcmdArg)
	case "keydiffcount":
		err = handleKeydiffcount(ds, db, *walksubcmdArg)
	case "du":
		err = handleDiskUsage(ds, db, *walksubcmdArg)
	default:
		fmt.Println("Unsupported walk sub-command: ", *walksubcmd)
		os.Exit(1)
	}

	if err != nil {
		fmt.Println("Walk failed with error: ", err.Error())
		os.Exit(1)
	}

	walkTime := time.Since(start)
	fmt.Println("Walk completed successfully in ", walkTime)
}
