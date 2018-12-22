// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/cmd/qutils/cmdproc"
	"github.com/aristanetworks/quantumfs/utils/simplebuffer"
)

func init() {
	registerPrintBlockCmd()
}

func registerPrintBlockCmd() {
	var cmd cmdproc.CommandInfo
	cmd.Name = "printBlock"
	cmd.Usage = "key"
	cmd.Short = "print the block pointed by key"
	cmd.Details = `
key
	key to the block
`
	cmd.Run = printBlockCmd
	cmdproc.RegisterCommand(cmd)
}

func printBlockCmd(args []string) error {
	if len(args) != 1 {
		return cmdproc.NewBadArgExitErr("incorrect arguments")
	}
	keyStr := args[0]
	key, err := quantumfs.FromString(keyStr)
	if err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}

	buf := simplebuffer.New(nil, key)
	if err := cs.qfsds.Get(&cs.ctx.Ctx, key, buf); err != nil {
		return cmdproc.NewBadCmdExitErr("%s", err)
	}

	if buf.Size() == 0 {
		return cmdproc.NewBadCmdExitErr("key exists but no block data")
	}

	switch key.Type() {
	case quantumfs.KeyTypeMetadata:
		printMetadata(buf)
	default:
		fmt.Println("Pretty printing is not supported for this key type.")
		fmt.Println()
		fmt.Println(hex.Dump(buf.Get()))
	}

	return nil
}

func prettyPrint(jsonInput string) {
	var out bytes.Buffer
	if err := json.Indent(&out, []byte(jsonInput), "", " "); err != nil {
		fmt.Printf("Indent failed: %v\n", err)
	}
	out.WriteTo(os.Stdout)
	fmt.Println()
}

// quantumfs.Buffer doesn't provide a means to detect the
// kind of metadata object it is pointing to. Currently we
// dump the same buffer as all possible metadata types and
// then visually inspect to see which metadata type it
// actually represents.
//
// During visual inspection, we can use clues like workspace
// root's base layer should have metadata key type, the hard link
// information in wsr should have non-null filename etc to decide
// the metadata object represented by buffer.
//
// In future, these clues can be turned into rules in the code
// to detect the metadata type automatically.
func printMetadata(buf quantumfs.Buffer) {
	var out string
	var err error

	wsr := buf.AsWorkspaceRoot()
	out, err = wsr.String()
	if err != nil {
		fmt.Printf("Error in JSON string for WSR: %v\n", err)
	}
	prettyPrint(out)

	de := buf.AsDirectoryEntry()
	out, err = de.String()
	if err != nil {
		fmt.Printf("Error in JSON string for DirEntry: %v\n", err)
	}
	prettyPrint(out)

	mb := buf.AsMultiBlockFile()
	out, _ = mb.String()
	if err != nil {
		fmt.Printf("Error in JSON string for MultiBlockFile: %v\n", err)
	}
	prettyPrint(out)

	vlf := buf.AsVeryLargeFile()
	out, _ = vlf.String()
	if err != nil {
		fmt.Printf("Error in JSON string for VeryLargeFile: %v\n", err)
	}
	prettyPrint(out)

	ea := buf.AsExtendedAttributes()
	out, _ = ea.String()
	if err != nil {
		fmt.Printf("Error in JSON string for ExtAttr: %v\n", err)
	}
	prettyPrint(out)

	hle := buf.AsHardlinkEntry()
	out, _ = hle.String()
	if err != nil {
		fmt.Printf("Error in JSON string for HardlinkEntry: %v\n", err)
	}
	prettyPrint(out)
}
