// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qupload is a command line tool used to upload a file or directory hierarchy
// into a datastore and workspace DB supported by QFS. This tool
// does not require an QFS instance to be available locally. The content
// uploaded by this tool can be accessed using any QFS instance.
package main

import "errors"
import "flag"
import "fmt"
import "os"
import "path/filepath"
import "strings"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr"
import "github.com/aristanetworks/quantumfs/thirdparty_backends"

// Various exit reasons returned to the shell as exit code
const (
	exitOk        = iota
	exitErrArgs   = iota
	exitErrUpload = iota
)

type params struct {
	progress    bool
	dsName      string
	dsConf      string
	wsdbName    string
	wsdbConf    string
	ws          string
	advance     string
	baseDir     string
	excludeFile string
	conc        uint
}

var dataStore quantumfs.DataStore
var wsDB quantumfs.WorkspaceDB
var version string

func showUsage() {
	fmt.Printf(`
qupload - tool to upload a directory hierarchy to a QFS supported datastore
version: %s
usage: qupload -datastore <dsname> -datastoreconf <dsconf>
               -workspaceDB <wsdbname> -workspaceDBconf <wsdbconf>
	       -workspace <wsname> [-progress -advance <wsname>]
	       -basedir <path> [ -exclude <file> | <reldirpath> ]
Exmaples:
1) qupload -workspace build/eos-trunk/11223344
		   -basedir /var/Abuild/66778899 -exclude excludeFile
Above command will upload the contents of /var/Abuild/66778899 directory
to the QFS workspace build/eos-trunk/11223344. The files and directories
specified by excludeFile are excluded from upload. See below for details about
the exclude file format.

2) qupload -workspace build/eos-trunk/11223344
		   -basedir /var/Abuild/66778899 src
Above command will upload the contents of /var/Abuild/66778899/src directory
to the QFS workspace build/eos-trunk/11223344. All files and sub-directories
are included since an exclude file is not specified.

The exclude file specifies paths that must be excluded from upload.
The exclude file should be formatted based on following rules:

One path per line
Path must be relative to the base directory specified
Absolute paths are not allowed
To create a directory without its contents, specify / at the end of path
To skip directory completely, specify directory name without trailing /
Comments and empty lines are allowed. A comment is any line that starts with #
`, version)
	flag.PrintDefaults()
}

func validateParams(p *params) error {
	var err error

	// check mandatory flags
	if p.ws == "" || p.baseDir == "" || p.dsName == "" ||
		p.dsConf == "" || p.wsdbName == "" || p.wsdbConf == "" {
		return errors.New("One or more mandatory flags are missing")
	}

	if strings.Count(p.ws, "/") != 2 {
		return errors.New("Workspace name must contain " +
			"precisely two \"/\"")
	}

	if p.advance != "" && strings.Count(p.advance, "/") != 2 {
		return errors.New("Workspace to be advanced must " +
			"contain precisely two \"/\"")
	}

	if (p.excludeFile == "" && flag.NArg() == 0) ||
		(p.excludeFile != "" && flag.NArg() > 0) {
		return errors.New("One of exclude file or directory " +
			"argument must be specified")
	}

	if flag.NArg() > 1 {
		return errors.New("At most 1 directory or exclude file " +
			"must be specified")
	}

	root := p.baseDir
	if flag.NArg() == 1 {
		if strings.HasPrefix(flag.Arg(0), "/") {
			return errors.New("Directory argument must be " +
				"relative to the base directory. Do not " +
				"absolute path")
		}
		root = filepath.Join(p.baseDir, flag.Arg(0))
	}

	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("%s : %s\n", root, err)
	}
	if !info.Mode().IsRegular() && !info.IsDir() {
		return fmt.Errorf("%s must be regular file or directory", root)
	}

	if p.conc == 0 {
		return errors.New("Concurrency must be > 0")
	}

	dataStore, err = thirdparty_backends.ConnectDatastore(p.dsName, p.dsConf)
	if err != nil {
		return err
	}
	wsDB, err = thirdparty_backends.ConnectWorkspaceDB(p.wsdbName, p.wsdbConf)
	if err != nil {
		return err
	}
	ws1Parts := strings.Split(p.ws, "/")
	_, err = wsDB.Workspace(nil,
		ws1Parts[0], ws1Parts[1], ws1Parts[2])
	werr, _ := err.(*quantumfs.WorkspaceDbErr)
	if err == nil {
		return fmt.Errorf("Workspace %s must not exist\n", p.ws)
	}
	if err != nil && werr.Code != quantumfs.WSDB_WORKSPACE_NOT_FOUND {
		return fmt.Errorf("Error in workspace %s\n", err)
	}
	if p.advance != "" {
		advParts := strings.Split(p.advance, "/")
		_, err = wsDB.Workspace(nil,
			advParts[0], advParts[1], advParts[2])
		if err != nil {
			return fmt.Errorf("Error in advance workspace %s\n", err)
		}
	}

	return nil
}

func main() {

	var cliParams params
	var err error

	flag.BoolVar(&cliParams.progress, "progress", false,
		"Show the data and metadata sizes uploaded")
	flag.StringVar(&cliParams.dsName, "datastore", "",
		"Name of the datastore to use")
	flag.StringVar(&cliParams.dsConf, "datastoreconf", "",
		"Options to pass to datastore")
	flag.StringVar(&cliParams.wsdbName, "workspaceDB", "",
		"Name of the workspace DB to use")
	flag.StringVar(&cliParams.wsdbConf, "workspaceDBconf", "",
		"Options to pass to workspace DB")
	flag.StringVar(&cliParams.ws, "workspace", "",
		"Name of workspace which'll contain uploaded data")
	flag.StringVar(&cliParams.advance, "advance", "",
		"Name of workspace which'll be advanced to point"+
			"to uploaded workspace")
	flag.StringVar(&cliParams.baseDir, "basedir", "",
		"All directory arguments are relative to this base directory")
	flag.StringVar(&cliParams.excludeFile, "exclude", "",
		"Exclude the files and directories specified in this file")
	flag.UintVar(&cliParams.conc, "concurrency", 10,
		"Number of concurrent uploaders")

	flag.Usage = showUsage
	flag.Parse()

	if flag.NFlag() == 0 {
		flag.Usage()
		os.Exit(0)
	}

	perr := validateParams(&cliParams)
	if perr != nil {
		fmt.Println("Flag validation failed: ", perr)
		os.Exit(exitErrArgs)
	}

	relpath := ""
	var exInfo *ExcludeInfo
	if cliParams.excludeFile != "" {
		exInfo, err = LoadExcludeInfo(cliParams.baseDir,
			cliParams.excludeFile)
		if err != nil {
			fmt.Println("Exclude file processing failed: ", err)
			os.Exit(exitErrArgs)
		}
	} else {
		relpath = flag.Arg(0)
	}

	if cliParams.progress == true {
		go func() {
			fmt.Println("Data and Metadata is the size being " +
				"read by qupload tool for uploading to QFS " +
				"datastore. Since upload data is deduped, " +
				"less data may be sent on wire")
			var d1, m1, d2, m2, speed uint64
			for {
				start := time.Now()
				d1 = qwr.DataBytesWritten
				m1 = qwr.MetadataBytesWritten
				fmt.Printf("\rData: %12d Metadata: %12d "+
					"Speed: %5d MB/s", d1, m1, speed)
				time.Sleep(1 * time.Second)
				d2 = qwr.DataBytesWritten
				m2 = qwr.MetadataBytesWritten
				speed = (((d2 + m2) - (d1 + m1)) /
					uint64(1000000)) /
					uint64(time.Since(start).Seconds())
			}
		}()
	}

	start := time.Now()
	upErr := upload(cliParams.ws, cliParams.advance,
		filepath.Join(cliParams.baseDir, relpath), exInfo,
		cliParams.conc)
	if upErr != nil {
		fmt.Println("Upload failed: ", upErr)
		os.Exit(exitErrUpload)
	}

	fmt.Printf("Upload completed. Total: %d bytes "+
		"(Data:%d(%d%%) Metadata:%d(%d%%)) in %.0f secs to %s\n",
		qwr.DataBytesWritten+qwr.MetadataBytesWritten,
		qwr.DataBytesWritten,
		(qwr.DataBytesWritten*100)/
			(qwr.DataBytesWritten+qwr.MetadataBytesWritten),
		qwr.MetadataBytesWritten,
		(qwr.MetadataBytesWritten*100)/
			(qwr.DataBytesWritten+qwr.MetadataBytesWritten),
		time.Since(start).Seconds(), cliParams.ws)
}
