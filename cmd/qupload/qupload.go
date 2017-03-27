// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qupload is a command line tool used to upload a file or directory hirarchy
// into a datastore and workspace DB supported by QFS. This tool
// does not require QFS instance to be available locally. The content
// uploaded by this tool can be accessed using any QFS instance.
package main

import "errors"
import "flag"
import "fmt"
import "os"
import "strings"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr"
import "github.com/aristanetworks/quantumfs/cmd/qupload/qwr/utils"

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
}

var version string

func showUsage() {
	fmt.Printf(`
qupload - tool to upload a directory hierarchy to a QFS supported datastore
version: %s
usage: qupload [-progress -datastore <dsname> -datastoreconf <dsconf>
                -workspaceDB <wsdbname> -workspaceDBconf <wsdbconf>]
				-workspace <wsname> [-advance <wsname>]
				-basedir <path> [ -exclude <file> | <reldirpath> ]
Exmaples:
1) qupload -workspace build/eos-trunk/11223344
		   -basedir /var/Abuild/66778899 -exclude excludeFile
Above command will upload the contents of /var/Abuild/66778899 directory
to the QFS workspace build/eos-trunk/11223344. The files and directories
specified by excludeFile are excluded from upload.

2) qupload -workspace build/eos-trunk/11223344
		   -basedir /var/Abuild/66778899 src
Above command will upload the contents of /var/Abuild/66778899/src directory
to the QFS workspace build/eos-trunk/11223344. The files and directories
specified by excludeFile are excluded from upload. 

`, version)
	flag.PrintDefaults()
}

func validateParams(p *params) (quantumfs.DataStore, quantumfs.WorkspaceDB, error) {

	// check mandatory args
	if p.dsName == "" || p.dsConf == "" || p.wsdbName == "" ||
		p.wsdbConf == "" || p.ws == "" || p.baseDir == "" {
		return nil, nil, errors.New("One or more mandatory flags are missing")
	}

	if strings.Count(p.ws, "/") != 2 {
		return nil, nil, errors.New("Workspace name must contain precisely two \"/\"")
	}

	if p.advance != "" && strings.Count(p.advance, "/") != 2 {
		return nil, nil, errors.New("Workspace to be advanced must contain precisely two \"/\"")
	}

	if p.excludeFile == "" && flag.Arg(0) == "" {
		return nil, nil, errors.New("One of -excludeFile or directory argument must be specified")
	}

	ds, dsErr := qwr.ConnectDatastore(p.dsName, p.dsConf)
	if dsErr != nil {
		return nil, nil, dsErr
	}
	wsdb, wsdbErr := qwr.ConnectWorkspaceDB(p.wsdbName, p.wsdbConf)
	if wsdbErr != nil {
		return nil, nil, wsdbErr
	}
	ws1Parts := strings.Split(p.ws, "/")
	advParts := strings.Split(p.advance, "/")
	_, err := wsdb.Workspace(nil,
		ws1Parts[0], ws1Parts[1], ws1Parts[2])
	werr, _ := err.(*quantumfs.WorkspaceDbErr)
	if err == nil {
		return nil, nil, fmt.Errorf("Workspace %s must not exist\n", p.ws)
	}
	if err != nil && werr.Code != quantumfs.WSDB_WORKSPACE_NOT_FOUND {
		return nil, nil, fmt.Errorf("Error in workspace %s\n", err)
	}
	_, aerr := wsdb.Workspace(nil,
		advParts[0], advParts[1], advParts[2])
	if aerr != nil {
		return nil, nil, fmt.Errorf("Error in advance workspace %s\n", aerr)
	}

	return ds, wsdb, nil
}

func main() {

	var cliParams params

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
		"Name of workspace which'll be advanced to point to uploaded workspace")
	flag.StringVar(&cliParams.baseDir, "basedir", "",
		"All directory arguments are relative to this base directory")
	flag.StringVar(&cliParams.excludeFile, "exclude", "",
		"Exclude the files and directories specified in this file")

	flag.Usage = showUsage
	flag.Parse()

	if flag.NFlag() == 0 {
		flag.Usage()
		os.Exit(0)
	}

	ds, wsdb, perr := validateParams(&cliParams)
	if perr != nil {
		fmt.Println(perr)
		os.Exit(exitErrArgs)
	}

	relpath := ""
	if cliParams.excludeFile != "" {
		exErr := utils.LoadExcludeList(cliParams.excludeFile)
		if exErr != nil {
			fmt.Println(exErr)
			os.Exit(exitErrArgs)
		}
	} else {
		relpath = flag.Arg(0)
	}

	if cliParams.progress == true {
		go func() {
			var d1, m1, d2, m2, speed uint64
			for {
				start := time.Now()
				d1 = qwr.DataBytesWritten
				m1 = qwr.MetadataBytesWritten
				fmt.Printf("\rData: %12d Metadata: %12d Speed: %4d MB/s",
					d1, m1, speed)
				time.Sleep(1 * time.Second)
				d2 = qwr.DataBytesWritten
				m2 = qwr.MetadataBytesWritten
				speed = (((d2 + m2) - (d1 + m1)) / uint64(1000000)) / uint64(time.Since(start).Seconds())
			}
		}()
	}

	start := time.Now()
	upErr := upload(ds, wsdb, cliParams.ws, cliParams.advance, cliParams.baseDir, relpath)
	if upErr != nil {
		fmt.Println(upErr)
		os.Exit(exitErrUpload)
	}

	fmt.Printf("Uploaded Total: %d bytes (Data:%d(%d%%) Metadata:%d(%d%%)) in %.0f secs to %s\n",
		qwr.DataBytesWritten+qwr.MetadataBytesWritten,
		qwr.DataBytesWritten, (qwr.DataBytesWritten*100)/(qwr.DataBytesWritten+qwr.MetadataBytesWritten),
		qwr.MetadataBytesWritten, (qwr.MetadataBytesWritten*100)/(qwr.DataBytesWritten+qwr.MetadataBytesWritten),
		time.Since(start).Seconds(), cliParams.ws)
}
