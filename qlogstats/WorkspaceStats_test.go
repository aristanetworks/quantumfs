// Copyright (c) 2018 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package qlogstats

import (
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/daemon"
	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
)

func TestWorkspaceStats(t *testing.T) {
	runTest(t, func(test *testHelper) {
		qlogHandle := test.Logger
		tOffset := int64(0)

		log := func(reqId uint64, format string, args ...interface{}) {
			qlogHandle.Log_(time.Unix(0, tOffset), qlog.LogTest, reqId,
				2, format, args...)
			tOffset++
		}
		logIn := func(reqId uint64, funcName string, format string,
			args ...interface{}) {

			log(reqId, qlog.FnEnterStr+funcName+" "+format,
				args...)
		}
		logOut := func(reqId uint64, funcName string) {
			log(reqId, qlog.FnExitStr+funcName)
		}

		logIn(1, daemon.LookupLog, daemon.InodeNameLog, 1234, "filename")
		logIn(2, daemon.ReadlinkLog, daemon.InodeOnlyLog, 1234)
		log(1, daemon.FuseRequestWorkspace, "req1/n/w")
		logOut(1, daemon.LookupLog)
		log(2, daemon.FuseRequestWorkspace, "req2/n/w")
		log(2, "test log")

		logIn(3, daemon.ReadLog, daemon.FileHandleLog, 16)
		log(3, daemon.FuseRequestWorkspace, "req1/n/w")
		logOut(3, daemon.ReadLog)

		logOut(2, daemon.ReadlinkLog)

		logIn(4, daemon.ReadLog, daemon.FileHandleLog, 17)
		log(4, daemon.FuseRequestWorkspace, "req4/n/w")
		logOut(4, daemon.ReadLog)

		log(15, daemon.WorkspaceFinishedFormat, "req1/n/w")
		log(16, daemon.WorkspaceFinishedFormat, "req2/n/w")

		checked := false
		checker := func(memdb *processlocal.Memdb) {
			foundReq1Lookup := false
			foundReq1Read := false
			foundReq2Readlink := false

			for _, d := range memdb.Data {
				test.Assert(d.Measurement == "quantumFsLatency",
					"Invalid name %s", d.Measurement)

				test.Assert(d.Tags["statName"] == "FUSE Requests",
					"Incorrect statName %s", d.Tags["statName"])

				ws := d.Fields["workspace"]
				op := d.Tags["operation"]

				switch {
				default:
					test.Assert(false, "Unknown ws/op: %s %s",
						ws, op)

				case ws == "req4/n/w":
					// We should never see any stats for req4
					// because we never mark the workspace as
					// finished.
					test.Assert(false,
						"Workspace req4 has stats!")

				case ws == "req1/n/w" && op == "Mux::Lookup":
					foundReq1Lookup = true

				case ws == "req1/n/w" && op == "Mux::Read":
					foundReq1Read = true

				case ws == "req2/n/w" && op == "Mux::Readlink":
					foundReq2Readlink = true
				}
			}

			test.Assert(foundReq1Lookup && foundReq1Read &&
				foundReq2Readlink, "Not all data found %t %t %t",
				foundReq1Lookup, foundReq1Read, foundReq2Readlink)
			checked = true
		}

		test.runExtractorTest(qlogHandle,
			NewExtWorkspaceStats("FUSE Requests"), checker)

		test.Assert(checked, "test not checking anything")
	})
}
