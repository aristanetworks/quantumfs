// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package utils

import (
	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/backends/cql"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/walker"
)

type ECtx walker.Ctx

func (dc *ECtx) Elog(fmtStr string, args ...interface{}) {
	(*walker.Ctx)(dc).Qctx.Elog(qlog.LogDatastore, fmtStr, args...)
}

func (dc *ECtx) Wlog(fmtStr string, args ...interface{}) {
	(*walker.Ctx)(dc).Qctx.Wlog(qlog.LogDatastore, fmtStr, args...)
}

func (dc *ECtx) Dlog(fmtStr string, args ...interface{}) {
	(*walker.Ctx)(dc).Qctx.Dlog(qlog.LogDatastore, fmtStr, args...)
}

func (dc *ECtx) Vlog(fmtStr string, args ...interface{}) {
	(*walker.Ctx)(dc).Qctx.Vlog(qlog.LogDatastore, fmtStr, args...)
}

type cqlFuncOut quantumfs.ExitFuncLog

func (e cqlFuncOut) Out() {
	(quantumfs.ExitFuncLog)(e).Out()
}

func (dc *ECtx) FuncIn(funcName string, fmtStr string,
	args ...interface{}) cql.FuncOut {

	el := (*walker.Ctx)(dc).Qctx.FuncIn(qlog.LogDatastore, funcName,
		fmtStr, args...)
	return (cqlFuncOut)(el)
}

func (dc *ECtx) FuncInName(funcName string) cql.FuncOut {
	return dc.FuncIn(funcName, "")
}

func ToECtx(c *walker.Ctx) *ECtx {
	return (*ECtx)(c)
}
