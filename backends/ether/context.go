// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package ether

// Ctx represents client context for Ether API calls.
// Since clients implement this interface, they can use custom loggers,
// log format, context information(eg: RequestID etc).
type Ctx interface {
	// Elog logs error message.
	Elog(fmtStr string, args ...interface{})
	// Wlog logs warning message.
	Wlog(fmtStr string, args ...interface{})
	// Dlog logs debug message.
	Dlog(fmtStr string, args ...interface{})
	// Vlog logs verbose message.
	Vlog(fmtStr string, args ...interface{})

	// FuncIn logs function details upon entry.
	FuncIn(funcName string, extraFmtStr string, args ...interface{}) FuncOut

	// FuncInName logs function name upon entry.
	FuncInName(funcName string) FuncOut
}

// FuncOut defines an interface for logging function details prior to return
//  FuncIn and FuncOut are paired.
type FuncOut interface {
	// Out logs function details before return from the function.
	Out()
}
