// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package main

import (
	"text/template"
)

var usageTemplate = `
qubit-walkercmd is a tool that walks a workspace and runs command on keys found.

Version: {{.CommonUsageInfo.Version}}

Usage:
 qubit-walkercmd {{.CommonUsageInfo.Usage}} [commands with arguments]

{{.CommonUsageInfo.Short}}
The exit codes from qubit-walkercmd are:
{{ call .CommonUsageInfo.ExplainExitCodes }}

The commands are:

{{range .Commands}}{{.Name | printf "%-20s"}} {{.Short | printf "%s\n"}}{{end}}
Use "qubit-walkercmd help [command]" for more information about a command.

`

var helpTemplate = `
usage: qubit-walkercmd {{.CommonUsageInfo.Usage}} {{.CommandHelpInfo.Name}} 
                       {{.CommandHelpInfo.Usage}}

{{.CommandHelpInfo.Short}}

{{.CommonUsageInfo.Short}}
{{with .CommandHelpInfo.Details -}}
Command specific info:
{{.}}
{{end}}
`

var commonUsage = `-cfg config [-progress]`
var commonShort = `Common options are:

-cfg config
	configuration to access Ether CQL keyspace
-progress
	show progress during the workspace walk
`

// currently there are no additional common details
var commonDetails = commonShort

// utility specific template information is
// registered with cmdproc
func setupTemplates() {
	commonInfo := CommonUsageInfo{
		Version:          version,
		Usage:            commonUsage,
		Short:            commonShort,
		Details:          commonShort,
		ExplainExitCodes: nil, // no utility specific explanation
	}

	usageTmpl := template.Must(template.New("usageTemplate").
		Parse(usageTemplate))
	helpTmpl := template.Must(template.New("helpTemplate").Parse(helpTemplate))

	RegisterTemplateInfo(commonInfo, usageTmpl, helpTmpl)
}
