// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// cmdproc provides following facilities to a command line application
//  a) Template driven usage text
//  b) Template driven command help text
//  c) Register command handler with command specific usage and help text
package main

import (
	"fmt"
	"io"
	"os"
	"text/template"
)

// CommandUsageInfo is usage level information for a specific command.
type CommandUsageInfo struct {
	Name  string
	Usage string
	Short string
}

// CommandHelpInfo is help information for a specific command.
type CommandHelpInfo struct {
	CommandUsageInfo
	Details string
}

// CommandInfo is registration information specific to a command.
type CommandInfo struct {
	CommandHelpInfo
	Run func(args []string) error
}

// CommonUsageInfo is usage information common to all commands.
type CommonUsageInfo struct {
	Version          string
	Usage            string
	Short            string
	Details          string
	ExplainExitCodes func() (string, error)
}

// UsageTemplateInfo is used to render usage template.
type UsageTemplateInfo struct {
	CommonUsageInfo
	Commands []CommandUsageInfo
}

// HelpTemplateInfo is used to render help template for a
// specific command.
type HelpTemplateInfo struct {
	CommonUsageInfo
	CommandHelpInfo
}

// exit codes from command processor
const (
	ExitOk      = iota
	ExitBadArgs = iota
	ExitPreCmd  = iota
	ExitBadCmd  = iota
)

// internal state maintained by the command processor
var state struct {
	commands  []CommandInfo
	uti       UsageTemplateInfo
	helpTmpl  *template.Template
	usageTmpl *template.Template
}

// ExplainExitCodes is the default exit code information
func ExplainExitCodes() (string, error) {
	return `
0 = no errors detected
1 = failure in argument parsing
2 = failure in pre-command processing
3 = failure in command processing
`, nil
}

func execTmpl(t *template.Template, w io.Writer, data interface{}) {
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func getCommandHelpInfo(c CommandInfo) HelpTemplateInfo {
	var hti HelpTemplateInfo

	hti.CommonUsageInfo = state.uti.CommonUsageInfo
	hti.CommandHelpInfo = c.CommandHelpInfo
	return hti
}

func printUsage(w io.Writer) {
	execTmpl(state.usageTmpl, w, state.uti)
}

// Usage can be registered with flags package for printing usage information.
func Usage() {
	printUsage(os.Stderr)
	os.Exit(ExitBadArgs)
}

func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		os.Exit(ExitOk)
	}

	arg := args[0]
	for _, cmd := range state.commands {
		if cmd.Name == arg {
			execTmpl(state.helpTmpl, os.Stdout,
				getCommandHelpInfo(cmd))
			os.Exit(ExitOk)
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown command %q\n", arg)
	os.Exit(ExitBadArgs)
}

// exitError struct and handleExitError enables clients of
// cmdproc to be able to use utility specific error codes
// in future. Currently all exit codes are defined in cmdproc.
type exitError struct {
	message  string
	exitCode int
}

func (e *exitError) Error() string {
	return fmt.Sprintf("Error Message: %s ExitCode: %d\n", e.message,
		e.exitCode)
}

func handleExitError(err error) {
	if err != nil {
		if ee, ok := err.(*exitError); ok {
			fmt.Fprintf(os.Stderr, "Error: %s\n", ee.message)
			os.Exit(ee.exitCode)
		}
		panic("err must be of exitError type")
	}
}

// NewPreCmdExitErr returns an error with ExtiPreCmd exit code.
func NewPreCmdExitErr(format string, data ...interface{}) error {
	return &exitError{
		message:  fmt.Sprintf(format, data...),
		exitCode: ExitPreCmd,
	}
}

// NewBadArgExitErr returns an error with ExitBadArgs exit code.
func NewBadArgExitErr(format string, data ...interface{}) error {
	return &exitError{
		message:  fmt.Sprintf(format, data...),
		exitCode: ExitBadArgs,
	}
}

// NewBadCmdExitErr returns an error with ExitBadCmd exit code.
func NewBadCmdExitErr(format string, data ...interface{}) error {
	return &exitError{
		message:  fmt.Sprintf(format, data...),
		exitCode: ExitBadCmd,
	}
}

// ProcessCommands is used to process the parsed arguments.
//
// preCommand is used to setup any state needed by commands. It is
// executed just before running a command. It is not executed during
// usage or help text generation.

// In case of arg errors, processCommands will exit with ExitBadArgs.
// In case of cmd errors, processCommands will exit with ExitBadCmd.
// Upon successful run of help, processCommands exits with ExitOk.
// Upon successful run of a command, processCommands returns.
func ProcessCommands(preCommand func() error, args []string) {
	if len(args) < 1 {
		// Program will exit after printing usage.
		// Execution of usage is considered an error and
		// hence processCommands exits with ExitBadArgs.
		Usage()
	}

	// should be possible to run "command help" thus ignoring
	// any mandatory common options/flags
	if args[0] == "help" {
		// program will exit after printing help
		help(args[1:])
	}

	for _, cmd := range state.commands {
		if cmd.Name == args[0] {
			if preCommand != nil {
				handleExitError(preCommand())
			}
			handleExitError(cmd.Run(args[1:]))
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown command %q\n", args[0])
	os.Exit(ExitBadArgs)
}

// RegisterCommand is used by CLI app to register commands with cmdproc.
func RegisterCommand(c CommandInfo) {
	state.commands = append(state.commands, c)
}

// RegisterTemplateInfo is used to register CLI app specific template
// information with cmdproc.
func RegisterTemplateInfo(cinfo CommonUsageInfo, usage *template.Template,
	help *template.Template) {

	state.uti.CommonUsageInfo = cinfo
	state.usageTmpl = usage
	state.helpTmpl = help

	if state.uti.CommonUsageInfo.ExplainExitCodes == nil {
		state.uti.CommonUsageInfo.ExplainExitCodes = ExplainExitCodes
	}

	for _, cmd := range state.commands {
		cuinfo := CommandUsageInfo{
			Name:  cmd.Name,
			Usage: cmd.Usage,
			Short: cmd.Short,
		}
		state.uti.Commands = append(state.uti.Commands, cuinfo)
	}
}
