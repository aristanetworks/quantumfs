// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// qfs is a command line tool used to interact with quantumfs and perform various
// special operations not available through normal POSIX interfaces.
package main

import "bytes"
import "flag"
import "fmt"
import "os"
import "os/exec"

import "github.com/aristanetworks/quantumfs"

// Various exit reasons, will be returned to the shell as an exit code
const (
	exitOk      = iota
	exitBadCmd  = iota
	exitBadArgs = iota
)

func main() {
	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Println("usage: qfs [options] <command> [ARG1[,ARG2[,...]]]")
		flag.PrintDefaults()
		fmt.Println("Available commands:")
		fmt.Println("  branch <workspaceO> <workspaceN>")
		fmt.Println("         - create a new workspaceN which is a copy of" +
			" workspaceO")
		fmt.Println("           as of this point in time")
		fmt.Println("  chroot")
		fmt.Println("         - Run shell in the specified workspace tree, ")
		fmt.Println("           persistent session and not isolated from")
		fmt.Println("           the rest of the machine")
		os.Exit(exitBadCmd)
	}

	cmd := flag.Arg(0)
	switch cmd {
	default:
		fmt.Printf("Unknown command \"%s\"\n", cmd)
		os.Exit(exitBadCmd)

	case "branch":
		branch()
	case "chroot":
		chroot()
	}
}

// Implement the branch command, which takes a workspace at the current spot, and
// creates a new workspace with the given name which is identical to the first
// workspace as of right now.
func branch() {
	if flag.NArg() != 3 {
		fmt.Println("Too few arguments for branch command")
		os.Exit(exitBadArgs)
	}

	src := flag.Arg(1)
	dst := flag.Arg(2)

	fmt.Printf("Branching workspace \"%s\" into \"%s\"\n", src, dst)
	api := quantumfs.NewApi()
	err := api.Branch(src, dst)

	if err != nil {
		fmt.Println("Operations failed:", err)
		os.Exit(exitBadArgs)
	}
}

func chroot() error {
	//	if flag.NArg() != 1 {
	//		fmt.Println("Too many arguments for branch command")
	//		os.Exit(exitBadArgs)
	//	}

	//fmt.Printf("Branching workspace \"%s\" into \"%s\"\n", src, dst)
	//	api := quantumfs.NewApi()
	//	err := api.Chroot()

	//	if err != nil {
	//		fmt.Println("Operations failed:", err)
	//		os.Exit(exitBadArgs)
	//	}
	mp := "/home/bernardy"
	wsr := "b160310"

	dirstat, err := os.Stat(mp + "/" + wsr)
	if err != nil {
		fmt.Println("stat mp/wsr error")
		return err
	}
	rootstat, err := os.Stat("/")
	if err != nil {
		fmt.Println("stat root error")
		return err
	}

	if os.SameFile(dirstat, rootstat) {
		fmt.Println("Alreadly chrooted")
		return fmt.Errorf("Alreadly chrooted")
	}

	wsr = mp + "/" + wsr
	err = os.Mkdir(wsr+"/etc", 0666)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	copy := exec.Command("cp", "/etc/resolv.conf", wsr+"/etc/resolv.conf")
	err = copy.Run()
	if err != nil {
		if !os.IsExist(err) {
			fmt.Println("copy DNS error")
			fmt.Println(err.Error())
			return err
		}
	}

	copy = exec.Command("cp", "/etc/AroraKernel-compatibility",
		wsr+"/etc/AroraKernel-compatibility")
	err = copy.Run()
	if err != nil {
		if !os.IsExist(err) {
			fmt.Println("copy compatibility error")
			return err
		}
	}
	ns := exec.Command("netns", "-q", wsr)
	netnsd_running := true
	err = ns.Run()
	if err != nil {
		fmt.Println("1::not running")
		netnsd_running = false
	}

	if netnsd_running {
		ns = exec.Command("netns", wsr, "/bin/bash")
		err = ns.Run()
		if err != nil {
			fmt.Println("netns " + wsr + " /bin/bash Error")
		}
	}

	svrName := wsr + "/chroot"
	ns = exec.Command("netns", "-q", svrName)
	netnsd_running = true
	err = ns.Run()
	if err != nil {
		fmt.Println("2::not running")
		netnsd_running = false
	}

	if netnsd_running {
		netnsExec := exec.Command("/usr/bin/netns", svrName, "/bin/sh", "-l", "-c", "$@", "/bin/bash", "/bin/bash")
		var netnsExecBuf bytes.Buffer
		netnsExec.Stderr = &netnsExecBuf
		err = netnsExec.Run()
		if err != nil {
			fmt.Println("netns1 Login shell error")
			fmt.Println(netnsExecBuf.String())
			return err
		} else {
			return nil
		}
	}

	prechrootScript := "sudo mount --rbind " + wsr + " " + wsr + ";"
	dstdev := wsr + "/dev"
	err = os.Mkdir(dstdev, 0666)
	if err != nil {
		if !os.IsExist(err) {
			fmt.Println("make /dev error")
			return err
		}
	}
	prechrootScript = prechrootScript + "sudo mount -t tmpfs none " + dstdev + ";"
	prechrootScript = prechrootScript + "sudo cp -ax /dev/. " + dstdev + ";"

	dstdev = wsr + "/var/run/netns"
	_, err = os.Stat(dstdev)
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(dstdev, 0666)
			if err != nil {
				fmt.Print("Create dir " + dstdev + " Error")
			}
		}
	}
	prechrootScript = prechrootScript + "sudo mount -t tmpfs tmpfs " + dstdev + ";"

	archCommand := exec.Command("uname", "-m")
	var archBuf bytes.Buffer
	archCommand.Stdout = &archBuf
	err = archCommand.Run()
	if err != nil {
		fmt.Println("Get architecture Error")
	}
	archstr := archBuf.String()
	fmt.Println("archstr:", archstr)

	// a tmp mnt directory for mounting old root
	err = os.Mkdir(wsr+"/mnt", 0666)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}

	nsFlags := "m"
	fmt.Println("--chroot:", wsr)
	fmt.Println("--pre-chroot-cmd=:", prechrootScript)
	netnsdCmd := exec.Command("/usr/bin/setarch", "i686", "/usr/bin/netnsd", "-d",
		"--no-netns-env", "-f", nsFlags, "--chroot="+wsr,
		"--pre-chroot-cmd="+prechrootScript, svrName)
	var netnsdBuf bytes.Buffer
	netnsdCmd.Stderr = &netnsdBuf
	err = netnsdCmd.Run()
	if err != nil {
		fmt.Println(netnsdBuf.String())
		fmt.Println(err.Error())
		return err
	}

	netnsExec := exec.Command("/usr/bin/netns", svrName, "/bin/sh", "-l", "-c", "$@", "/bin/bash", "/bin/bash")
	netnsExec.Stderr = &netnsdBuf
	err = netnsExec.Run()
	if err != nil {
		fmt.Println("netns2 Login shell error")
		fmt.Println(netnsdBuf.String())
		return err
	}

	fmt.Println("chroot correct")

	return nil
}
