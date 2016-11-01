// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// tests of qfs chroot tool
package main

import "fmt"
import "io/ioutil"
import "os"
import "os/exec"
import "testing"

// setup a minimal workspace
func setupWorkspace() (string, error) {
	dirTest, err := ioutil.TempDir("/tmp", "TestChrootT")
	if err != nil {
		return "", fmt.Errorf("Creating directory %s error: %s", dirTest,
			err.Error())
	}

	if err := os.Chmod(dirTest, 0777); err != nil {
		return "", fmt.Errorf("Changing mode of directory %s error: %s",
			dirTest, err.Error())
	}

	dirUsr := dirTest + "/usr"
	if err := os.Mkdir(dirUsr, 0777); err != nil {
		return "", fmt.Errorf("Creating directory %s error: %s", dirUsr,
			err.Error())
	}

	dirUsrBin := dirUsr + "/bin"
	cmdCopyUsrBin := exec.Command("cp", "-ax", "/usr/bin/.", dirUsrBin)
	if err := cmdCopyUsrBin.Run(); err != nil {
		return "", fmt.Errorf("Copying contents of directory %s error: %s",
			dirUsrBin, err.Error())
	}

	dirUsrSbin := dirUsr + "/sbin"
	cmdCopyUsrSbin := exec.Command("cp", "-ax", "/usr/sbin/.", dirUsrSbin)
	if err := cmdCopyUsrSbin.Run(); err != nil {
		return "", fmt.Errorf("Copying contents of directory %s error: %s",
			dirUsrSbin, err.Error())
	}

	dirUsrLib := dirUsr + "/lib"
	cmdCopyUsrLib := exec.Command("cp", "-ax", "/usr/lib/.", dirUsrLib)
	if err := cmdCopyUsrLib.Run(); err != nil {
		return "", fmt.Errorf("Copying contents of directory %s error: %s",
			dirUsrLib, err.Error())
	}

	dirUsrLib64 := dirUsr + "/lib64"
	cmdCopyUsrLib64 := exec.Command("cp", "-ax", "/usr/lib64/.", dirUsrLib64)
	if err := cmdCopyUsrLib64.Run(); err != nil {
		return "", fmt.Errorf("Copying contents of directory %s error: %s",
			dirUsrLib64, err.Error())
	}

	dirBin := dirTest + "/bin"
	cmdSymlinkBin := exec.Command("ln", "-s", "usr/bin", dirBin)
	if err := cmdSymlinkBin.Run(); err != nil {
		return "", fmt.Errorf("Creating symlink %s error: %s", dirBin,
			err.Error())
	}

	dirSbin := dirTest + "/sbin"
	cmdSymlinkSbin := exec.Command("ln", "-s", "usr/sbin", dirSbin)
	if err := cmdSymlinkSbin.Run(); err != nil {
		return "", fmt.Errorf("Creating symlink %s error: %s", dirSbin,
			err.Error())
	}

	dirLib := dirTest + "/lib"
	cmdSymlinkLib := exec.Command("ln", "-s", "usr/lib", dirLib)
	if err := cmdSymlinkLib.Run(); err != nil {
		return "", fmt.Errorf("Creating symlink %s error: %s", dirLib,
			err.Error())
	}

	dirLib64 := dirTest + "/lib64"
	cmdSymlinkLib64 := exec.Command("ln", "-s", "usr/lib64", dirLib64)
	if err := cmdSymlinkLib64.Run(); err != nil {
		return "", fmt.Errorf("Creating symlink %s error: %s", dirLib64,
			err.Error())
	}

	dirUsrShare := dirUsr + "/share"
	if err := os.MkdirAll(dirUsrShare, 0777); err != nil {
		return "", fmt.Errorf("Creat directory %s error: %s", dirUsrShare,
			err.Error())
	}

	dirUsrShareArtools := dirUsrShare + "/Artools"
	cmdCopyArtools := exec.Command("cp", "-ax", "/usr/share/Artools/.",
		dirUsrShareArtools)
	if err := cmdCopyArtools.Run(); err != nil {
		return "", fmt.Errorf("Copying contents of directory %s error: %s",
			dirUsrShareArtools, err.Error())
	}

	dirUsrMnt := dirTest + "/mnt"
	if err := os.Mkdir(dirUsrMnt, 0777); err != nil {
		return "", fmt.Errorf("Creating directory %s error: %s", dirUsrMnt,
			err.Error())
	}

	dirEtc := dirTest + "/etc"
	cmdCopyEtc := exec.Command("cp", "-ax", "/etc/.", dirEtc)
	if err := cmdCopyEtc.Run(); err != nil {
		return "", fmt.Errorf("Copying contents of directory %s error: %s",
			dirEtc, err.Error())
	}

	dirTmp := dirTest + "/tmp"
	if err := os.Mkdir(dirTmp, 0777); err != nil {
		return "", fmt.Errorf("Creating directory %s error: %s", dirTmp,
			err.Error())
	}

	dirCurrent := dirTest + dirTest
	if err := os.MkdirAll(dirCurrent, 0777); err != nil {
		return "", fmt.Errorf("Creating directory %s error: %s",
			dirCurrent, err.Error())
	}

	return dirTest, nil
}

func cleanupWorkspace(workspace string, t *testing.T) {
	if err := os.RemoveAll(workspace); err != nil {
		t.Error("Error cleanning up testing workspace: %s", err.Error())
	}
}

func TestPersistentChroot(t *testing.T) {
	dirTest, err := setupWorkspace()
	if err != nil {
		t.Error(err.Error())
	}

	defer cleanupWorkspace(dirTest, t)

	if err := os.Chdir(dirTest); err != nil {
		t.Errorf("Changing to directory %s error: %s", dirTest, err.Error())
	}

	var fileTest string
	if fd, err := ioutil.TempFile(dirTest, "ChrootTestFile"); err != nil {
		t.Errorf("Creating test file error: %s", err.Error())
	} else {
		fileTest = fd.Name()[len(dirTest):]
		fd.Close()
	}

	cmdChroot := exec.Command("qfs", "chroot")

	stdin, err := cmdChroot.StdinPipe()
	if err != nil {
		t.Errorf("Error getting stdin: %s", err.Error())
	}

	stdout, err := cmdChroot.StdoutPipe()
	if err != nil {
		t.Errorf("Error getting stdout: %s", err.Error())
	}

	cmdFileTest := "cd /;cd ..;ls -l " + fileTest
	if _, err := stdin.Write([]byte(cmdFileTest)); err != nil {
		t.Errorf("Error writing command: %s", err.Error())
	}

	if err := cmdChroot.Start(); err != nil {
		t.Errorf("Executing error:%s", err.Error())
	}

	if err := stdin.Close(); err != nil {
		t.Errorf("Error closing standard input: %s", err.Error())
	}

	if _, err := ioutil.ReadAll(stdout); err != nil {
		t.Errorf("Error reading standard output: %s", err.Error())
	}

	if err := cmdChroot.Wait(); err != nil {
		t.Errorf("%s\n", err.Error())
	}
}

func TestNetnsPersistency(t *testing.T) {
	dirTest, err := setupWorkspace()
	if err != nil {
		t.Error(err.Error())
	}

	defer cleanupWorkspace(dirTest, t)

	if err := os.Chdir(dirTest); err != nil {
		t.Errorf("Changing to directory %s error: %s", dirTest, err.Error())
	}

	var fileTest string
	if fd, err := ioutil.TempFile(dirTest, "ChrootTestFile"); err != nil {
		t.Errorf("Creating test file error: %s", err.Error())
	} else {
		fileTest = fd.Name()[len(dirTest):]
		fd.Close()
	}

	cmdChroot := exec.Command("qfs", "chroot")

	stdin, err := cmdChroot.StdinPipe()
	if err != nil {
		t.Errorf("Error getting stdin: %s", err.Error())
	}

	cmdExit := "exit"
	if _, err := stdin.Write([]byte(cmdExit)); err != nil {
		t.Errorf("Error writing command: %s", err.Error())
	}

	if err := cmdChroot.Start(); err != nil {
		t.Errorf("Executing error:%s", err.Error())
	}

	if err := stdin.Close(); err != nil {
		t.Errorf("Error closing standard input: %s", err.Error())
	}

	if err := cmdChroot.Wait(); err != nil {
		t.Errorf("Error waiting chroot command: %s\n", err.Error())
	}

	cmdNetnsLogin := exec.Command(netns, dirTest+"/chroot",
		sh, "-l", "-c", "$@", bash, bash)
	stdin, err = cmdNetnsLogin.StdinPipe()
	if err != nil {
		t.Errorf("Error getting stdin: %s", err.Error())
	}

	cmdFileTest := "cd /; cd ..; ls -l " + fileTest
	if _, err := stdin.Write([]byte(cmdFileTest)); err != nil {
		t.Errorf("Error writting command: %s", err.Error())
	}

	if err := cmdNetnsLogin.Start(); err != nil {
		t.Errorf("Error starting netnsLogin command: %s", err.Error())
	}

	if err := stdin.Close(); err != nil {
		t.Errorf("Error closing standarded input: %s", err.Error())
	}

	if err := cmdNetnsLogin.Wait(); err != nil {
		t.Errorf("Error waiting netnsLogin command: %s", err.Error())
	}
}

func TestNonPersistentChroot(t *testing.T) {
	dirTest, err := setupWorkspace()
	if err != nil {
		t.Error(err.Error())
	}

	defer cleanupWorkspace(dirTest, t)

	var fileTest string
	if fd, err := ioutil.TempFile(dirTest, "ChrootTestFile"); err != nil {
		t.Errorf("Creating test file error: %s", err.Error())
	} else {
		fileTest = fd.Name()[len(dirTest):]
		fd.Close()
	}

	cmdChroot := exec.Command("qfs", "chroot", "--nonpersistent", dirTest,
		"ls", fileTest)

	stdout, err := cmdChroot.StdoutPipe()
	if err != nil {
		t.Errorf("Error getting stdout: %s", err.Error())
	}

	if err := cmdChroot.Start(); err != nil {
		t.Errorf("Chroot starting error:%s", err.Error())
	}

	if _, err := ioutil.ReadAll(stdout); err != nil {
		t.Errorf("Error reading standard output: %s", err.Error())
	}

	if err := cmdChroot.Wait(); err != nil {
		t.Errorf("Chroot waiting error:%s", err.Error())
	}

}
