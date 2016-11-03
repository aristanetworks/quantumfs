// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// tests of qfs chroot tool
package main

import "io/ioutil"
import "os"
import "os/exec"
import "testing"

var commandsInUsrBin = []string{sudo, mount, umount, netns, netnsd, setarch,
	cp, chns, sh, bash, "/usr/bin/mkdir", "/usr/bin/ls"}

var commandsInUsrSbin = []string{pivot_root}

var libsToCopy = []string{
	"/usr/lib64/libSysPreloadUtils.so", "/usr/lib64/libtinfo.so.5",
	"/usr/lib64/libdl.so.2", "/usr/lib64/libc.so.6", "/usr/lib64/librt.so.1",
	"/usr/lib64/libstdc++.so.6", "/usr/lib64/libm.so.6",
	"/usr/lib64/libgcc_s.so.1", "/usr/lib64/libpcre.so.1",
	"/usr/lib64/ld-linux-x86-64.so.2", "/usr/lib64/libpthread.so.0",
	"/usr/lib64/libcap.so.2", "/usr/lib64/libacl.so.1",
	"/usr/lib64/libattr.so.1", "/usr/lib64/libselinux.so.1"}

func runCommand(t *testing.T, name string, args ...string) {
	cmd := exec.Command(name, args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatalf("Error getting stderr pipe of runCommand: %s\n"+
			"Command: %s %v",
			err.Error(), name, args)
	}

	if err := cmd.Start(); err != nil {
		t.Fatalf("Error starting process in runCommand: %s\nCommand: %s %v",
			err.Error(), name, args)
	}

	buf, err := ioutil.ReadAll(stderr)
	if err != nil {
		t.Fatalf("Error reading stderr in runCommand: %s\nCommand: %s %v",
			err.Error(), name, args)
	}

	if err := cmd.Wait(); err != nil {
		t.Fatalf("Error waiting process in runCommand: %s\n"+
			"Command: %s %v\nStderr: %s",
			err.Error(), name, args, string(buf))
	}
}

// setup a minimal workspace
func setupWorkspace(t *testing.T) string {
	dirTest, err := ioutil.TempDir("", "TestChrootH")
	if err != nil {
		t.Fatalf("Creating directory %s error: %s", dirTest,
			err.Error())
	}

	if err := os.Chmod(dirTest, 0666); err != nil {
		t.Fatalf("Changing mode of directory %s error: %s",
			dirTest, err.Error())
	}

	dirUsrBin := dirTest + "/usr/bin"
	if err := os.MkdirAll(dirUsrBin, 0666); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirUsrBin,
			err.Error())
	}

	for _, command := range commandsInUsrBin {
		runCommand(t, "cp", command, dirUsrBin)

	}

	dirUsrSbin := dirTest + "/usr/sbin"
	if err := os.MkdirAll(dirUsrSbin, 0666); err != nil {
		t.Fatalf("Creating directory %s error: %s",
			dirUsrSbin, err.Error())
	}

	for _, command := range commandsInUsrSbin {
		runCommand(t, "cp", command, dirUsrSbin)
	}

	dirUsrLib64 := dirTest + "/usr/lib64"
	if err := os.MkdirAll(dirUsrLib64, 0666); err != nil {
		t.Fatalf("Creating directory %s error: %s",
			dirUsrLib64, err.Error())

	}

	for _, lib := range libsToCopy {
		runCommand(t, "cp", lib, dirUsrLib64)
	}

	dirBin := dirTest + "/bin"
	runCommand(t, "ln", "-s", "usr/bin", dirBin)

	dirSbin := dirTest + "/sbin"
	runCommand(t, "ln", "-s", "usr/sbin", dirSbin)

	dirLib64 := dirTest + "/lib64"
	runCommand(t, "ln", "-s", "usr/lib64", dirLib64)

	dirUsrShare := dirTest + "/usr/share"
	if err := os.MkdirAll(dirUsrShare, 0666); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirUsrShare,
			err.Error())
	}

	dirUsrShareArtools := dirUsrShare + "/Artools"
	runCommand(t, "cp", "-ax", ArtoolsDir, dirUsrShareArtools)

	dirUsrMnt := dirTest + "/mnt"
	if err := os.Mkdir(dirUsrMnt, 0666); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirUsrMnt,
			err.Error())
	}

	dirEtc := dirTest + "/etc"
	if err := os.Mkdir(dirEtc, 0666); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirEtc, err.Error())
	}

	runCommand(t, "cp", "/etc/passwd", dirEtc+"/")

	dirTmp := dirTest + "/tmp"
	if err := os.Mkdir(dirTmp, 0666); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirTmp,
			err.Error())
	}

	dirCurrent := dirTest + dirTest
	if err := os.MkdirAll(dirCurrent, 0666); err != nil {
		t.Fatalf("Creating directory %s error: %s",
			dirCurrent, err.Error())
	}

	return dirTest
}

func cleanupWorkspace(workspace string, t *testing.T) {
	if err := os.RemoveAll(workspace); err != nil {
		t.Fatalf("Error cleanning up testing workspace: %s", err.Error())
	}
}

func terminateNetnsdServer(rootdir string, t *testing.T) {
	svrName := rootdir + "/chroot"

	if serverRunning(svrName) {
		runCommand(t, sudo, netns, "-k", svrName)
	}
}

func TestPersistentChroot(t *testing.T) {
	dirTest := setupWorkspace(t)

	defer cleanupWorkspace(dirTest, t)
	defer terminateNetnsdServer(dirTest, t)

	var fileTest string
	if fd, err := ioutil.TempFile(dirTest, "ChrootTestFile"); err != nil {
		t.Fatalf("Creating test file error: %s", err.Error())
	} else {
		fileTest = fd.Name()[len(dirTest):]
		fd.Close()
	}

	if err := os.Chdir(dirTest); err != nil {
		t.Fatal("Changing to directory %s error: %s", dirTest, err.Error())
	}

	cmdChroot := exec.Command("qfs", "chroot")

	stdin, err := cmdChroot.StdinPipe()
	if err != nil {
		t.Fatalf("Error getting stdin: %s", err.Error())
	}

	stderr, err := cmdChroot.StderrPipe()
	if err != nil {
		t.Fatalf("Error getting stderr: %s", err.Error())
	}

	if err := cmdChroot.Start(); err != nil {
		t.Fatalf("Executing error:%s", err.Error())
	}

	cmdFileTest := "cd /;cd ..;ls -l " + fileTest
	if _, err := stdin.Write([]byte(cmdFileTest)); err != nil {
		t.Fatalf("Error writing command: %s", err.Error())
	}

	if err := stdin.Close(); err != nil {
		t.Fatalf("Error closing command writer: %s",
			err.Error())
	}

	errInfo, err := ioutil.ReadAll(stderr)
	if err != nil {
		t.Fatalf("Error reading standard error: %s", err.Error())
	}

	if err := cmdChroot.Wait(); err != nil {
		t.Fatalf("Error waiting chroot command: %s \n"+
			"Error info: %s", err.Error(), string(errInfo))
	}
}

func TestNetnsPersistency(t *testing.T) {
	dirTest := setupWorkspace(t)

	defer cleanupWorkspace(dirTest, t)
	defer terminateNetnsdServer(dirTest, t)

	var fileTest string
	if fd, err := ioutil.TempFile(dirTest, "ChrootTestFile"); err != nil {
		t.Fatalf("Creating test file error: %s", err.Error())
	} else {
		fileTest = fd.Name()[len(dirTest):]
		fd.Close()
	}

	if err := os.Chdir(dirTest); err != nil {
		t.Fatal("Changing directory to %s error", dirTest)
	}

	cmdChroot := exec.Command("qfs", "chroot")

	stdin, err := cmdChroot.StdinPipe()
	if err != nil {
		t.Fatalf("Error getting stdin: %s", err.Error())
	}

	stderr, err := cmdChroot.StderrPipe()
	if err != nil {
		t.Fatalf("Error getting stderr: %s", err.Error())
	}

	if err := cmdChroot.Start(); err != nil {
		t.Fatalf("Executing error:%s", err.Error())
	}

	cmdExit := "exit"
	if _, err := stdin.Write([]byte(cmdExit)); err != nil {
		t.Fatalf("Error writing command %s\nError Info: %s",
			cmdExit, err.Error())
	}

	if err := stdin.Close(); err != nil {
		t.Fatalf("Error closing standard input: %s", err.Error())
	}

	errInfo, err := ioutil.ReadAll(stderr)
	if err != nil {
		t.Fatalf("Error reading standard error: %s", err.Error())
	}

	if err := cmdChroot.Wait(); err != nil {
		t.Fatalf("Error waiting chroot command: %s \n"+
			"Error info: %s", err.Error(), string(errInfo))
	}

	cmdNetnsLogin := exec.Command(netns, dirTest+"/chroot",
		sh, "-l", "-c", "$@", bash, bash)
	stdinNetnsLogin, err := cmdNetnsLogin.StdinPipe()
	if err != nil {
		t.Fatalf("Error getting stdinNetnsLogin: %s", err.Error())
	}

	if err := cmdNetnsLogin.Start(); err != nil {
		t.Fatalf("Error starting netnsLogin command: %s", err.Error())
	}

	cmdFileTest := "cd /; cd ..; ls -l " + fileTest
	if _, err := stdinNetnsLogin.Write([]byte(cmdFileTest)); err != nil {
		t.Fatalf("Error writting command: %s", err.Error())
	}

	if err := stdinNetnsLogin.Close(); err != nil {
		t.Fatalf("Error closing standarded input: %s", err.Error())
	}

	if err := cmdNetnsLogin.Wait(); err != nil {
		t.Fatalf("Error waiting netnsLogin command: %s", err.Error())
	}
}

// Here we can define several variants of each of the three arguments
// <WSR>:
// AbsWsr: Absolute path of workspaceroot in the filesystem before chroot
// RelWsr: Workspaceroot relative to the directory where chroot is run
// <DIR>:
// AbsDirBefore: Absolute path of working directory in the filesystem before chroot
// AbsDirAfter: Absolute path of working directory in the filesystem after chroot
// RelDir: Working directory path relative to the directory where chroot is run
// <CMD>:
// AbsCmd: Command with absolute path in the filesystem after chroot
// RelCmd: Command with path relative to working directory

func setupNonPersistentChrootTest(t *testing.T, rootTest string) (string, string) {
	dirTest := ""
	fileTest := ""

	if dir, err := ioutil.TempDir(rootTest, "ChrootTestDirectory"); err != nil {
		t.Fatalf("Creating test file error: %s", err.Error())
	} else {
		dirTest = dir
	}

	if fd, err := ioutil.TempFile(dirTest, "ChrootTestFile"); err != nil {
		t.Fatalf("Creating test file error: %s", err.Error())
	} else {
		fileTest = fd.Name()
		fd.Close()
	}

	return dirTest, fileTest
}

func TestNonPersistentChrootAbsWsrAbsDirBeforeAbsCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = fileTest[len(rootTest):]

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootAbsWsrAbsDirBeforeRelCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = "." + fileTest[len(dirTest):]

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootAbsWsrAbsDirAfterAbsCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = fileTest[len(rootTest):]
	dirTest = dirTest[len(rootTest):]

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootAbsWsrAbsDirAfterRelCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	fileTest = "." + fileTest[len(dirTest):]
	dirTest = dirTest[len(rootTest):]

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootAbsWsrRelDirAbsCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = fileTest[len(rootTest):]
	dirTest = "." + dirTest

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootAbsWsrRelDirRelCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = "." + fileTest[len(rootTest):]
	dirTest = "." + dirTest

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootRelWsrAbsDirBeforeAbsCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = fileTest[len(rootTest):]
	rootTest = "." + rootTest

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootRelWsrAbsDirBeforeRelCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = "." + fileTest[len(dirTest):]
	rootTest = "." + rootTest

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootRelWsrAbsDirAfterAbsCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = fileTest[len(rootTest):]
	dirTest = dirTest[len(rootTest):]
	rootTest = "." + rootTest

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootRelWsrAbsDirAfterRelCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = "." + fileTest[len(dirTest):]
	dirTest = dirTest[len(rootTest):]
	rootTest = "." + rootTest

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootRelWsrRelDirAbsCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = fileTest[len(rootTest):]
	dirTest = "." + dirTest
	rootTest = "." + rootTest

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}

func TestNonPersistentChrootRelWsrRelDirRelCmd(t *testing.T) {
	rootTest := setupWorkspace(t)

	defer cleanupWorkspace(rootTest, t)

	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = "." + fileTest[len(dirTest):]
	dirTest = "." + dirTest
	rootTest = "." + rootTest

	runCommand(t, "qfs", "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest)
}
