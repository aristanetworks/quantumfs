// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// tests of qfs chroot tool
package main

import "fmt"
import "io/ioutil"
import "os"
import "os/exec"
import "runtime"
import "strings"
import "syscall"
import "testing"
import "time"

import "github.com/aristanetworks/quantumfs/utils"

var commandsInUsrBin = []string{
	umount,
	setarch,
	sh,
	bash,
	"/usr/bin/ls",
}

var libsToCopy map[string]bool

var testqfs string

func init() {
	testqfs = os.Getenv("GOPATH") + "/bin/qfs"

	libsToCopy = make(map[string]bool)
	libsToCopy["/usr/lib64/ld-linux-x86-64.so.2"] = true

	for _, binary := range commandsInUsrBin {
		ldd := exec.Command("ldd", binary)
		output, err := ldd.CombinedOutput()
		if err != nil {
			fmt.Printf("Failed to get libraries for binary %s: %v\n",
				binary, err)
			continue
		}

		lines := strings.Split(string(output), "\n")

		for _, line := range lines {
			if !strings.Contains(line, "=>") ||
				!strings.Contains(line, "/lib") {

				// This line doesn't contain a library we can copy
				continue
			}

			tokens := strings.Split(line, " ")
			library := tokens[2]
			libsToCopy[library] = true
		}
	}
}

// setup a minimal workspace
func setupWorkspace(t *testing.T) string {
	dirTest, err := utils.SetupTestspace(1, "TestChroot")
	if err != nil {
		t.Fatalf("Setting up testing environment of %s error: %s", dirTest,
			err.Error())
	}

	dirUsrBin := dirTest + "/usr/bin"
	if err := utils.MkdirAll(dirUsrBin, 0777); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirUsrBin,
			err.Error())
	}

	for _, command := range commandsInUsrBin {
		if err := runCommand("cp", command, dirUsrBin); err != nil {
			t.Fatal(err.Error())
		}
	}

	dirUsrSbin := dirTest + "/usr/sbin"
	if err := utils.MkdirAll(dirUsrSbin, 0777); err != nil {
		t.Fatalf("Creating directory %s error: %s",
			dirUsrSbin, err.Error())
	}

	dirUsrLib64 := dirTest + "/usr/lib64"
	if err := utils.MkdirAll(dirUsrLib64, 0777); err != nil {
		t.Fatalf("Creating directory %s error: %s",
			dirUsrLib64, err.Error())

	}

	for lib := range libsToCopy {
		if err := runCommand("cp", lib, dirUsrLib64); err != nil {
			t.Fatal(err.Error())
		}
	}

	dirBin := dirTest + "/bin"
	if err := syscall.Symlink("usr/bin", dirBin); err != nil {
		t.Fatal("Creating symlink usr/bin error: " + err.Error())
	}

	dirSbin := dirTest + "/sbin"
	if err := syscall.Symlink("usr/sbin", dirSbin); err != nil {
		t.Fatal(err.Error())
	}

	dirLib64 := dirTest + "/lib64"
	if err := syscall.Symlink("usr/lib64", dirLib64); err != nil {
		t.Fatal(err.Error())
	}

	dirUsrShare := dirTest + "/usr/share"
	if err := utils.MkdirAll(dirUsrShare, 0777); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirUsrShare,
			err.Error())
	}

	dirUsrShareArtools := dirUsrShare + "/Artools"
	if err := runCommand("cp", "-ax", ArtoolsDir,
		dirUsrShareArtools); err != nil {

		t.Fatal(err.Error())
	}

	dirUsrMnt := dirTest + "/mnt"
	if err := syscall.Mkdir(dirUsrMnt, 0777); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirUsrMnt,
			err.Error())
	}

	dirEtc := dirTest + "/etc"
	if err := syscall.Mkdir(dirEtc, 0777); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirEtc, err.Error())
	}

	if err := runCommand("cp", "/etc/passwd", dirEtc); err != nil {
		t.Fatal(err.Error())
	}

	dirTmp := dirTest + "/tmp"
	if err := syscall.Mkdir(dirTmp, 0777); err != nil {
		t.Fatalf("Creating directory %s error: %s", dirTmp,
			err.Error())
	}

	return dirTest
}

func cleanupWorkspace(workspace string, t *testing.T) {
	var err error

	for i := 0; i < 10; i++ {
		if err = os.RemoveAll(workspace); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if err != nil {
		t.Fatalf("Error cleaning up testing workspace: %s", err.Error())
	}
}

func terminateNetnsdServer(rootdir string, t *testing.T) {
	svrName := rootdir + "/chroot"

	if serverRunning(svrName) {
		if err := runCommand(sudo, netns, "-k", svrName); err != nil {
			t.Fatal(err.Error())
		}
		// Wait for the chroot to no longer be used
		time.Sleep(100 * time.Millisecond)
	}
}

// Change the UID/GID the test thread to the given values. Use -1 not to change
// either the UID or GID.
func setUidGid(uid int, gid int, t *testing.T) {
	// The quantumfs tests are run as root because some tests require
	// root privileges. However, root can read or write any file
	// irrespective of the file permissions. Obviously if we want to
	// test permissions then we cannot run as root.
	//
	// To accomplish this we lock this goroutine to a particular OS
	// thread, then we change the EUID of that thread to something which
	// isn't root. Finally at the end we need to restore the EUID of the
	// thread before unlocking ourselves from that thread. If we do not
	// follow this precise cleanup order other tests or goroutines may
	// run using the other UID incorrectly.
	runtime.LockOSThread()
	if gid != -1 {
		err := syscall.Setregid(-1, gid)
		if err != nil {
			runtime.UnlockOSThread()
			t.Fatal(err.Error())
		}
	}

	if uid != -1 {
		err := syscall.Setreuid(-1, uid)
		if err != nil {
			syscall.Setregid(-1, 0)
			runtime.UnlockOSThread()
			t.Fatal(err.Error())
		}
	}

}

// Set the UID and GID back to the defaults
func setUidGidToDefault(t *testing.T) {
	defer runtime.UnlockOSThread()

	// Test always runs as root, so its euid and egid is 0
	err1 := syscall.Setreuid(-1, 0)
	err2 := syscall.Setregid(-1, 0)
	if err1 != nil {
		t.Fatal(err1.Error())
	}
	if err2 != nil {
		t.Fatal(err2.Error())
	}
}

func testPersistentChroot(t *testing.T, dirTest string) {
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

	cmdChroot := exec.Command(testqfs, "chroot")

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

func TestPersistentChroot(t *testing.T) {
	cleanup := func(t *testing.T, dirTest string) {
		terminateNetnsdServer(dirTest, t)
		cleanupWorkspace(dirTest, t)
	}

	func() {
		dirTest := setupWorkspace(t)

		defer cleanup(t, dirTest)

		testPersistentChroot(t, dirTest)
	}()

	func() {
		dirTest := setupWorkspace(t)

		defer cleanup(t, dirTest)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testPersistentChroot(t, dirTest)
	}()
}

func testNetnsPersistency(t *testing.T, dirTest string) {
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

	cmdChroot := exec.Command(testqfs, "chroot")

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

func TestNetnsPersistency(t *testing.T) {
	cleanup := func(t *testing.T, dirTest string) {
		terminateNetnsdServer(dirTest, t)
		cleanupWorkspace(dirTest, t)
	}

	func() {
		dirTest := setupWorkspace(t)
		defer cleanup(t, dirTest)

		testNetnsPersistency(t, dirTest)
	}()

	func() {
		dirTest := setupWorkspace(t)
		defer cleanup(t, dirTest)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNetnsPersistency(t, dirTest)
	}()
}

// Here we can define several variants of each of the following arguments
// <WSR>:
// AbsWsr: Absolute path of workspaceroot in the filesystem before chroot
// RelWsr: Workspaceroot relative to the directory where chroot is run
// <DIR>:
// AbsDir: Absolute path of working directory in filesystem after chroot
// RelDir: Working directory path relative to workspaceroot
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

	if err := os.Chmod(dirTest, 0777); err != nil {
		t.Fatalf("Changing mode of directory: %s error: %s",
			dirTest, err.Error())
	}

	if fd, err := ioutil.TempFile(dirTest, "ChrootTestFile"); err != nil {
		t.Fatalf("Creating test file error: %s", err.Error())
	} else {
		fileTest = fd.Name()
		fd.Close()
	}

	if err := os.Chmod(fileTest, 0777); err != nil {
		t.Fatalf("Changing mode of file: %s error: %s",
			fileTest, err.Error())
	}

	return dirTest, fileTest
}

func testNonPersistentChrootAbsWsrAbsDirAbsCmd(t *testing.T, rootTest string) {
	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	fileTest = fileTest[len(rootTest):]
	dirTest = dirTest[len(rootTest):]

	if err := runCommand(testqfs, "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest); err != nil {

		t.Fatal(err.Error())
	}
}

func TestNonPersistentChrootAbsWsrAbsDirAbsCmd(t *testing.T) {
	func() {
		rootTest := setupWorkspace(t)
		defer cleanupWorkspace(rootTest, t)

		testNonPersistentChrootAbsWsrAbsDirAbsCmd(t, rootTest)
	}()

	func() {
		rootTest := setupWorkspace(t)

		defer cleanupWorkspace(rootTest, t)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNonPersistentChrootAbsWsrAbsDirAbsCmd(t, rootTest)
	}()
}

func testNonPersistentChrootAbsWsrAbsDirRelCmd(t *testing.T, rootTest string) {
	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	fileTest = "." + fileTest[len(dirTest):]
	dirTest = dirTest[len(rootTest):]

	if err := runCommand(testqfs, "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest); err != nil {

		t.Fatal(err.Error())
	}
}

func TestNonPersistentChrootAbsWsrAbsDirRelCmd(t *testing.T) {
	func() {
		rootTest := setupWorkspace(t)
		defer cleanupWorkspace(rootTest, t)

		testNonPersistentChrootAbsWsrAbsDirRelCmd(t, rootTest)
	}()

	func() {
		rootTest := setupWorkspace(t)

		defer cleanupWorkspace(rootTest, t)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNonPersistentChrootAbsWsrAbsDirRelCmd(t, rootTest)
	}()
}

func testNonPersistentChrootRelWsrAbsDirAbsCmd(t *testing.T, rootTest string) {
	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = fileTest[len(rootTest):]
	dirTest = dirTest[len(rootTest):]
	rootTest = "." + rootTest

	if err := runCommand(testqfs, "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest); err != nil {

		t.Fatal(err.Error())
	}
}

func TestNonPersistentChrootRelWsrAbsDirAbsCmd(t *testing.T) {
	func() {
		rootTest := setupWorkspace(t)
		defer cleanupWorkspace(rootTest, t)

		testNonPersistentChrootRelWsrAbsDirAbsCmd(t, rootTest)
	}()

	func() {
		rootTest := setupWorkspace(t)

		defer cleanupWorkspace(rootTest, t)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNonPersistentChrootRelWsrAbsDirAbsCmd(t, rootTest)
	}()
}

func testNonPersistentChrootRelWsrAbsDirRelCmd(t *testing.T, rootTest string) {
	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = "." + fileTest[len(dirTest):]
	dirTest = dirTest[len(rootTest):]
	rootTest = "." + rootTest

	if err := runCommand(testqfs, "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest); err != nil {

		t.Fatal(err.Error())
	}
}

func TestNonPersistentChrootRelWsrAbsDirRelCmd(t *testing.T) {
	func() {
		rootTest := setupWorkspace(t)
		defer cleanupWorkspace(rootTest, t)

		testNonPersistentChrootRelWsrAbsDirRelCmd(t, rootTest)
	}()

	func() {
		rootTest := setupWorkspace(t)

		defer cleanupWorkspace(rootTest, t)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNonPersistentChrootRelWsrAbsDirRelCmd(t, rootTest)
	}()
}

func testNonPersistentChrootAbsWsrRelDirAbsCmd(t *testing.T, rootTest string) {
	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	fileTest = fileTest[len(rootTest):]
	dirTest = dirTest[len(rootTest)+1:]

	if err := runCommand(testqfs, "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest); err != nil {

		t.Fatal(err.Error())
	}
}

func TestNonPersistentChrootAbsWsrRelDirAbsCmd(t *testing.T) {
	func() {
		rootTest := setupWorkspace(t)
		defer cleanupWorkspace(rootTest, t)

		testNonPersistentChrootAbsWsrRelDirAbsCmd(t, rootTest)
	}()

	func() {
		rootTest := setupWorkspace(t)

		defer cleanupWorkspace(rootTest, t)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNonPersistentChrootAbsWsrRelDirAbsCmd(t, rootTest)
	}()
}

func testNonPersistentChrootAbsWsrRelDirRelCmd(t *testing.T, rootTest string) {
	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	fileTest = "." + fileTest[len(dirTest):]
	dirTest = dirTest[len(rootTest)+1:]

	if err := runCommand(testqfs, "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest); err != nil {

		t.Fatal(err.Error())
	}
}

func TestNonPersistentChrootAbsWsrRelDirRelCmd(t *testing.T) {
	func() {
		rootTest := setupWorkspace(t)
		defer cleanupWorkspace(rootTest, t)

		testNonPersistentChrootAbsWsrRelDirRelCmd(t, rootTest)
	}()

	func() {
		rootTest := setupWorkspace(t)

		defer cleanupWorkspace(rootTest, t)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNonPersistentChrootAbsWsrRelDirRelCmd(t, rootTest)

	}()
}

func testNonPersistentChrootRelWsrRelDirAbsCmd(t *testing.T, rootTest string) {
	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = fileTest[len(rootTest):]
	dirTest = dirTest[len(rootTest)+1:]
	rootTest = "." + rootTest

	if err := runCommand(testqfs, "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest); err != nil {

		t.Fatal(err.Error())
	}
}

func TestNonPersistentChrootRelWsrRelDirAbsCmd(t *testing.T) {
	func() {
		rootTest := setupWorkspace(t)
		defer cleanupWorkspace(rootTest, t)

		testNonPersistentChrootRelWsrRelDirAbsCmd(t, rootTest)
	}()

	func() {
		rootTest := setupWorkspace(t)

		defer cleanupWorkspace(rootTest, t)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNonPersistentChrootRelWsrRelDirAbsCmd(t, rootTest)

	}()
}

func testNonPersistentChrootRelWsrRelDirRelCmd(t *testing.T, rootTest string) {
	dirTest, fileTest := setupNonPersistentChrootTest(t, rootTest)

	if err := os.Chdir("/"); err != nil {
		t.Fatalf("Changing to directory / error: %s",
			err.Error())
	}

	fileTest = "." + fileTest[len(dirTest):]
	dirTest = dirTest[len(rootTest)+1:]
	rootTest = "." + rootTest

	if err := runCommand(testqfs, "chroot", "--nonpersistent", rootTest,
		dirTest, "ls", fileTest); err != nil {

		t.Fatal(err.Error())
	}
}

func TestNonPersistentChrootRelWsrRelDirRelCmd(t *testing.T) {
	func() {
		rootTest := setupWorkspace(t)
		defer cleanupWorkspace(rootTest, t)

		testNonPersistentChrootRelWsrRelDirRelCmd(t, rootTest)
	}()

	func() {
		rootTest := setupWorkspace(t)

		defer cleanupWorkspace(rootTest, t)

		setUidGid(99, 99, t)
		defer setUidGidToDefault(t)
		testNonPersistentChrootRelWsrRelDirRelCmd(t, rootTest)

	}()
}
