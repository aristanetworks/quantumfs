// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// tests of qfs chroot tool
package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
)

var commandsInUsrBin = []string{
	umount,
	"/usr/bin/setarch",
	sh,
	"/usr/bin/bash",
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

// A helper function to run command which gives better error information
func runCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)

	if buf, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("Error in runCommand: %s\n"+
			"Command: %s %v\n Output: %s",
			err.Error(), name, args, string(buf))
	}

	return nil
}

func runQfsChroot(wsr string, dir string, filename string) error {
	return runCommand(testqfs, "chroot", "arastra", wsr, dir, "ls", filename)
}

// setup a minimal workspace
func setupWorkspace(t *testing.T) string {
	dirTest := testutils.SetupTestspace("TestChroot")

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

	if err := runQfsChroot(rootTest, dirTest, fileTest); err != nil {
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

	if err := runQfsChroot(rootTest, dirTest, fileTest); err != nil {
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

	if err := runQfsChroot(rootTest, dirTest, fileTest); err != nil {
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

	if err := runQfsChroot(rootTest, dirTest, fileTest); err != nil {
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

	if err := runQfsChroot(rootTest, dirTest, fileTest); err != nil {
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

	if err := runQfsChroot(rootTest, dirTest, fileTest); err != nil {
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

	if err := runQfsChroot(rootTest, dirTest, fileTest); err != nil {
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

	if err := runQfsChroot(rootTest, dirTest, fileTest); err != nil {
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

func TestCopyDirStayOnFs(t *testing.T) {
	err := checkCopyDirStayOnFs(true)
	utils.Assert(err == nil, "Unable to copy source dir %s", err)
}

func TestCopyDirStayOnFsToFail(t *testing.T) {
	err := checkCopyDirStayOnFs(false)
	utils.Assert(err != nil, "Test not checking race condition")
}

func checkCopyDirStayOnFs(ignoreFails bool) error {
	src, err := ioutil.TempDir("", "SourceDir")
	utils.Assert(err == nil, "Unable to create source dir")

	dst, err := ioutil.TempDir("", "DestDir")
	utils.Assert(err == nil, "Unable to create destination dir")

	// Create enough directories to make copyDirStayOnFs take a while
	dirs := 1000
	for i := 0; i < dirs; i++ {
		err = os.MkdirAll(fmt.Sprintf(src+"/dir%d", i), 0777)
		utils.Assert(err == nil, "Unable to create directory %d", i)
	}

	// make a bunch of file system removals in parallel
	var perr error
	go func() {
		for i := 0; i < dirs; i += 10 {
			perr = os.Remove(fmt.Sprintf(src+"/dir%d", i))
			if perr != nil {
				return
			}

			time.Sleep(time.Millisecond)
		}
	}()

	err = copyDirStayOnFs(src, dst, ignoreFails)

	utils.Assert(perr == nil, "Unable to remove source dirs")

	return err
}
