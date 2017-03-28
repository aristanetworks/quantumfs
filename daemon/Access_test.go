// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

// Test that Access works in a variety of scenarios
import "errors"
import "os"
import "sync"
import "syscall"
import "testing"

import "github.com/aristanetworks/quantumfs/testutils"

func permTest(test *testHelper, filename string, modeCheck uint32,
	shouldPass bool) error {

	err := syscall.Access(filename + "garbage", F_OK)
	if err == nil {
		return errors.New("Access on invalid file passes")
	}

	err = syscall.Access(filename, modeCheck)
	if (err != nil) && shouldPass {
		return errors.New("Access fails with permission")
	} else if (err == nil) && !shouldPass {
		return errors.New("Access doesn't fail when missing permission")
	}

	return nil
}

// Access uses the real uid, so we can't use setregid the way we normally do.
// Since we can't re-get sudo after changing the real uid, we have to run this check
// in a go routine that we throw away.
func permTestSub(test *testHelper, filename string, modeCheck uint32,
	shouldPass bool, asRoot bool, wg *sync.WaitGroup, err *error) {

	defer syscall.Setreuid(0, -1)
	defer wg.Done()

	if !asRoot {
		*err = syscall.Setreuid(99, -1)
		if *err != nil {
			return
		}
	}

	*err = permTest(test, filename, modeCheck, shouldPass)
}

func accessTest(test *testHelper, filename string, shouldPass bool, asRoot bool) {
	var err error

	wg := sync.WaitGroup{}
	wg.Add(1)
	syscall.Chmod(filename, 0000)
	// This should always fail, unless you're root
	go permTestSub(test, filename, 0000, asRoot, asRoot, &wg, &err)
	wg.Wait()
	test.AssertNoErr(err)

	wg = sync.WaitGroup{}
	wg.Add(1)
	syscall.Chmod(filename, 0004)
	go permTestSub(test, filename, 0004, shouldPass, asRoot, &wg, &err)
	wg.Wait()
	test.AssertNoErr(err)

	wg = sync.WaitGroup{}
	wg.Add(1)
	syscall.Chmod(filename, 0002)
	go permTestSub(test, filename, 0002, shouldPass, asRoot, &wg, &err)
	wg.Wait()
	test.AssertNoErr(err)

	wg = sync.WaitGroup{}
	wg.Add(1)
	syscall.Chmod(filename, 0001)
	go permTestSub(test, filename, 0001, shouldPass, asRoot, &wg, &err)
	wg.Wait()
	test.AssertNoErr(err)
}

func accessTestBothUsers(test *testHelper, filename string) {

	err := os.Chown(filename, 100, 100)
	test.AssertNoErr(err)

	// first run the test as root
	accessTest(test, filename, true, true)

	// Now run it as a different user who may not have access
	accessTest(test, filename, false, false)

	// Now run it as the user who has access
	err = os.Chown(filename, 0, 99)
	test.AssertNoErr(err)
	accessTest(test, filename, true, false)
}

func TestAccessListFileInWsr(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		filename := workspace +"/testFile"
		err := testutils.PrintToFile(filename, string(genData(1000)))
		test.AssertNoErr(err)

		accessTestBothUsers(test, filename)
	})
}

func TestAccessListFileSubdir(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/subdir", 0777)

		filename := workspace +"/subdir/testFile"
		err := testutils.PrintToFile(filename, string(genData(1000)))
		test.AssertNoErr(err)

		accessTestBothUsers(test, filename)
	})
}

func TestAccessListHardlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/subdir/subsubdir", 0777)

		filename := workspace +"/subdir/testFile"
		err := testutils.PrintToFile(filename, string(genData(1000)))
		test.AssertNoErr(err)

		linkname := workspace +"/subdir/subsubdir/linkFile"
		err = syscall.Link(filename, linkname)

		accessTestBothUsers(test, linkname)
		accessTestBothUsers(test, filename)
	})
}

func TestAccessSymlink(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/subdir/subsubdir", 0777)

		filename := workspace +"/subdir/testFile"
		err := testutils.PrintToFile(filename, string(genData(1000)))
		test.AssertNoErr(err)

		linkname := workspace +"/subdir/subsubdir/linkFile"
		err = syscall.Symlink(filename, linkname)

		accessTestBothUsers(test, linkname)
		accessTestBothUsers(test, filename)
	})
}

func TestAccessDirectory(t *testing.T) {
	runTest(t, func(test *testHelper) {
		workspace := test.newWorkspace()
		os.MkdirAll(workspace+"/subdir", 0777)

		accessTestBothUsers(test, workspace+"/subdir")
	})
}
