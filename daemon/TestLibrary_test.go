// Copyright (c) 2017 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package daemon

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/processlocal"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/testutils"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

func TestMain(m *testing.M) {
	flag.Parse()

	// Precompute a bunch of our GenData to save time during tests
	GenData(40 * 1024 * 1024)

	PreTestRuns()
	result := m.Run()
	PostTestRuns()

	os.Exit(result)
}

func TestRandomNamespaceName(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		name1 := randomNamespaceName(8)
		name2 := randomNamespaceName(8)
		name3 := randomNamespaceName(10)

		test.Assert(len(name1) == 8, "name1 wrong length: %d", len(name1))
		test.Assert(name1 != name2, "name1 == name2: '%s'", name1)
		test.Assert(len(name3) == 10, "name3 wrong length: %d", len(name1))
	})
}

// If a test never returns from some event, such as an infinite loop, the test
// should timeout and cleanup after itself.
func TestTimeout(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.ShouldFail = true
		time.Sleep(60 * time.Second)

		// If we get here then the test library didn't time us out and we
		// should fail this test.
		test.ShouldFail = false
		test.Assert(false, "Test didn't fail due to timeout")
	})
}

func TestGenData(t *testing.T) {
	runTestNoQfs(t, func(test *testHelper) {
		hardcoded := "012345678910111213141516171819202122232425262"
		data := GenData(len(hardcoded))

		test.Assert(bytes.Equal([]byte(hardcoded), data),
			"Data gen function off: %s vs %s", hardcoded, data)
	})
}

// If a quantumfs test fails then it may leave the filesystem mount hanging around in
// a blocked state. testHelper needs to forcefully abort and umount these to keep the
// system functional. Test this forceful unmounting here.
func TestPanicFilesystemAbort(t *testing.T) {
	runTest(t, func(test *testHelper) {
		test.ShouldFailLogscan = true

		api := test.getApi()

		// Introduce a panicing error into quantumfs
		test.qfs.mapMutex.Lock()
		for k, v := range test.qfs.fileHandles {
			test.qfs.fileHandles[k] = &crashOnWrite{FileHandle: v}
		}
		test.qfs.mapMutex.Unlock()

		// panic Quantumfs
		api.Branch(quantumfs.NullSpaceName+"/"+quantumfs.NullSpaceName+"/"+
			quantumfs.NullSpaceName, "branch/test/crash")
	})
}

// This is the normal way to run tests in the most time efficient manner
func runTest(t *testing.T, test quantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, 1, nil)
}

// If you need to initialize the QuantumFS instance in some special way,
// then use this variant.
func runTestNoQfs(t *testing.T, test quantumFsTest) {
	t.Parallel()
	runTestCommon(t, test, 0, nil)
}

// configModifier is a function which is given the default configuration
// and should make whichever modifications the test requires in place.
type configModifierFunc func(test *testHelper, config *QuantumFsConfig)

// If you need to initialize QuantumFS with a special configuration, but not poke
// into its internals before the test proper begins, use this.
func runTestCustomConfig(t *testing.T, configModifier configModifierFunc,
	test quantumFsTest) {

	t.Parallel()
	runTestCommon(t, test, 1, configModifier)
}

// If you need to initialize the QuantumFS instance in some special way and the test
// is relatively expensive, then use this variant.
func runTestNoQfsExpensiveTest(t *testing.T, test quantumFsTest) {
	runTestCommon(t, test, 0, nil)
}

// If you need to run two concurrent instances of QuantumFS in the same test, use
// runTestDualQuantumFS().
func runDualQuantumFsTest(t *testing.T, test quantumFsTest) {
	runTestCommon(t, test, 2, nil)
}

// If you have a test which is expensive in terms of CPU time, then use
// runExpensiveTest() which will not run it at the same time as other tests. This is
// to prevent multiple expensive tests from running concurrently and causing each
// other to time out due to CPU starvation.
func runExpensiveTest(t *testing.T, test quantumFsTest) {
	runTestCommon(t, test, 1, nil)
}

func runTestCommon(t *testing.T, test quantumFsTest, numDefaultQfs int,
	configModifier configModifierFunc) {

	// the stack depth of test name for all callers of runTestCommon
	// is 2. Since the stack looks as follows:
	// 2 <testname>
	// 1 runTest
	// 0 runTestCommon
	testName := testutils.TestName(2)
	th := &testHelper{
		TestHelper: TestHelper{
			TestHelper: testutils.NewTestHelper(testName,
				TestRunDir, t),
		},
	}
	th.CreateTestDirs()
	defer th.EndTest()

	var alt utils.AlternatingLocker
	func() {
		defer alt.ALock().AUnlock()
		startQuantumFsInstances(numDefaultQfs, configModifier, th)
	}()

	defer alt.RLock().RUnlock()
	th.RunTestCommonEpilog(testName, th.testHelperUpcast(test))
}

func startQuantumFsInstances(numDefaultQfs int, configModifier configModifierFunc,
	th *testHelper) {

	// Allow tests to run for up to 1 seconds before considering them timed out.
	// If we are going to start a standard QuantumFS instance we can start the
	// timer before the test proper and therefore avoid false positive test
	// failures due to timeouts caused by system slowness as we try to mount
	// dozens of FUSE filesystems at once.
	if numDefaultQfs >= 1 {
		config := th.defaultConfig()
		if configModifier != nil {
			configModifier(th, &config)
		}

		th.startQuantumFs(config)
	}
	if numDefaultQfs >= 2 {
		config := th.defaultConfig()
		wsdb := th.qfs.config.WorkspaceDB.(*processlocal.WorkspaceDB)
		config.WorkspaceDB = wsdb.GetSecondHead()

		config.DurableStore = th.qfs.config.DurableStore
		config.MountPath += "2"

		if configModifier != nil {
			configModifier(th, &config)
		}

		th.startQuantumFs(config)
	}
	if numDefaultQfs > 2 {
		th.T.Fatalf("Too many QuantumFS instances requested")
	}
}

type quantumFsTest func(test *testHelper)

func (th *testHelper) testHelperUpcast(
	testFn func(test *testHelper)) testutils.QuantumFsTest {

	return func(test testutils.TestArg) {
		testFn(th)
	}
}

// testHelper holds the variables important to maintain the state of testing
// in a package. This helper is more of a namespacing mechanism than a
// coherent object.
type testHelper struct {
	TestHelper
}

// Retrieve a list of FileDescriptor from an Inode
func (th *testHelper) fileDescriptorFromInodeNum(inodeNum uint64) []*FileDescriptor {
	handles := make([]*FileDescriptor, 0)

	defer th.qfs.mapMutex.Lock().Unlock()

	for _, file := range th.qfs.fileHandles {
		fh, ok := file.(*FileDescriptor)
		if !ok {
			continue
		}

		if fh.inodeNum == InodeId(inodeNum) {
			handles = append(handles, fh)
		}
	}

	return handles
}

// Return the inode number from QuantumFS. Fails if the absolute path doesn't exist.
func (th *testHelper) getInodeNum(path string) InodeId {
	var stat syscall.Stat_t
	err := syscall.Stat(path, &stat)
	th.Assert(err == nil, "Error grabbing file inode (%s): %v", path, err)

	return InodeId(stat.Ino)
}

// Retrieve the Inode from Quantumfs. Returns nil is not instantiated
func (th *testHelper) getInode(path string) Inode {
	inodeNum := th.getInodeNum(path)
	return th.qfs.inodeNoInstantiate(&th.qfs.c, inodeNum)
}

func (th *testHelper) workspaceRootId(typespace string, namespace string,
	workspace string) (quantumfs.ObjectKey, quantumfs.WorkspaceNonce) {

	key, nonce, err := th.qfs.c.workspaceDB.Workspace(&th.newCtx().Ctx,
		typespace, namespace, workspace)
	th.Assert(err == nil, "Error fetching key")

	return key, nonce
}

func (th *testHelper) MakeFile(filepath string) (data []byte) {
	// Make subdirectories needed
	lastIndex := strings.LastIndex(filepath, "/")
	if lastIndex > 0 {
		err := os.MkdirAll(filepath[:lastIndex], 0777)
		th.AssertNoErr(err)
	}

	// choose an offset and length based on the filepath so that it varies, but
	// is consistent from run to run
	charSum := 100
	for i := 0; i < len(filepath); i++ {
		charSum += int(filepath[i] - ' ')
	}

	// Add a little protection in case somehow this is negative
	if charSum < 0 {
		charSum = -charSum
	}

	offset := charSum
	length := offset

	data = GenData(offset + length)[offset:]
	err := testutils.PrintToFile(filepath, string(data))
	th.AssertNoErr(err)

	return data

}

func (th *testHelper) CheckData(filepath string, data []byte) {
	readData, err := ioutil.ReadFile(filepath)
	th.AssertNoErr(err)
	th.Assert(bytes.Equal(readData, data), "Data changed in CheckData")
}

func (th *testHelper) SysStat(filepath string) syscall.Stat_t {
	var stat syscall.Stat_t
	err := syscall.Stat(filepath, &stat)
	th.AssertNoErr(err)

	return stat
}

func (th *testHelper) SysLstat(filepath string) syscall.Stat_t {
	var stat syscall.Stat_t
	err := syscall.Lstat(filepath, &stat)
	th.AssertNoErr(err)

	return stat
}

// Temporary directory for this test run
var testRunDir string

func init() {
	syscall.Umask(0)
	testRunDir = testutils.SetupTestspace("daemonQuantumfsTest")
}

// Produce a request specific ctx variable to use for quantumfs internal calls
func (th *testHelper) newCtx() *ctx {
	reqId := atomic.AddUint64(&requestId, 1)
	c := th.dummyReq(reqId)
	c.Ctx.Vlog(qlog.LogTest, "Allocating request %d to test %s", reqId,
		th.TestName)
	return c
}

func (th *testHelper) remountFilesystem() {
	th.Log("Remounting filesystem")
	th.putApi()
	for i := 0; i < 100; i++ {
		err := syscall.Mount("", th.TempDir+"/mnt", "",
			syscall.MS_REMOUNT|syscall.MS_RDONLY, "")
		if err != nil {
			th.Log("Remount failed with " + err.Error() + " retrying...")
			time.Sleep(time.Millisecond)
		} else {
			break
		}
		th.Assert(i < 99, "Cannot remount readonly %v", err)
	}

	err := syscall.Mount("", th.TempDir+"/mnt", "", syscall.MS_REMOUNT, "")
	th.Assert(err == nil, "Unable to remount %v", err)
}

// Modify the QuantumFS cache time to 100 milliseconds
func cacheTimeout100Ms(test *testHelper, config *QuantumFsConfig) {
	config.CacheTimeSeconds = 0
	config.CacheTimeNsecs = 100000
}

// Modify the QuantumFS flush delay to 100 milliseconds
func dirtyDelay100Ms(test *testHelper, config *QuantumFsConfig) {
	config.DirtyFlushDelay = 100 * time.Millisecond
}

// Extract namespace and workspace path from the absolute path of
// a workspaceroot
func (th *testHelper) getWorkspaceComponents(abspath string) (string,
	string, string) {

	relpath := th.RelPath(abspath)
	components := strings.Split(relpath, "/")

	return components[0], components[1], components[2]
}

// Convert an absolute workspace path to the matching WorkspaceRoot object. The same
// as MUX::getWorkspaceRoot(), the caller of this function should run Forget function
// at the end.
func (th *testHelper) getWorkspaceRoot(workspace string) (wsr *WorkspaceRoot,
	cleanup func()) {

	parts := strings.Split(th.RelPath(workspace), "/")
	wsr, cleanup, ok := th.qfs.getWorkspaceRoot(&th.qfs.c,
		parts[0], parts[1], parts[2])
	th.Assert(ok, "WorkspaceRoot object for %s not found", workspace)

	return wsr, cleanup
}

func (th *testHelper) getAccessList(workspace string) *quantumfs.PathsAccessed {
	wsr, cleanup := th.getWorkspaceRoot(workspace)
	defer cleanup()
	accessed := wsr.getList()
	return &accessed
}

func (th *testHelper) assertAccessList(testlist quantumfs.PathsAccessed,
	wsrlist *quantumfs.PathsAccessed, message string) {

	eq := reflect.DeepEqual(&testlist, wsrlist)
	msg := fmt.Sprintf("\ntestlist:%v\n, wsrlist:%v\n", testlist, wsrlist)
	message = message + msg
	th.Assert(eq, message)
}

func (th *testHelper) assertWorkspaceAccessList(testlist quantumfs.PathsAccessed,
	workspaceName string) {

	gotAccessList := th.getAccessList(workspaceName)
	th.assertAccessList(testlist, gotAccessList, "Error two maps differ")
}

func (th *testHelper) checkSparse(fileA string, fileB string, offset int,
	len int) {

	fdA, err := os.OpenFile(fileA, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileA for RDONLY")
	defer fdA.Close()

	fdB, err := os.OpenFile(fileB, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileB for RDONLY")
	defer fdB.Close()

	statA, err := fdA.Stat()
	th.Assert(err == nil, "Unable to fetch fileA stats")
	statB, err := fdB.Stat()
	th.Assert(err == nil, "Unable to fetch fileB stats")
	th.Assert(statB.Size() == statA.Size(), "file sizes don't match")

	rtnA := make([]byte, len)
	rtnB := make([]byte, len)

	for idx := int64(0); idx+int64(len) < statA.Size(); idx += int64(offset) {
		var readA int
		for readA < len {
			readIt, err := fdA.ReadAt(rtnA[readA:], idx+int64(readA))

			if err == io.EOF {
				return
			}
			th.Assert(err == nil,
				"Error while reading from fileA at %d", idx)
			readA += readIt
		}

		var readB int
		for readB < len {
			readIt, err := fdB.ReadAt(rtnB[readB:], idx+int64(readB))

			if err == io.EOF {
				return
			}
			th.Assert(err == nil,
				"Error while reading from fileB at %d", idx)
			readB += readIt
		}
		th.Assert(bytes.Equal(rtnA, rtnB), "data mismatch, %v vs %v",
			rtnA, rtnB)
	}
}

func (th *testHelper) checkZeroSparse(fileA string, offset int) {
	fdA, err := os.OpenFile(fileA, os.O_RDONLY, 0777)
	th.Assert(err == nil, "Unable to open fileA for RDONLY")
	defer fdA.Close()

	statA, err := fdA.Stat()
	th.Assert(err == nil, "Unable to fetch fileA stats")

	rtnA := make([]byte, 1)
	for idx := int64(0); idx < statA.Size(); idx += int64(offset) {
		_, err := fdA.ReadAt(rtnA, idx)

		if err == io.EOF {
			return
		}
		th.Assert(err == nil,
			"Error while reading from fileA at %d", idx)

		th.Assert(bytes.Equal(rtnA, []byte{0}), "file %s not zeroed",
			fileA)
	}
}

func (test *testHelper) getRootId(workspace string) quantumfs.ObjectKey {
	wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
		test.getWorkspaceComponents(workspace)

	key, _ := test.workspaceRootId(wsTypespaceName, wsNamespaceName,
		wsWorkspaceName)

	return key
}

func (test *testHelper) advanceWorkspace(workspace string,
	nonce quantumfs.WorkspaceNonce, src quantumfs.ObjectKey,
	dst quantumfs.ObjectKey) {

	ctx := test.TestCtx()
	wsdb := test.GetWorkspaceDB()

	wsTypespaceName, wsNamespaceName, wsWorkspaceName :=
		test.getWorkspaceComponents(workspace)

	_, err := wsdb.AdvanceWorkspace(&ctx.Ctx, wsTypespaceName,
		wsNamespaceName, wsWorkspaceName, nonce, src, dst)
	test.AssertNoErr(err)

	wsr, cleanup := test.getWorkspaceRoot(workspace)
	defer cleanup()
	test.Assert(wsr != nil, "workspace root does not exist")
	wsr.publishedRootId = dst
}

// Sync the workspace, perform the nosync_op, then sync the workspace again,
// and verify that the workspaceroot has changed because of the operation.
func (test *testHelper) synced_op(workspace string,
	nosync_op func()) quantumfs.ObjectKey {

	test.SyncAllWorkspaces()
	oldRootId := test.getRootId(workspace)
	nosync_op()
	test.SyncAllWorkspaces()
	newRootId := test.getRootId(workspace)
	test.Assert(!newRootId.IsEqualTo(oldRootId), "no changes to the rootId")
	test.Log("new rootID %s", newRootId.String())

	return newRootId
}

func (test *testHelper) createFile(workspace string, name string,
	size int) {

	filename := workspace + "/" + name
	err := testutils.PrintToFile(filename, string(GenData(size)))
	test.AssertNoErr(err)
}

func (test *testHelper) createFileSync(workspace string, name string,
	size int) quantumfs.ObjectKey {

	return test.synced_op(workspace, func() {
		test.createFile(workspace, name, size)
	})
}

func (test *testHelper) removeFile(
	workspace string, name string) {

	filename := workspace + "/" + name
	err := os.Remove(filename)
	test.AssertNoErr(err)
}

func (test *testHelper) removeFileSync(
	workspace string, name string) quantumfs.ObjectKey {

	return test.synced_op(workspace, func() {
		test.removeFile(workspace, name)
	})
}

func (test *testHelper) linkFile(workspace string, src string,
	dst string) {

	srcfilename := workspace + "/" + src
	dstfilename := workspace + "/" + dst
	test.Log("Before link %s -> %s", src, dst)
	test.AssertNoErr(syscall.Link(srcfilename, dstfilename))
}

func (test *testHelper) linkFileSync(workspace string, src string,
	dst string) quantumfs.ObjectKey {

	return test.synced_op(workspace, func() {
		test.linkFile(workspace, src, dst)
	})
}

func (test *testHelper) moveFile(workspace string, src string,
	dst string) {

	srcfilename := workspace + "/" + src
	dstfilename := workspace + "/" + dst
	test.Log("Before move %s -> %s", src, dst)
	test.AssertNoErr(syscall.Rename(srcfilename, dstfilename))
}

func (test *testHelper) moveFileSync(workspace string, src string,
	dst string) quantumfs.ObjectKey {

	return test.synced_op(workspace, func() {
		test.moveFile(workspace, src, dst)
	})
}

func (test *testHelper) setXattr(workspace string,
	testfile string, attr string, data []byte) {

	testFilename := workspace + "/" + testfile
	test.Log("Before setting xattr %s on %s", attr, testfile)
	err := syscall.Setxattr(testFilename, attr, data, 0)
	test.AssertNoErr(err)
}

func (test *testHelper) verifyXattr(workspace string,
	testfile string, attr string, content []byte) {

	data := make([]byte, 100)
	size, err := syscall.Getxattr(workspace+"/"+testfile, attr, data)
	test.Assert(err == nil, "Error reading data XAttr: %v", err)
	test.Assert(size == len(content),
		"data XAttr size incorrect: %d", size)
	test.Assert(bytes.Equal(data[:size], content),
		"Didn't get the same data back '%s' '%s'", data,
		content)
}

func (test *testHelper) verifyNoXattr(workspace string,
	testfile string, attr string) {

	data := make([]byte, 100)
	_, err := syscall.Getxattr(workspace+"/"+testfile, attr, data)
	test.AssertErr(err)
	test.Assert(err == syscall.ENODATA, "xattr must not exist %s", err.Error())
}

func (test *testHelper) setXattrSync(
	workspace string, testfile string, attr string,
	data []byte) quantumfs.ObjectKey {

	return test.synced_op(workspace, func() {
		test.setXattr(workspace, testfile, attr, data)
	})
}

func (test *testHelper) delXattr(workspace string, testfile string,
	attr string) {

	testFilename := workspace + "/" + testfile
	test.Log("Before removing xattr %s on %s", attr, testfile)
	err := syscall.Removexattr(testFilename, attr)
	test.AssertNoErr(err)
}

func (test *testHelper) delXattrSync(workspace string, testfile string,
	attr string) quantumfs.ObjectKey {

	return test.synced_op(workspace, func() {
		test.delXattr(workspace, testfile, attr)
	})
}

func (test *testHelper) createSymlink(workspace string, name string,
	symname string) {

	err := syscall.Symlink(workspace+"/"+name, workspace+"/"+symname)
	test.AssertNoErr(err)
}

func (test *testHelper) createSymlinkSync(
	workspace string, name string, symname string) quantumfs.ObjectKey {

	return test.synced_op(workspace, func() {
		test.createSymlink(workspace, name, symname)
	})
}

func (test *testHelper) createSpecialFile(workspace string, name string,
	dev int) {

	err := syscall.Mknod(workspace+"/"+name, syscall.S_IFBLK|syscall.S_IRWXU,
		dev)
	test.AssertNoErr(err)
}

func (test *testHelper) createSpecialFileSync(workspace string,
	name string, dev int) quantumfs.ObjectKey {

	return test.synced_op(workspace, func() {
		test.createSpecialFile(workspace, name, dev)
	})
}

func (test *testHelper) assertFileIsOfSize(fullname string, size int64) {
	var stat syscall.Stat_t

	err := syscall.Stat(fullname, &stat)
	test.AssertNoErr(err)
	test.Assert(stat.Size == size,
		"Incorrect file size. Expected: %d", stat.Size)
}

func (test *testHelper) assertNoFile(fullname string) {
	var stat syscall.Stat_t
	err := syscall.Stat(fullname, &stat)
	test.AssertErr(err)
	test.Assert(err == syscall.ENOENT, "Expected ENOENT, got %s", err.Error())
}

func (test *testHelper) assertOpenFileIsOfSize(fd int, size int64) {
	var stat syscall.Stat_t

	err := syscall.Fstat(fd, &stat)
	test.AssertNoErr(err)
	test.Assert(stat.Size == size,
		"Incorrect file size. Expected: %d", stat.Size)
}

func (test *testHelper) verifyContentStartsWith(file *os.File, expected string) {
	content := make([]byte, len(expected))
	_, err := file.Seek(0, os.SEEK_SET)
	test.AssertNoErr(err)
	_, err = io.ReadFull(file, content)
	test.AssertNoErr(err)
	test.Assert(string(content) == expected,
		"content mismatch %s vs. %s", content, expected)
}

func createSparseFile(name string, size int64) error {
	fd, err := syscall.Creat(name, 0124)
	if err != nil {
		return err
	}
	err = syscall.Close(fd)
	if err != nil {
		return err
	}
	return os.Truncate(name, size)
}

func CreateSmallFile(name string, content string) error {
	fd, err := syscall.Creat(name, 0124)
	if err != nil {
		return err
	}
	err = syscall.Close(fd)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func CreateMediumFile(name string, content string) error {
	size := int64(quantumfs.MaxMediumFileSize()) -
		int64(quantumfs.MaxBlockSize)
	err := createSparseFile(name, size)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func CreateLargeFile(name string, content string) error {
	size := int64(quantumfs.MaxMediumFileSize()) +
		int64(quantumfs.MaxBlockSize)
	err := createSparseFile(name, size)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func CreateVeryLargeFile(name string, content string) error {
	size := int64(quantumfs.MaxLargeFileSize()) +
		int64(quantumfs.MaxBlockSize)
	err := createSparseFile(name, size)
	if err != nil {
		return err
	}
	return testutils.OverWriteFile(name, content)
}

func CreateHardlink(name string, content string) error {
	fd, err := syscall.Creat(name, syscall.O_CREAT)
	if err != nil {
		return err
	}
	err = syscall.Close(fd)
	if err != nil {
		return err
	}
	err = testutils.OverWriteFile(name, content)
	if err != nil {
		return err
	}
	return syscall.Link(name, name+"_link")
}

func ManualLookup(c *ctx, parent Inode, childName string) {
	var dummy fuse.EntryOut
	defer parent.RLockTree().RUnlock()
	parent.Lookup(c, childName, &dummy)
}
