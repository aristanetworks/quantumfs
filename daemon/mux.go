// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// The QuantumFS internals are implemented here. This is not the package you want,
// try quantumfs.
package daemon

// go-fuse creates a goroutine for every request. The code here simply takes these
// requests and forwards them to the correct Inode.

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/signal"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aristanetworks/quantumfs"
	"github.com/aristanetworks/quantumfs/qlog"
	"github.com/aristanetworks/quantumfs/utils"
	"github.com/hanwen/go-fuse/fuse"
)

const InodeNameLog = "inode %d name %s"
const InodeOnlyLog = "inode %d"
const FileHandleLog = "Fh %d"
const FileOffsetLog = "Fh %d offset %d"
const SetAttrArgLog = "inode %d valid 0x%x size %d"

func NewQuantumFs_(config QuantumFsConfig, qlogIn *qlog.Qlog) *QuantumFs {
	qfs := &QuantumFs{
		RawFileSystem:          fuse.NewDefaultRawFileSystem(),
		config:                 config,
		inodes:                 make(map[InodeId]Inode),
		inodeRefcounts:         make(map[InodeId]int32),
		inodeIds:               newInodeIds(time.Minute, time.Minute*5),
		fileHandleNum:          0,
		flusher:                NewFlusher(),
		parentOfUninstantiated: make(map[InodeId]InodeId),
		lookupCounts:           make(map[InodeId]uint64),
		workspaceMutability:    make(map[string]workspaceState),
		toBeReleased:           make(chan uint64, 1000000),
		toNotifyFuse:           make(chan FuseNotification, 10000),
		stopWaitingForSignals:  make(chan struct{}),
		syncAllRetries:         -1,
		c: ctx{
			Ctx: quantumfs.Ctx{
				Qlog:      qlogIn,
				RequestId: qlog.MuxReqId,
			},
			config:      &config,
			workspaceDB: config.WorkspaceDB,
			dataStore: newDataStore(config.DurableStore,
				int(config.CacheSize)),
		},
	}

	qfs.c.vlog("Random seed: %d", utils.RandomSeed)

	qfs.c.qfs = qfs

	typespaceList := NewTypespaceList()
	qfs.inodes[quantumfs.InodeIdRoot] = typespaceList
	qfs.inodes[quantumfs.InodeIdApi] = NewApiInode(&qfs.c,
		typespaceList.treeState(), typespaceList)
	qfs.inodes[quantumfs.InodeIdLowMemMarker] = NewLowMemFile(&qfs.c,
		typespaceList.treeState(), typespaceList)
	return qfs
}

func NewQuantumFsLogs(config QuantumFsConfig, qlogIn *qlog.Qlog) *QuantumFs {
	return NewQuantumFs_(config, qlogIn)
}

func NewQuantumFs(config QuantumFsConfig, version string) (*QuantumFs, error) {
	logger, err := qlog.NewQlogExt(config.CachePath,
		config.MemLogBytes, version, qlog.PrintToStdout)
	if err != nil {
		return nil, err
	}

	logger.ErrorExec = config.ErrorExec

	if !config.VerboseTracing {
		logger.SetMaxLevel(2) // dlog
	}

	return NewQuantumFs_(config, logger), nil
}

type workspaceState int

const (
	workspaceImmutable workspaceState = iota
	workspaceMutable
	workspaceImmutableUntilRestart
)

type MetaInodeDeletionRecord struct {
	inodeId  InodeId
	parentId InodeId
	name     string
}

type QuantumFs struct {
	fuse.RawFileSystem
	server        *fuse.Server
	config        QuantumFsConfig
	inodeIds      *inodeIds
	fileHandleNum uint64
	c             ctx

	syncAllRetries int

	// We present the sum of the size of all responses waiting on the api file as
	// the size of that file because the kernel will clear any reads beyond what
	// is believed to be the file length. Thus the file length needs to be at
	// least as long as the largest response and using the sum of all response
	// lengths is more efficient than computing the maximum response length over
	// a large number of ApiHandles.
	apiFileSize int64

	// This is a leaf lock for protecting the instantiation maps
	// Do not grab other locks while holding this
	mapMutex       utils.DeferableRwMutex
	inodes         map[InodeId]Inode
	inodeRefcounts map[InodeId]int32

	fileHandles sync.Map // map[FileHandleId]FileHandle

	flusher *Flusher

	// We must prevent instantiation of Inodes while we are uninstantiating an
	// Inode. This prevents a race between a Directory being uninstantiated as
	// one of its children is just being instantiated. Or the same inode getting
	// instantiated in multiple threads.
	//
	// This lock must always be grabbed before the mapMutex to ensure consistent
	// lock ordering.
	instantiationLock utils.DeferableMutex

	// Uninstantiated Inodes are inode numbers which have been reserved for a
	// particular inode, but the corresponding Inode has not yet been
	// instantiated. The Inode this map points to is the parent Inode which
	// should be called to instantiate the uninstantiated inode when necessary.
	parentOfUninstantiated map[InodeId]InodeId

	// lookupCounts are used as the other side of Forget. That is, Forget
	// specifies a certain number of lookup counts to forget, which may not be
	// all of them. We cannot truly forget and delete an Inode until the lookup
	// count is zero. Because our concept of uninstantiated Inode allows us to
	// not instantiate an Inode for certain operations which the kernel increases
	// its lookup count, we must keep an entirely separate table.
	//
	// Any inode number without an entry is assumed to have zero lookups and not
	// be instantiated. Some inode numbers will have an entry with a zero value.
	// These are instantiated inodes waiting to be uninstantiated. Inode numbers
	// with positive values are still referenced by the kernel.
	lookupCountLock utils.DeferableMutex
	lookupCounts    map[InodeId]uint64

	// The workspaceMutability defines whether all inodes in each of the local
	// workspace is mutable(write-permitted). Once if a workspace is not
	// immutable, can it be set mutable, so TRUE should be put into the map.
	// Empty entires are default as read-only.  When set the workspace immutable,
	// delete the entry from the map
	mutabilityLock      utils.DeferableRwMutex
	workspaceMutability map[string]workspaceState

	toBeReleased chan uint64 // FileHandleId

	// FUSE notification requests cannot be made from the same goroutine handling
	// any FUSE request or a deadlock inside the kernel may result. Instead we
	// queue all such notifications to a separate goroutine.
	toNotifyFuse chan FuseNotification

	stopWaitingForSignals chan struct{}

	inLowMemoryMode bool

	inodeIdGeneration uint64
}

func (qfs *QuantumFs) Mount(mountOptions fuse.MountOptions) error {
	qfs.c.dlog("QuantumFs::Mount Initializing server")

	// Set the common set of required options
	mountOptions.AllowOther = true
	mountOptions.MaxBackground = 1024
	mountOptions.MaxWrite = quantumfs.MaxBlockSize
	mountOptions.FsName = "QuantumFS"
	mountOptions.Options = append(mountOptions.Options, "suid", "dev")

	if !qfs.config.MagicOwnership {
		mountOptions.Options = append(mountOptions.Options,
			"default_permissions")
	}

	server, err := fuse.NewServer(qfs, qfs.config.MountPath, &mountOptions)
	if err != nil {
		qfs.c.elog("Failed to create new server %s", err.Error())
		return err
	}

	go qfs.adjustKernelKnobs()
	go batchProcessor(qfs.toBeReleased, qfs.fileHandleReleaser)
	go qfs.fuseNotifier()
	go qfs.waitForSignals()

	qfs.config.WorkspaceDB.SetCallback(qfs.handleWorkspaceChanges)

	qfs.server = server
	return nil
}

const ReleaseFileHandleLog = "Mux::fileHandleReleaser"

func (qfs *QuantumFs) fileHandleReleaser(ids []uint64) {
	defer qfs.c.statsFuncIn(ReleaseFileHandleLog).Out()
	defer qfs.mapMutex.Lock().Unlock()
	for _, id := range ids {
		qfs.setFileHandle_(&qfs.c, FileHandleId(id), nil)
	}
}

func batchProcessor(inputChan <-chan uint64, handleBatch func(ids []uint64)) {
	const maxPerCycle = 1000
	i := 0
	ids := make([]uint64, 0, maxPerCycle)
	for shutdown := false; !shutdown; {
		shutdown = func() bool {
			ids = ids[:0]
			id, ok := <-inputChan
			if !ok {
				return true
			}
			ids = append(ids, id)
			for i = 1; i < maxPerCycle; i++ {
				select {
				case id, ok := <-inputChan:
					if !ok {
						return true
					}
					ids = append(ids, id)
				default:
					return false
				}
			}
			return false
		}()
		handleBatch(ids)

		if !shutdown && i < maxPerCycle {
			// If we didn't need our full allocation, sleep to accumulate
			// more work with minimal mapMutex contention.
			time.Sleep(100 * time.Millisecond)
		}
	}
}

type FuseNotificationType int

const (
	NotifyFuseInvalid = FuseNotificationType(iota)
	NotifyFuseDeleted
	NotifyFuseCreated
)

type FuseNotification struct {
	c      *ctx
	op     FuseNotificationType
	parent InodeId
	child  InodeId
	inode  InodeId
	name   string
}

func (qfs *QuantumFs) fuseNotifier() {
	for notification := range qfs.toNotifyFuse {
		var err fuse.Status

		switch notification.op {
		case NotifyFuseInvalid:
			notification.c.vlog("Notifying FUSE of invalid %d",
				notification.inode)
			err = qfs.server.InodeNotify(uint64(notification.inode), 0,
				-1)

		case NotifyFuseDeleted:
			notification.c.vlog("Notifying FUSE of delete %d (%s) in %d",
				notification.child, notification.name,
				notification.parent)
			err = qfs.server.DeleteNotify(uint64(notification.parent),
				uint64(notification.child), notification.name)

		case NotifyFuseCreated:
			notification.c.vlog("Notifying FUSE of create (%s) in %d",
				notification.name, notification.parent)
			err = qfs.server.EntryNotify(uint64(notification.parent),
				notification.name)
		}

		if err != fuse.OK && err != fuse.ENOENT {
			notification.c.dlog("Kernel error when notifying: %d", err)
		}
	}
}

func (qfs *QuantumFs) waitForSignals() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGUSR1, syscall.SIGUSR2)
	go qfs.signalHandler(sigChan)
}

// If we receive the signal SIGUSR1, then we will enter a low memory mode where we,
// among other things, prevent further writes to the cache and drop the contents of
// the cache. The intended use is as a way to free the bulk of the memory used by
// quantumfsd when it is being gracefully shutdown by lazily unmounting it.
// If we receive the signal SIGUSR2, then we will print some information about the
// internal state of the daemon. This signal is only used for debugging and testing
// purposes.
func (qfs *QuantumFs) signalHandler(sigChan chan os.Signal) {
	for {
		select {
		case sig := <-sigChan:
			switch sig {
			case syscall.SIGUSR1:
				qfs.c.wlog("Entering low memory mode")
				qfs.inLowMemoryMode = true
				qfs.c.dataStore.shutdown()

				// Release the memory
				debug.FreeOSMemory()
			case syscall.SIGUSR2:
				qfs.verifyNoLeaks()
			}

		case <-qfs.stopWaitingForSignals:
			signal.Stop(sigChan)
			close(sigChan)
			return
		}
	}
}

func (qfs *QuantumFs) verifyNoLeaks() {
	defer qfs.c.funcIn("QuantumFs::verifyNoLeaks").Out()
	defer qfs.instantiationLock.Lock().Unlock()
	defer qfs.lookupCountLock.Lock().Unlock()
	defer qfs.mapMutex.Lock().Unlock()

	for id, parent := range qfs.parentOfUninstantiated {
		if parent != quantumfs.InodeIdRoot {
			qfs.c.elog("leaked inode %d parent inode %d", id, parent)
		}
	}

	for inodeId, count := range qfs.lookupCounts {
		if inodeId != quantumfs.InodeIdRoot &&
			inodeId != quantumfs.InodeIdApi {

			qfs.c.elog("leaked inode %d lookupCount %d", inodeId, count)
		}
	}
}

func (qfs *QuantumFs) Serve() {
	qfs.c.dlog("QuantumFs::Serve Serving")
	qfs.server.Serve()
	qfs.c.dlog("QuantumFs::Serve Finished serving")

	qfs.c.dlog("QuantumFs::Serve Waiting for flush thread to end")

	for qfs.flusher.syncAll(&qfs.c) != nil {
		qfs.c.dlog("Cannot give up on syncing, retrying shortly")
		time.Sleep(100 * time.Millisecond)

		if qfs.syncAllRetries < 0 {
			continue
		} else if qfs.syncAllRetries == 0 {
			qfs.c.elog("Unable to syncAll after Serve")
			break
		}

		qfs.syncAllRetries--
	}
	close(qfs.stopWaitingForSignals)
}

func (qfs *QuantumFs) Shutdown() error {
	if err := qfs.c.Qlog.Sync(); err != 0 {
		qfs.c.elog("Syncing log file failed with %d. Closing it.", err)
	}
	close(qfs.toBeReleased)
	close(qfs.toNotifyFuse)
	return qfs.c.Qlog.Close()
}

func (qfs *QuantumFs) handleWorkspaceChanges(
	updates map[string]quantumfs.WorkspaceState) {

	c := qfs.c.reqId(qlog.RefreshReqId, nil)

	defer c.FuncIn("Mux::handleWorkspaceChanges", "%d updates",
		len(updates)).Out()

	for name, state := range updates {
		if state.Deleted {
			go qfs.handleDeletedWorkspace(c, name)
		} else {
			go qfs.refreshWorkspace(c, name, state.RootId, state.Nonce)
		}
	}
}

func (qfs *QuantumFs) handleMetaInodeRemoval(c *ctx, id InodeId, name string,
	parentId InodeId) {

	defer c.FuncIn("QuantumFs::handleMetaInodeRemoval", "%s inode %d",
		name, id).Out()

	// This function might have been called as a result of a lookup.
	// Therefore, it is not safe to call back into the kernel, telling it
	// about the deletion. Schedule this call for later.
	qfs.noteDeletedInode(c, parentId, id, name)

	// This is a no-op if the inode is instantiated. This check should happen
	// before checking whether id is instantiated to avoid racing with someone
	// instantiating this inode
	c.qfs.removeUninstantiated(c, []InodeId{id})

	inode := qfs.inodeNoInstantiate(c, id)
	if inode == nil {
		return
	}
	defer inode.getParentLock().Lock().Unlock()
	if inode.isOrphaned_() {
		return
	}
	inode.orphan_(c, nil)
}

func (qfs *QuantumFs) handleDeletedWorkspace(c *ctx, name string) {
	defer c.FuncIn("Mux::handleDeletedWorkspace", "%s", name).Out()

	defer logRequestPanic(c)
	parts := strings.Split(name, "/")

	wsrLineage, err := qfs.getWsrLineageNoInstantiate(c,
		parts[0], parts[1], parts[2])
	if err != nil {
		c.elog("getting wsrLineage failed: %s", err.Error())
	} else if len(wsrLineage) == 4 {
		wsr := qfs.inodeNoInstantiate(c, wsrLineage[3])
		if wsr != nil {
			c.vlog("Setting tree skipFlush")
			wsr.treeState().skipFlush = true
		}

		// In case the deletion has happened remotely, workspacelisting does
		// not have the capability of orphaning the workspace if the
		// namespace or typespace have been removed as well.
		qfs.handleMetaInodeRemoval(c,
			wsrLineage[3], parts[2], wsrLineage[2])
	}

	// Instantiating the workspace has the side effect of querying the
	// workspaceDB and updating the in-memory data structures.
	// We need the current in-memory state though to take
	// other required actions
	_, cleanup, _ := qfs.getWorkspaceRoot(c, parts[0], parts[1], parts[2])
	cleanup()
}

func (qfs *QuantumFs) refreshWorkspace(c *ctx, name string,
	rootId quantumfs.ObjectKey, nonce quantumfs.WorkspaceNonce) {

	defer c.FuncIn("Mux::refreshWorkspace", "workspace %s", name).Out()

	c = c.refreshCtx()
	defer logRequestPanic(c)

	if qfs.inLowMemoryMode {
		c.wlog("Will not refresh workspace %s in low memory mode.", name)
		return
	}

	parts := strings.Split(name, "/")
	wsr, cleanup, ok := qfs.getWorkspaceRoot(c, parts[0], parts[1], parts[2])
	defer cleanup()

	if !ok {
		c.wlog("No workspace root for workspace %s", name)
		return
	}

	if qfs.workspaceIsMutable(c, wsr) {
		c.wlog("Refusing to refresh locally mutable workspace %s", name)
		return
	}

	if !nonce.SameIncarnation(&wsr.nonce) {
		c.dlog("Not refreshing workspace %s due to mismatching "+
			"nonces %s vs %s", name, wsr.nonce.String(), nonce.String())
		return
	}

	published := func() bool {
		defer wsr.RLockTree().RUnlock()
		return wsr.publishedRootId.IsEqualTo(rootId)
	}()

	if published {
		c.vlog("Not refreshing workspace %s as there has been no updates",
			name)
		return
	}

	defer wsr.LockTree().Unlock()

	err := qfs.flusher.syncWorkspace_(c, name)
	if err != nil {
		c.elog("Unable to syncWorkspace: %s", err.Error())
		return
	}

	wsr.refresh_(c)
}

func forceMerge(c *ctx, wsr *WorkspaceRoot) error {
	defer c.funcIn("Mux::forceMerge").Out()

	rootId, nonce, err := c.workspaceDB.Workspace(&c.Ctx,
		wsr.typespace, wsr.namespace, wsr.workspace)

	if err != nil {
		c.elog("Unable to get workspace rootId")
		return err
	}

	if !nonce.SameIncarnation(&wsr.nonce) {
		c.wlog("Nothing to merge, new workspace")
		return nil
	}

	if wsr.publishedRootId.IsEqualTo(rootId) {
		c.vlog("Not merging as there are no updates upstream")
		return nil
	}

	newRootId := publishWorkspaceRoot(c,
		wsr.baseLayerId, wsr.hardlinkTable.hardlinks, publishNow)

	// We should eventually be able to Advance after merging
	for {
		rootId, nonce, err := c.workspaceDB.Workspace(&c.Ctx,
			wsr.typespace, wsr.namespace, wsr.workspace)

		if err != nil {
			c.elog("Unable to get workspace rootId")
			return err
		}

		if !nonce.SameIncarnation(&wsr.nonce) {
			c.wlog("Nothing to merge, new workspace")
			return nil
		}

		mergedId, err := mergeWorkspaceRoot(c, wsr.publishedRootId, rootId,
			newRootId, quantumfs.PreferNewer,
			&mergeSkipPaths{paths: make(map[string]struct{}, 0)},
			wsr.typespace+"/"+wsr.namespace+"/"+wsr.workspace)

		if err != nil {
			c.elog("Unable to merge: %s", err.Error())
			return err
		}

		// now try to advance the workspace from the fresh id
		_, err = c.workspaceDB.AdvanceWorkspace(&c.Ctx, wsr.typespace,
			wsr.namespace, wsr.workspace, wsr.nonce, rootId, mergedId)

		if wsdbErr, isWsdbErr := err.(quantumfs.WorkspaceDbErr); isWsdbErr &&
			wsdbErr.Code == quantumfs.WSDB_OUT_OF_DATE {

			c.wlog("Workspace advanced during merge of %s, retrying.",
				wsr.fullname())
			// Try again
			continue
		}

		return err
	}
}

// Should be called with the tree locked for read or write
func (qfs *QuantumFs) flushInode_(c *ctx, inode Inode) bool {
	defer c.funcIn("Mux::flushInode_").Out()

	if inode.isOrphaned() {
		return true
	}
	return inode.flush(c).IsValid()
}

// There are several configuration knobs in the kernel which can affect FUSE
// performance. Don't depend on the system being configured correctly for QuantumFS,
// instead try to change the settings ourselves.
func (qfs *QuantumFs) adjustKernelKnobs() {
	qfs.c.funcIn("adjustKernelKnobs").Out()

	mountId := findFuseConnection(&qfs.c, qfs.config.MountPath)
	if mountId == -1 {
		// We don't know where we are mounted, give up
		return
	}

	adjustBdi(&qfs.c, mountId)
	adjustVmDirtyBackgroundBytes(&qfs.c)
}

func adjustBdi(c *ctx, mountId int) {
	defer c.funcIn("adjustBdi").Out()

	// /sys/class/bdi/<mount>/read_ahead_kb indicates how much data, up to the
	// end of the file, should be speculatively read by the kernel. Setting this
	// to the block size should improve the correlation between what the kernel
	// reads and what QuantumFS can provide most efficiently. Since this is the
	// amount in addition to the original read the kernel will read the entire
	// block containing the user's read and then some portion of the next block.
	// Thus QuantumFS will have time to fetch the next block in advance of it
	// being required.
	filename := fmt.Sprintf("/sys/class/bdi/0:%d/read_ahead_kb", mountId)
	value := fmt.Sprintf("%d", quantumfs.MaxBlockSize/1024)
	err := ioutil.WriteFile(filename, []byte(value), 000)
	if err != nil {
		c.wlog("Unable to set read_ahead_kb: %s", err.Error())
	}

	// /sys/class/bdi/<mount>/max_ratio indicates the percentage of the
	// write-back cache this filesystem is allowed to use. Thus it is a maximum
	// of <system memory size>*<vm.dirty_bytes_ratio>*<max_ratio>. On a 256G
	// system with defaults and no memory pressure that amounts to ~540MB. Since
	// QuantumFS is a trusted filesystem and on most systems nearly all the IO
	// will be through QuantumFS, treat QuantumFS like any other kernel file
	// system and allow it to use the full write-back cache if necessary.
	filename = fmt.Sprintf("/sys/class/bdi/0:%d/max_ratio", mountId)
	err = ioutil.WriteFile(filename, []byte("100"), 000)
	if err != nil {
		c.wlog("Unable to set bdi max_ratio: %s", err.Error())
	}
}

func adjustVmDirtyBackgroundBytes(c *ctx) {
	defer c.funcIn("adjustVmDirtyBackgroundBytes").Out()

	// Sometimes, for reasons which have not been root caused yet, the kernel
	// will start delaying FUSE requests for about 200ms. This appears related to
	// some IO throughput control mechanism erroneously trending to zero for
	// QuantumFS.
	//
	// Work around this for now by setting vm.dirty_background_bytes to a low
	// number, which prevents FUSE from getting stuck in this manner.
	err := ioutil.WriteFile("/proc/sys/vm/dirty_background_bytes",
		[]byte("100000"), 000)
	if err != nil {
		c.wlog("Unable to set vm.dirty_background_bytes: %s", err.Error())
	}
}

// Must hold the mapMutex
func (qfs *QuantumFs) getInode_(c *ctx, id InodeId) (Inode, bool) {
	inode, instantiated := qfs.inodes[id]
	if instantiated {
		return inode, false
	}

	_, uninstantiated := qfs.parentOfUninstantiated[id]
	return nil, uninstantiated
}

type emptyUnlocker struct {
	// placeholder interface implementor
}

func (eu *emptyUnlocker) Unlock() {
	// do nothing
}

func (er *emptyUnlocker) RUnlock() {
	// do nothing
}

// Often we need to grab an inode and lock the tree. We need the inode to lock its
// tree, however by the time we lock the tree the inode may be forgotten and the
// inode we grabbed invalid. This is a worker function to handle that case correctly.
func (qfs *QuantumFs) RLockTreeGetInode(c *ctx, inodeId InodeId) (Inode, func()) {
	inode, release := qfs.inode(c, inodeId)
	if inode == nil {
		release()
		return nil, func() {}
	}

	inode.RLockTree()

	return inode, func() {
		inode.treeState().RUnlock()
		release()
	}
}

// Same as the RLockTreeGetInode, but for writes
func (qfs *QuantumFs) LockTreeGetInode(c *ctx, inodeId InodeId) (Inode, func()) {
	inode, release := qfs.inode(c, inodeId)
	if inode == nil {
		release()
		return nil, func() {}
	}

	inode.LockTree()

	return inode, func() {
		inode.treeState().Unlock()
		release()
	}
}

func (qfs *QuantumFs) RLockTreeGetHandle(c *ctx, fh FileHandleId) (FileHandle,
	func()) {

	fileHandle := qfs.fileHandle(c, fh)
	if fileHandle == nil {
		return nil, func() {}
	}

	fileHandle.RLockTree()

	// once we have the lock, re-grab
	fileHandle = qfs.fileHandle(c, fh)
	return fileHandle, fileHandle.treeState().RUnlock
}

func (qfs *QuantumFs) LockTreeGetHandle(c *ctx, fh FileHandleId) (FileHandle,
	func()) {

	fileHandle := qfs.fileHandle(c, fh)
	if fileHandle == nil {
		return nil, func() {}
	}

	fileHandle.LockTree()

	// once we have the lock, re-grab
	fileHandle = qfs.fileHandle(c, fh)
	return fileHandle, fileHandle.treeState().Unlock
}

func (qfs *QuantumFs) inodeNoInstantiate(c *ctx, id InodeId) Inode {
	defer qfs.mapMutex.RLock().RUnlock()
	inode, _ := qfs.getInode_(c, id)
	return inode
}

func releaserFn(c *ctx, inode Inode) func() {
	// We want to call delRef asynchronously to ease the locking requirements.
	// delRef calls a number of locks, and so calling this synchronously could
	// cause deadlocks if the caller isn't careful about what they have locked.
	return func() {
		go inode.delRef(c)
	}
}

// Get an inode in a thread safe way. Release *must* be called when the inode is no
// longer needed since a reference count is added to prevent uninstantiation.
func (qfs *QuantumFs) inode(c *ctx, id InodeId) (newInode Inode, release func()) {
	// Handle the special case of invalid id
	if id == quantumfs.InodeIdInvalid {
		return nil, func() {}
	}

	// First find the Inode under a cheaper lock
	inode := func() Inode {
		defer qfs.mapMutex.Lock().Unlock()
		inode_, _ := qfs.getInode_(c, id)
		if inode_ != nil {
			addInodeRef_(c, id)
		}
		return inode_
	}()
	if inode != nil {
		return inode, releaserFn(c, inode)
	}

	// If we didn't find it, get the more expensive lock and check again. This
	// will instantiate the Inode if necessary and possible.
	instantiated := false
	parent := InodeId(0)
	func() {
		defer qfs.instantiationLock.Lock().Unlock()
		defer qfs.mapMutex.Lock().Unlock()

		parent = qfs.parentOfUninstantiated[id]
		inode, instantiated = qfs.inode_(c, id)
		// Add an inode reference for what will be using this inode
		addInodeRef_(c, id)
	}()

	func() {
		// Maybe give back the speculative lookupCount reference.
		//
		// See QuantumFs.inode_()
		if instantiated {
			defer qfs.lookupCountLock.Lock().Unlock()
			if _, exists := qfs.lookupCounts[id]; !exists {
				c.vlog("Removing speculative lookup reference")
				inode.delRef(c)
			} else {
				c.vlog("Retaining speculative lookup reference")
			}
		}
	}()

	defer func() {
		if panicErr := recover(); panicErr != nil {
			// rollback instantiation
			releaserFn(c, inode)()
			defer qfs.instantiationLock.Lock().Unlock()
			defer qfs.mapMutex.Lock().Unlock()
			qfs.parentOfUninstantiated[id] = parent
			qfs.setInode_(c, id, nil)

			panic(panicErr)
		}
	}()

	if instantiated {
		uninstantiated := inode.finishInit(c)
		if len(uninstantiated) > 0 {
			defer qfs.mapMutex.Lock().Unlock()
			qfs.addUninstantiated_(c, uninstantiated)
		}
	}

	return inode, releaserFn(c, inode)
}

// Must hold the instantiationLock and mapMutex for write
// Returns the inode and whether the inode was instantiated
// as part of this function, or was instantiated already.
func (qfs *QuantumFs) inode_(c *ctx, id InodeId) (Inode, bool) {
	inode, needsInstantiation := qfs.getInode_(c, id)
	if !needsInstantiation && inode != nil {
		return inode, false
	}

	c.vlog("Inode %d needs to be instantiated", id)

	parentId, uninstantiated := qfs.parentOfUninstantiated[id]
	if !uninstantiated {
		// We don't know anything about this Inode
		c.vlog("Inode not in parentOfUninstantiated")
		return nil, false
	}

	for {
		parent, _ := qfs.getInode_(c, parentId)
		if parent == nil {
			panic(fmt.Sprintf("Unable to instantiate parent %d",
				parentId))
		}

		func() {
			qfs.mapMutex.Unlock()
			defer qfs.mapMutex.Lock()
			// without mapMutex the child could move underneath this
			// parent, in such cases, find the new parent
			inode = parent.instantiateChild(c, id)
		}()
		if inode != nil {
			break
		}
		// a nil inode means the dentry has moved or has been removed
		newParentId, uninstantiated := qfs.parentOfUninstantiated[id]
		if !uninstantiated {
			// The dentry has been removed
			return nil, false
		}
		// The dentry is still there, verify the parent has changed
		utils.Assert(newParentId != parentId,
			"parent of inode %d is still %d", id, parentId)
		parentId = newParentId
	}

	delete(qfs.parentOfUninstantiated, id)
	qfs.inodes[id] = inode

	// We need to start an Inode with some initial reference count and we would
	// like to be able to catch increments from zero. Therefore we speculatively
	// provide a reference for lookupCounts here. In inode() after returning
	// from here we will get the lookupCountLock and remove the reference if we
	// speculated incorrectly.
	//
	// See QuantumFs.inode(). This is the ref for "has > 0 lookups"
	addInodeRef_(c, id)

	return inode, true
}

// Set an inode in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setInode(c *ctx, id InodeId, inode Inode) {
	defer qfs.mapMutex.Lock().Unlock()

	qfs.setInode_(c, id, inode)
}

// Must hold mapMutex exclusively
func (qfs *QuantumFs) setInode_(c *ctx, id InodeId, inode Inode) {
	if inode != nil {
		qfs.inodes[id] = inode
	} else {
		c.vlog("Clearing inode %d", id)
		delete(qfs.inodes, id)
	}
}

// Set a list of inode numbers to be uninstantiated with the given parent
type inodePair struct {
	child  InodeId
	parent InodeId
}

func newInodePair(c InodeId, p InodeId) inodePair {
	return inodePair{
		child:  c,
		parent: p,
	}
}

func (qfs *QuantumFs) addUninstantiated(c *ctx, uninstantiated []inodePair) {
	defer qfs.mapMutex.Lock().Unlock()

	qfs.addUninstantiated_(c, uninstantiated)
}

// Requires the mapMutex for writing
func (qfs *QuantumFs) addUninstantiated_(c *ctx, uninstantiated []inodePair) {
	defer c.funcIn("Mux::addUninstantiated_").Out()

	for _, pair := range uninstantiated {
		utils.Assert(pair.parent != 0,
			"Invalid parentId in addUninstantiated")

		qfs.parentOfUninstantiated[pair.child] = pair.parent
		c.vlog("Adding uninstantiated %d from %d (%d)", pair.child,
			pair.parent, len(qfs.parentOfUninstantiated))
	}
}

// Remove a list of inode numbers from the parentOfUninstantiated list
func (qfs *QuantumFs) removeUninstantiated(c *ctx, uninstantiated []InodeId) {
	defer c.funcIn("Mux::removeUninstantiated").Out()
	defer qfs.mapMutex.Lock().Unlock()

	for _, inodeNum := range uninstantiated {
		if _, hasRefs := qfs.inodeRefcounts[inodeNum]; !hasRefs {
			// It's only safe to release the inode id when it has been
			// removed from both refcounting and parentOfUninstantiated
			qfs.inodeIds.releaseInodeId(c, inodeNum)
		}

		delete(qfs.parentOfUninstantiated, inodeNum)
		c.vlog("Removing uninstantiated %d (%d)", inodeNum,
			len(qfs.parentOfUninstantiated))
	}
}

// Increment an Inode's lookup count. This must be called whenever a fuse.EntryOut is
// returned.
func (qfs *QuantumFs) incrementLookupCount(c *ctx, inodeId InodeId) {
	defer c.FuncIn("Mux::incrementLookupCount", "inode %d", inodeId).Out()
	defer qfs.lookupCountLock.Lock().Unlock()
	qfs.incrementLookupCount_(c, inodeId)
}

// Increment several Inode's lookup counts.
func (qfs *QuantumFs) incrementLookupCounts(c *ctx, children []directoryContents) {
	defer c.FuncIn("Mux::incrementLookupCounts", "%d inodes",
		len(children)).Out()
	defer qfs.lookupCountLock.Lock().Unlock()
	for _, child := range children {
		if child.filename == "." || child.filename == ".." {
			continue
		}
		qfs.incrementLookupCount_(c, InodeId(child.attr.Ino))
	}
}

// Must hold lookupCountLock
func (qfs *QuantumFs) incrementLookupCount_(c *ctx, inodeId InodeId) {
	prev, exists := qfs.lookupCounts[inodeId]
	if !exists {
		qfs.lookupCounts[inodeId] = 1

		if inodeId <= quantumfs.InodeIdReservedEnd {
			// These Inodes always exist
			c.vlog("Skipping refcount of permanent inode")
		} else {
			defer qfs.mapMutex.Lock().Unlock()
			inode, _ := qfs.getInode_(c, inodeId)
			if inode != nil {
				addInodeRef_(c, inodeId)
				utils.Assert(c.qfs.inodeRefcounts[inodeId] > 1,
					"Increased from zero refcount for inode %d!",
					inodeId)
			} else {
				c.vlog("Inode isn't instantiated")
			}
		}
	} else {
		qfs.lookupCounts[inodeId] = prev + 1
	}
}

func (qfs *QuantumFs) lookupCount(inodeId InodeId) (uint64, bool) {
	defer qfs.lookupCountLock.Lock().Unlock()
	lookupCount, exists := qfs.lookupCounts[inodeId]
	if !exists {
		return 0, false
	}

	return lookupCount, true
}

const alreadyUninstantiatedLog = "inode %d wasn't instantiated"

// Returns true if the count became zero or was previously zero
func (qfs *QuantumFs) shouldForget(c *ctx, inodeId InodeId, count uint64) bool {
	defer c.FuncIn("Mux::shouldForget", "inode %d count %d", inodeId,
		count).Out()

	if inodeId == quantumfs.InodeIdApi || inodeId == quantumfs.InodeIdRoot {
		return false
	}

	forgotten := false

	defer qfs.lookupCountLock.Lock().Unlock()
	lookupCount, exists := qfs.lookupCounts[inodeId]
	if !exists {
		c.vlog("inode %d has not been instantiated", inodeId)
		forgotten = true
		goto maybeReleaseRef
	}

	if lookupCount < count {
		c.elog("lookupCount less than zero %d %d", lookupCount, count)
	}

	lookupCount -= count
	qfs.lookupCounts[inodeId] = lookupCount
	if lookupCount == 0 {
		if count > 1 {
			c.vlog("Forgetting inode with lookupCount of %d", count)
		}
		delete(qfs.lookupCounts, inodeId)
		forgotten = true
	} else {
		forgotten = false
	}

maybeReleaseRef:
	if forgotten {
		inode := qfs.inodeNoInstantiate(c, inodeId)
		if inode != nil {
			inode.delRef(c)
		} else {
			c.vlog(alreadyUninstantiatedLog, inodeId)
		}
	}
	return forgotten
}

// Get a file handle in a thread safe way
func (qfs *QuantumFs) fileHandle(c *ctx, id FileHandleId) FileHandle {
	fileHandle, _ := qfs.fileHandles.Load(id)
	if fileHandle == nil {
		return nil
	}

	return fileHandle.(FileHandle)
}

// Set a file handle in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setFileHandle(c *ctx, id FileHandleId, fileHandle FileHandle) {
	defer c.funcIn("Mux::setFileHandle").Out()

	qfs.setFileHandle_(c, id, fileHandle)
}

// Must hold mapMutex exclusively
func (qfs *QuantumFs) setFileHandle_(c *ctx, id FileHandleId,
	fileHandle FileHandle) {

	if fileHandle != nil {
		qfs.fileHandles.Store(id, fileHandle)
	} else {
		// clean up any remaining response queue size from the apiFileSize
		fh, _ := qfs.fileHandles.Load(id)
		if api, ok := fh.(*ApiHandle); ok {
			api.drainResponseData(c)
		}

		qfs.fileHandles.Delete(id)
	}
}

func (qfs *QuantumFs) newInodeIdInfo(id InodeId) InodeIdInfo {
	rtn := InodeIdInfo {
		id:		id,
	}

	if id != quantumfs.InodeIdInvalid {
		// We increment a generation counter since its faster than a
		// random number function. We don't care if this wraps, only that it
		// is never zero so we can detect undefined generation. We need this
		// generation counter to prevent the kernel from confusing inodes
		// when we reuse inode ids.
		newGen := atomic.AddUint64(&qfs.inodeIdGeneration, 1)
		if newGen == 0 {
			newGen = atomic.AddUint64(&qfs.inodeIdGeneration, 1)
			utils.Assert(newGen != 0,
				"Double add resulted in zero both times")
		}
		rtn.generation = newGen
	}

	qfs.c.vlog("Generation for inode %d is %d", id, rtn.generation)

	return rtn
}

// Retrieves a unique inode number
func (qfs *QuantumFs) newInodeId() InodeIdInfo {
	for {
		// When we get a new id, it's possible that we're still using it.
		// If it's still in use, grab another one instead
		newId, reused := qfs.inodeIds.newInodeId(&qfs.c)

		// If this is a reused id, then it's safe to use immediately
		if reused {
			return qfs.newInodeIdInfo(newId)
		}

		// We expect that ids will be reused more often than not. To optimize
		// for that case, we only lock the map mutex when we absolutely must
		idIsFree := func() bool {
			defer qfs.mapMutex.Lock().Unlock()

			inode, inodeIdUsed := qfs.getInode_(&qfs.c, newId)
			return inode == nil && !inodeIdUsed
		}()

		if !idIsFree {
			continue
		}
		return qfs.newInodeIdInfo(newId)
	}
}

// Retrieve a unique filehandle number
func (qfs *QuantumFs) newFileHandleId() FileHandleId {
	return FileHandleId(atomic.AddUint64(&qfs.fileHandleNum, 1))
}

// Trigger all active workspaces to sync
const SyncAllLog = "Mux::syncAll"

func (qfs *QuantumFs) syncAll(c *ctx) error {
	defer c.statsFuncIn(SyncAllLog).Out()
	return qfs.flusher.syncAll(c)
}

// Sync the workspace keyed with key
const SyncWorkspaceLog = "Mux::syncWorkspace"

func (qfs *QuantumFs) syncWorkspace(c *ctx, workspace string) error {
	defer c.statsFuncIn(SyncWorkspaceLog).Out()

	parts := strings.Split(workspace, "/")
	ids, err := qfs.getWsrLineageNoInstantiate(c, parts[0], parts[1], parts[2])
	if err != nil {
		return errors.New("Unable to get WorkspaceRoot for Sync")
	}

	if len(ids) < 4 {
		// not instantiated yet, so nothing to sync
		return nil
	}

	inode := qfs.inodeNoInstantiate(c, ids[3])
	if inode == nil {
		return nil
	}

	wsr := inode.(*WorkspaceRoot)
	defer wsr.LockTree().Unlock()

	err = qfs.flusher.syncWorkspace_(c, workspace)
	if err != nil {
		return err
	}

	wsr.refresh_(c)
	return nil
}

func logRequestPanic(c *ctx) {
	exception := recover()
	if exception == nil {
		return
	}

	stackTrace := debug.Stack()

	c.elog("PANIC (%d): '"+fmt.Sprintf("%v", exception)+
		"' BT: %v", c.RequestId, utils.BytesToString(stackTrace))
}

const FuseRequestWorkspace = "FUSE Request Workspace: %s"

func logInodeWorkspace(c *ctx, inode Inode) {
	wsName := "none"
	if inode != nil {
		wsName = inode.treeState().name
	}
	c.vlog(FuseRequestWorkspace, wsName)
}

func logFilehandleWorkspace(c *ctx, filehandle FileHandle) {
	wsName := "none"
	if filehandle != nil {
		wsName = filehandle.treeState().name
	}
	c.vlog(FuseRequestWorkspace, wsName)
}

const LookupLog = "Mux::Lookup"

func (qfs *QuantumFs) Lookup(header *fuse.InHeader, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(LookupLog, InodeNameLog, header.NodeId, name).Out()

	if isFilenameTooLong(name) {
		return ENAMETOOLONG
	}
	return qfs.lookupCommon(c, InodeId(header.NodeId), name, out)
}

func (qfs *QuantumFs) lookupCommon(c *ctx, inodeId InodeId, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("Mux::lookupCommon", "inode %d name %s", inodeId, name).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, inodeId)
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.dlog("Obsolete inode")
		return fuse.ENOENT
	}

	return inode.Lookup(c, name, out)
}

// Returns the inode id of the root, the typespace, the namespace and the workspace
func (qfs *QuantumFs) getWsrLineageNoInstantiate(c *ctx,
	typespace, namespace, workspace string) (ids []InodeId, err error) {

	defer c.FuncIn("QuantumFs::getWsrLineageNoInstantiate", "%s/%s/%s",
		typespace, namespace, workspace).Out()

	var id InodeId
	var exists bool

	ids = append(ids, quantumfs.InodeIdRoot)
	inode := qfs.inodeNoInstantiate(c, quantumfs.InodeIdRoot)
	if inode == nil {
		return nil, fmt.Errorf("root inode not instantiated")
	}
	typespacelist, ok := inode.(*TypespaceList)
	if !ok {
		return nil, fmt.Errorf("bad typespacelist")
	}
	keepSearching := func() bool {
		defer typespacelist.RLock().RUnlock()
		idInfo, exists := typespacelist.typespacesByName[typespace]
		id = idInfo.id
		if !exists {
			c.vlog("typespace %s does not exist", typespace)
			return false
		}
		return true
	}()
	if !keepSearching {
		return
	}

	ids = append(ids, id)
	inode = qfs.inodeNoInstantiate(c, id)
	if inode == nil {
		c.vlog("typespacelist inode %d not instantiated", id)
		return
	}
	namespacelist, ok := inode.(*NamespaceList)
	if !ok {
		return nil, fmt.Errorf("bad namespacelist")
	}
	keepSearching = func() bool {
		defer namespacelist.RLock().RUnlock()
		idInfo, exists := namespacelist.namespacesByName[namespace]
		id = idInfo.id
		if !exists {
			return false
		}
		return true
	}()
	if !keepSearching {
		return
	}

	ids = append(ids, id)
	inode = qfs.inodeNoInstantiate(c, id)
	if inode == nil {
		c.vlog("namespacelist inode %d not instantiated", id)
		return
	}
	workspacelist, ok := inode.(*WorkspaceList)
	if !ok {
		return nil, fmt.Errorf("bad workspacelist")
	}
	var wsrInfo workspaceInfo
	keepSearching = func() bool {
		defer workspacelist.RLock().RUnlock()
		wsrInfo, exists = workspacelist.workspacesByName[workspace]
		if !exists {
			return false
		}
		return true
	}()
	if !keepSearching {
		return
	}

	ids = append(ids, wsrInfo.id.id)
	return
}

func (qfs *QuantumFs) getWsrLineage(c *ctx,
	typespace, namespace, workspace string) (ids []InodeId, cleanup func()) {

	defer c.FuncIn("QuantumFs::getWsrLineage", "%s/%s/%s",
		typespace, namespace, workspace).Out()

	// In order to run getWsrLineage, we must set a proper value for
	// the variable nLookup. If the function is called internally, it needs to
	// reduce the increased lookupCount, so set nLookup to 1. Only if it is
	// triggered by kernel, should lookupCount be increased by one, and nLookup
	// should be 0. Therefore, lookupCount's in QuantumFS and kernel can match.
	//
	// For now, all getWorkspaceRoot() are called from internal functions, so
	// nLookup is always 1.
	var nLookup uint64 = 1
	// Before workspace root is successfully instantiated, there is no need to
	// uninstantiated it, so cleanup() should be a no-op
	// Get the WorkspaceList Inode number
	var typespaceAttr fuse.EntryOut
	ids = append(ids, quantumfs.InodeIdRoot)
	cleanup = func() {}

	result := qfs.lookupCommon(c, quantumfs.InodeIdRoot, typespace,
		&typespaceAttr)
	if result != fuse.OK {
		return
	}
	ids = append(ids, InodeId(typespaceAttr.NodeId))
	cleanup = func() {
		qfs.Forget(typespaceAttr.NodeId, nLookup)
	}

	var namespaceAttr fuse.EntryOut
	result = qfs.lookupCommon(c, InodeId(typespaceAttr.NodeId), namespace,
		&namespaceAttr)
	if result != fuse.OK {
		return
	}
	ids = append(ids, InodeId(namespaceAttr.NodeId))
	cleanup = func() {
		qfs.Forget(namespaceAttr.NodeId, nLookup)
		qfs.Forget(typespaceAttr.NodeId, nLookup)
	}

	// Get the WorkspaceRoot Inode number
	var workspaceRootAttr fuse.EntryOut
	result = qfs.lookupCommon(c, InodeId(namespaceAttr.NodeId), workspace,
		&workspaceRootAttr)
	if result != fuse.OK {
		return
	}
	ids = append(ids, InodeId(workspaceRootAttr.NodeId))
	return
}

// The returned cleanup function of workspaceroot should be called at the end of the
// caller
func (qfs *QuantumFs) getWorkspaceRoot(c *ctx, typespace, namespace,
	workspace string) (*WorkspaceRoot, func(), bool) {

	defer c.FuncIn("QuantumFs::getWorkspaceRoot", "Workspace %s/%s/%s",
		typespace, namespace, workspace).Out()
	ids, cleanup := qfs.getWsrLineage(c, typespace, namespace, workspace)
	defer cleanup()
	if len(ids) != 4 {
		c.vlog("Workspace inode not found")
		return nil, func() {}, false
	}
	wsrInode := ids[3]
	c.vlog("Instantiating workspace inode %d", wsrInode)
	inode, release := qfs.inode(c, wsrInode)
	if inode == nil {
		release()
		return nil, func() {}, false
	}
	wsrCleanup := func() {
		release()
		qfs.Forget(uint64(wsrInode), 1)
	}
	return inode.(*WorkspaceRoot), wsrCleanup, true
}

func (qfs *QuantumFs) workspaceIsMutable(c *ctx, inode Inode) bool {
	defer c.FuncIn("Mux::workspaceIsMutable", "inode %d", inode.inodeNum()).Out()

	var wsr *WorkspaceRoot
	switch inode.(type) {
	// The default cases will be inode such as file, symlink, hardlink etc, they
	// get workspaceroots from their parents.
	default:
		defer inode.getParentLock().RLock().RUnlock()
		// if inode is already forgotten, the workspace doesn't process it.
		if inode.isOrphaned_() {
			return true
		}
		parent, release := inode.parent_(c)
		defer release()

		switch parent.(type) {
		default:
			panic(fmt.Sprintf("The inode type is unexpected: %v",
				reflect.TypeOf(parent)))
		case *WorkspaceList:
			return true
		case *WorkspaceRoot:
			wsr = parent.(*WorkspaceRoot)
		case *Directory:
			wsr = parent.(*Directory).
				hardlinkTable.getWorkspaceRoot()
		}
	case *WorkspaceRoot:
		wsr = inode.(*WorkspaceRoot)
	case *Directory:
		wsr = inode.(*Directory).hardlinkTable.getWorkspaceRoot()
	case *TypespaceList:
		// If the inode is typespace/namespace/workspace/api, return true
		// immediately since workspaceroot shouldn't have authority over them
		return true
	case *NamespaceList:
		return true
	case *WorkspaceList:
		return true
	case *ApiInode:
		return true
	}

	defer qfs.mutabilityLock.RLock().RUnlock()

	key := wsr.typespace + "/" + wsr.namespace + "/" + wsr.workspace
	mutability, exists := qfs.workspaceMutability[key]
	if !exists || mutability != workspaceMutable {
		return false
	}

	return true

}

func (qfs *QuantumFs) setWorkspaceImmutable(wsr string) {

	defer qfs.mutabilityLock.RLock().RUnlock()

	qfs.workspaceMutability[wsr] = workspaceImmutable
}

func (qfs *QuantumFs) invalidateInode(c *ctx, inodeId InodeId) {
	qfs.toNotifyFuse <- FuseNotification{
		c:     c,
		op:    NotifyFuseInvalid,
		inode: inodeId,
	}
}

func (qfs *QuantumFs) noteDeletedInode(c *ctx, parentId InodeId, childId InodeId,
	name string) {

	qfs.toNotifyFuse <- FuseNotification{
		c:      c,
		op:     NotifyFuseDeleted,
		parent: parentId,
		child:  childId,
		name:   name,
	}
}

func (qfs *QuantumFs) noteChildCreated(c *ctx, parentId InodeId, name string) {
	qfs.toNotifyFuse <- FuseNotification{
		c:      c,
		op:     NotifyFuseCreated,
		parent: parentId,
		name:   name,
	}
}

func (qfs *QuantumFs) workspaceIsMutableAtOpen(c *ctx, inode Inode,
	flags uint32) bool {

	defer c.FuncIn("Mux::workspaceIsMutableAtOpen", "flags %d", flags).Out()

	// Only if the Open() requires write permission, is it blocked by the
	// read-only workspace
	if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return true
	}

	return qfs.workspaceIsMutable(c, inode)
}

const ForgetLog = "Mux::Forget"

func (qfs *QuantumFs) Forget(nodeID uint64, nlookup uint64) {
	c := qfs.c.forgetCtx()
	defer c.statsFuncIn(ForgetLog).Out()
	defer logRequestPanic(c)

	c.vlog("Forget called on inode %d Looked up %d Times", nodeID, nlookup)

	inodeId := InodeId(nodeID)

	if !qfs.shouldForget(c, inodeId, nlookup) {
		c.vlog("inode %d lookup not zero yet", inodeId)
	}
}

const GetAttrLog = "Mux::GetAttr"

func (qfs *QuantumFs) GetAttr(input *fuse.GetAttrIn,
	out *fuse.AttrOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(GetAttrLog, InodeOnlyLog, input.NodeId).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	return inode.GetAttr(c, out)
}

const SetAttrLog = "Mux::SetAttr"

func (qfs *QuantumFs) SetAttr(input *fuse.SetAttrIn,
	out *fuse.AttrOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(SetAttrLog, SetAttrArgLog, input.NodeId,
		input.Valid, input.Size).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.SetAttr(c, input, out)
}

const MknodLog = "Mux::Mknod"

func (qfs *QuantumFs) Mknod(input *fuse.MknodIn, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(MknodLog, InodeNameLog, input.NodeId, name).Out()

	if isFilenameTooLong(name) {
		return ENAMETOOLONG
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Mknod(c, name, input, out)
}

const MkdirLog = "Mux::Mkdir"

func (qfs *QuantumFs) Mkdir(input *fuse.MkdirIn, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(MkdirLog, InodeNameLog, input.NodeId, name).Out()

	if isFilenameTooLong(name) {
		return ENAMETOOLONG
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Mkdir(c, name, input, out)
}

const UnlinkLog = "Mux::Unlink"

func (qfs *QuantumFs) Unlink(header *fuse.InHeader,
	name string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(UnlinkLog, InodeNameLog, header.NodeId, name).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Unlink(c, name)
}

const RmdirLog = "Mux::Rmdir"

func (qfs *QuantumFs) Rmdir(header *fuse.InHeader,
	name string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(RmdirLog, InodeNameLog, header.NodeId, name).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Rmdir(c, name)
}

const RenameLog = "Mux::Rename"
const RenameDebugLog = "Inode %d newdir %d %s -> %s"

func (qfs *QuantumFs) Rename(input *fuse.RenameIn, oldName string,
	newName string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(RenameLog, RenameDebugLog, input.NodeId, input.Newdir,
		oldName, newName).Out()

	if isFilenameTooLong(oldName) || isFilenameTooLong(newName) {
		return ENAMETOOLONG
	}

	srcInode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, srcInode)
	if srcInode == nil {
		c.vlog("Obsolete src inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, srcInode) {
		return fuse.EROFS
	}

	if input.NodeId == input.Newdir {
		return srcInode.RenameChild(c, oldName, newName)
	} else {
		dstInode, release := qfs.inode(c, InodeId(input.Newdir))
		defer release()
		if dstInode == nil {
			c.vlog("Obsolete dst inode")
			return fuse.ENOENT
		}

		if dstInode.treeState() != srcInode.treeState() {
			dstInode, unlock := qfs.RLockTreeGetInode(c,
				InodeId(input.Newdir))
			defer unlock()

			// In case it was deleted prior to grabbing the other
			// workspace treelock.
			if dstInode == nil {
				c.vlog("Obsolete dst inode")
				return fuse.ENOENT
			}
		}

		if !qfs.workspaceIsMutable(c, dstInode) {
			return fuse.EROFS
		}

		return srcInode.MvChild(c, dstInode, oldName, newName)
	}
}

const LinkLog = "Mux::Link"
const LinkDebugLog = "inode %d to name %s in dstDir %d"

func (qfs *QuantumFs) Link(input *fuse.LinkIn, filename string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(LinkLog, LinkDebugLog, input.Oldnodeid, filename,
		input.NodeId).Out()

	if isFilenameTooLong(filename) {
		return ENAMETOOLONG
	}

	srcInode, release := qfs.inode(c, InodeId(input.Oldnodeid))
	defer release()
	logInodeWorkspace(c, srcInode)
	if srcInode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	dstInode, release := qfs.inode(c, InodeId(input.NodeId))
	defer release()
	if dstInode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, dstInode) {
		return fuse.EROFS
	}

	// Via races, srcInode and dstInode can be forgotten here

	if srcInode.treeState() == dstInode.treeState() {
		// If src and dst live in the same workspace, we only need one lock
		defer dstInode.LockTree().Unlock()
	} else {
		// When we have to lock multiple inodes, we must make sure that we
		// lock the inodes in correct sequence to prevent deadlock
		firstLock, lastLock := getLockOrder(dstInode, srcInode)
		defer firstLock.LockTree().Unlock()
		defer lastLock.LockTree().Unlock()
	}

	return dstInode.link_DOWN(c, srcInode, filename, out)
}

const SymlinkLog = "Mux::Symlink"

func (qfs *QuantumFs) Symlink(header *fuse.InHeader, pointedTo string,
	linkName string, out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(SymlinkLog, InodeNameLog, header.NodeId, linkName).Out()

	if isFilenameTooLong(linkName) {
		return ENAMETOOLONG
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Symlink(c, pointedTo, linkName, out)
}

const ReadlinkLog = "Mux::Readlink"

func (qfs *QuantumFs) Readlink(header *fuse.InHeader) (out []byte,
	result fuse.Status) {

	out = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(ReadlinkLog, InodeOnlyLog, header.NodeId).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return nil, fuse.ENOENT
	}

	return inode.Readlink(c)
}

const AccessLog = "Mux::Access"

func (qfs *QuantumFs) Access(input *fuse.AccessIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(AccessLog, InodeOnlyLog, input.NodeId).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	return inode.Access(c, input.Mask, input.Uid, input.Gid)
}

func isPosixAclName(name string) bool {
	if name == "system.posix_acl_access" || name == "system.posix_acl_default" {
		return true
	}

	return false
}

func isFilenameTooLong(name string) bool {
	return len(name) > quantumfs.MaxFilenameLength
}

const GetXAttrSizeLog = "Mux::GetXAttrSize"

func (qfs *QuantumFs) GetXAttrSize(header *fuse.InHeader, attr string) (size int,
	result fuse.Status) {

	size = 0
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(GetXAttrSizeLog, InodeOnlyLog, header.NodeId).Out()

	if isPosixAclName(attr) {
		return 0, fuse.EINVAL
	}

	if strings.HasPrefix(attr, quantumfs.XAttrTypePrefix) {
		if attr == quantumfs.XAttrTypeKey {
			_, status := getQuantumfsExtendedKey(c, qfs,
				InodeId(header.NodeId))
			if status != fuse.OK {
				return 0, status
			}
			return quantumfs.ExtendedKeyLength, status
		}
		return 0, fuse.ENODATA
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return 0, fuse.ENOENT
	}

	return inode.GetXAttrSize(c, attr)
}

func getQuantumfsExtendedKey(c *ctx, qfs *QuantumFs, inodeId InodeId) ([]byte,
	fuse.Status) {

	defer c.FuncIn("getQuantumfsExtendedKey", "inode %d", inodeId).Out()

	inode, unlock := qfs.LockTreeGetInode(c, inodeId)
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return nil, fuse.ENOENT
	}

	if inode.isWorkspaceRoot() {
		c.vlog("Parent is workspaceroot, returning")
		return nil, fuse.ENOATTR
	}

	// Update the Hash value before generating the key
	inode.Sync_DOWN(c)

	return inode.getQuantumfsExtendedKey(c)
}

const GetXAttrDataLog = "Mux::GetXAttrData"

func (qfs *QuantumFs) GetXAttrData(header *fuse.InHeader, attr string) (data []byte,
	result fuse.Status) {

	data = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(GetXAttrDataLog, InodeOnlyLog, header.NodeId).Out()

	if isPosixAclName(attr) {
		return nil, fuse.EINVAL
	}

	if strings.HasPrefix(attr, quantumfs.XAttrTypePrefix) {
		if attr == quantumfs.XAttrTypeKey {
			return getQuantumfsExtendedKey(c, qfs,
				InodeId(header.NodeId))
		}
		return nil, fuse.ENODATA
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return nil, fuse.ENOENT
	}

	return inode.GetXAttrData(c, attr)
}

const ListXAttrLog = "Mux::ListXAttr"

func (qfs *QuantumFs) ListXAttr(header *fuse.InHeader) (attributes []byte,
	result fuse.Status) {

	attributes = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(ListXAttrLog, InodeOnlyLog, header.NodeId).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return nil, fuse.ENOENT
	}

	return inode.ListXAttr(c)
}

const SetXAttrLog = "Mux:SetXAttr"

func (qfs *QuantumFs) SetXAttr(input *fuse.SetXAttrIn, attr string,
	data []byte) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(SetXAttrLog, InodeOnlyLog, input.NodeId).Out()

	if isPosixAclName(attr) {
		return fuse.EINVAL
	}

	if strings.HasPrefix(attr, quantumfs.XAttrTypePrefix) {
		// quantumfs keys are immutable from userspace
		return fuse.EPERM
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.SetXAttr(c, attr, data)
}

const RemoveXAttrLog = "Mux::RemoveXAttr"

func (qfs *QuantumFs) RemoveXAttr(header *fuse.InHeader,
	attr string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn(RemoveXAttrLog, InodeOnlyLog, header.NodeId).Out()

	if strings.HasPrefix(attr, quantumfs.XAttrTypePrefix) {
		// quantumfs keys are immutable from userspace
		return fuse.EPERM
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.RemoveXAttr(c, attr)
}

const CreateLog = "Mux::Create"

func (qfs *QuantumFs) Create(input *fuse.CreateIn, name string,
	out *fuse.CreateOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(CreateLog, InodeNameLog, input.NodeId, name).Out()

	if isFilenameTooLong(name) {
		return ENAMETOOLONG
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.elog("Create failed", input)
		return fuse.EACCES // TODO Confirm this is correct
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Create(c, input, name, out)
}

const OpenLog = "Mux::Open"

func (qfs *QuantumFs) Open(input *fuse.OpenIn,
	out *fuse.OpenOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(OpenLog, InodeOnlyLog, input.NodeId).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutableAtOpen(c, inode, input.Flags) {
		return fuse.EROFS
	}

	return inode.Open(c, input.Flags, input.Mode, out)
}

const ReadLog = "Mux::Read"

func (qfs *QuantumFs) Read(input *fuse.ReadIn, buf []byte) (readRes fuse.ReadResult,
	result fuse.Status) {

	readRes = nil
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(ReadLog, FileHandleLog, input.Fh).Out()

	fileHandle, unlock := qfs.RLockTreeGetHandle(c, FileHandleId(input.Fh))
	defer unlock()
	logFilehandleWorkspace(c, fileHandle)
	if fileHandle == nil {
		c.elog("Read failed %d", input.Fh)
		return nil, fuse.ENOENT
	}

	return fileHandle.Read(c, input.Offset, input.Size,
		buf, utils.BitFlagsSet(uint(input.Flags), uint(syscall.O_NONBLOCK)))
}

const ReleaseLog = "Mux::Release"

func (qfs *QuantumFs) Release(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(ReleaseLog, FileHandleLog, input.Fh).Out()

	qfs.toBeReleased <- input.Fh
}

const WriteLog = "Mux::Write"

func (qfs *QuantumFs) Write(input *fuse.WriteIn, data []byte) (written uint32,
	result fuse.Status) {

	written = 0
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(WriteLog, FileHandleLog, input.Fh).Out()

	fileHandle, unlock := qfs.RLockTreeGetHandle(c, FileHandleId(input.Fh))
	defer unlock()
	logFilehandleWorkspace(c, fileHandle)
	if fileHandle == nil {
		c.elog("Write failed")
		return 0, fuse.ENOENT
	}

	return fileHandle.Write(c, input.Offset, input.Size,
		input.Flags, data)
}

const FlushLog = "Mux::Flush"
const FlushDebugLog = "Fh %v Context %d %d %d"

func (qfs *QuantumFs) Flush(input *fuse.FlushIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(FlushLog, FlushDebugLog, input.Fh, input.Context.Uid,
		input.Context.Gid, input.Context.Pid).Out()

	return fuse.OK
}

const FsyncLog = "Mux::Fsync"

func (qfs *QuantumFs) Fsync(input *fuse.FsyncIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(FsyncLog, FileHandleLog, input.Fh).Out()

	return fuse.ENOSYS
}

const FallocateLog = "Mux::Fallocate"

func (qfs *QuantumFs) Fallocate(input *fuse.FallocateIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.statsFuncIn(FallocateLog).Out()

	c.wlog("Unhandled request Fallocate")
	return fuse.ENOSYS
}

const OpenDirLog = "Mux::OpenDir"

func (qfs *QuantumFs) OpenDir(input *fuse.OpenIn,
	out *fuse.OpenOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(OpenDirLog, InodeOnlyLog, input.NodeId).Out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock()
	logInodeWorkspace(c, inode)
	if inode == nil {
		c.vlog("Obsolete inode")
		return fuse.ENOENT
	}

	return inode.OpenDir(c, input.Flags, input.Mode, out)
}

const ReadDirLog = "Mux::ReadDir"

func (qfs *QuantumFs) ReadDir(input *fuse.ReadIn,
	out *fuse.DirEntryList) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(ReadDirLog, FileOffsetLog, input.Fh, input.Offset).Out()

	c.elog("Unhandled request ReadDir")
	return fuse.ENOSYS
}

const ReadDirPlusLog = "Mux::ReadDirPlus"

func (qfs *QuantumFs) ReadDirPlus(input *fuse.ReadIn,
	out *fuse.DirEntryList) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(ReadDirPlusLog, FileOffsetLog, input.Fh,
		input.Offset).Out()

	fileHandle, unlock := qfs.RLockTreeGetHandle(c, FileHandleId(input.Fh))
	defer unlock()
	logFilehandleWorkspace(c, fileHandle)
	if fileHandle == nil {
		c.elog("ReadDirPlus failed", fileHandle)
		return fuse.ENOENT
	}

	return fileHandle.ReadDirPlus(c, input, out)
}

const ReleaseDirLog = "Mux::ReleaseDir"

func (qfs *QuantumFs) ReleaseDir(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(ReleaseDirLog, FileHandleLog, input.Fh).Out()

	qfs.toBeReleased <- input.Fh
}

const FsyncDirLog = "Mux::FsyncDir"

func (qfs *QuantumFs) FsyncDir(input *fuse.FsyncIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.StatsFuncIn(FsyncDirLog, FileHandleLog, input.Fh).Out()

	return fuse.ENOSYS
}

const StatFsLog = "Mux::StatFs"

func (qfs *QuantumFs) StatFs(input *fuse.InHeader,
	out *fuse.StatfsOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(input)
	defer logRequestPanic(c)
	defer c.statsFuncIn(StatFsLog).Out()

	out.Blocks = 2684354560 // 10TB
	out.Bfree = out.Blocks / 2
	out.Bavail = out.Bfree
	out.Files = 0
	out.Ffree = math.MaxUint64
	out.Bsize = uint32(qfsBlockSize)
	out.NameLen = uint32(quantumfs.MaxFilenameLength)
	out.Frsize = 0

	return fuse.OK
}

func (qfs *QuantumFs) Init(*fuse.Server) {
}

func (qfs *QuantumFs) increaseApiFileSize(c *ctx, offset int) {
	result := atomic.AddInt64(&qfs.apiFileSize, int64(offset))
	c.vlog("QuantumFs::APIFileSize adds %d upto %d", offset, result)
}

func (qfs *QuantumFs) decreaseApiFileSize(c *ctx, offset int) {
	result := atomic.AddInt64(&qfs.apiFileSize, -1*int64(offset))
	c.vlog("QuantumFs::APIFileSize subtract %d downto %d", offset, result)
	if result < 0 {
		c.elog("PANIC Global variable %d should"+
			" be greater than zero", result)
		atomic.StoreInt64(&qfs.apiFileSize, 0)
	}
}
