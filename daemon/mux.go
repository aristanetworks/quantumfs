// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// go-fuse creates a goroutine for every request. The code here simply takes these
// requests and forwards them to the correct Inode.
package daemon

import "reflect"
import "container/list"
import "fmt"
import "io/ioutil"
import "math"
import "runtime/debug"
import "syscall"
import "sync"
import "sync/atomic"
import "time"

import "github.com/aristanetworks/quantumfs"
import "github.com/aristanetworks/quantumfs/qlog"
import "github.com/aristanetworks/quantumfs/utils"
import "github.com/hanwen/go-fuse/fuse"

const defaultCacheSize = 32768
const flushSanityTimeout = time.Minute

type dirtyInode struct {
	inode               Inode
	shouldUninstantiate bool
	expiryTime          time.Time
}

func NewQuantumFs_(config QuantumFsConfig, qlogIn *qlog.Qlog) *QuantumFs {
	qfs := &QuantumFs{
		RawFileSystem:          fuse.NewDefaultRawFileSystem(),
		config:                 config,
		inodes:                 make(map[InodeId]Inode),
		fileHandles:            make(map[FileHandleId]FileHandle),
		inodeNum:               quantumfs.InodeIdReservedEnd,
		fileHandleNum:          quantumfs.InodeIdReservedEnd,
		dirtyQueue:             make(map[*sync.RWMutex]*list.List),
		kickFlush:              make(chan struct{}, 1),
		flushAll:               make(chan *ctx),
		flushComplete:          make(chan struct{}),
		parentOfUninstantiated: make(map[InodeId]InodeId),
		lookupCounts:           make(map[InodeId]uint64),
		workspaceMutability:    make(map[string]workspaceState),
		c: ctx{
			Ctx: quantumfs.Ctx{
				Qlog:      qlogIn,
				RequestId: qlog.MuxReqId,
			},
			config:      &config,
			workspaceDB: config.WorkspaceDB,
			dataStore: newDataStore(config.DurableStore,
				defaultCacheSize),
		},
	}

	qfs.c.qfs = qfs

	typespaceList := NewTypespaceList()
	qfs.inodes[quantumfs.InodeIdRoot] = typespaceList
	qfs.inodes[quantumfs.InodeIdApi] = NewApiInode(typespaceList.treeLock(),
		typespaceList.inodeNum())
	return qfs
}

func NewQuantumFsLogs(config QuantumFsConfig, qlogIn *qlog.Qlog) *QuantumFs {
	return NewQuantumFs_(config, qlogIn)
}

func NewQuantumFs(config QuantumFsConfig) *QuantumFs {
	return NewQuantumFs_(config, qlog.NewQlogExt(config.CachePath,
		config.MemLogBytes, qlog.PrintToStdout))
}

type workspaceState int

const (
	workspaceImmutable workspaceState = iota
	workspaceMutable
	workspaceImmutableUntilRestart
)

type QuantumFs struct {
	fuse.RawFileSystem
	server        *fuse.Server
	config        QuantumFsConfig
	inodeNum      uint64
	fileHandleNum uint64
	c             ctx

	// We present the sum of the size of all responses waiting on the api file as
	// the size of that file because the kernel will clear any reads beyond what
	// is believed to be the file length. Thus the file length needs to be at
	// least as long as the largest response and using the sum of all response
	// lengths is more efficient than computing the maximum response length over
	// a large number of ApiHandles.
	apiFileSize int64

	mapMutex    utils.DeferableRwMutex
	inodes      map[InodeId]Inode
	fileHandles map[FileHandleId]FileHandle

	// This is a map from the treeLock to a list of dirty inodes. We use the
	// treelock because every Inode already has the treelock of its workspace so
	// this is an easy way to sort Inodes by workspace.
	//
	// The Front of the list are the Inodes next in line to flush.
	dirtyQueueLock utils.DeferableMutex
	dirtyQueue     map[*sync.RWMutex]*list.List

	// Notify the flusher that there is a new entry in the dirty queue
	kickFlush chan struct{}

	// Notify the flusher that all dirty inodes should be flushed
	flushAll chan *ctx

	// Notify whoever used flushAll that flushing is complete
	flushComplete chan struct{}

	// We must prevent instantiation of Inodes while we are uninstantiating an
	// Inode. This prevents a race between a Directory being uninstantiated as
	// one of its children is just being instantiated.
	//
	// If you are instantiating an Inode you only need to grab this lock for
	// reading. If you are uninstantiating you must grab it exclusively.
	//
	// This lock must always be grabbed before the mapMutex to ensure consistent
	// lock ordering.
	instantiationLock utils.DeferableRwMutex

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
}

func (qfs *QuantumFs) Serve(mountOptions fuse.MountOptions) error {
	qfs.c.dlog("QuantumFs::Serve Initializing server")
	server, err := fuse.NewServer(qfs, qfs.config.MountPath, &mountOptions)
	if err != nil {
		return err
	}

	stopFlushTimer := make(chan bool)
	flushTimerStopped := make(chan bool)

	go qfs.flusher(stopFlushTimer, flushTimerStopped)
	go qfs.adjustKernelKnobs()

	qfs.server = server
	qfs.c.dlog("QuantumFs::Serve Serving")
	qfs.server.Serve()
	qfs.c.dlog("QuantumFs::Serve Finished serving")

	qfs.c.dlog("QuantumFs::Serve Waiting for flush thread to end")
	stopFlushTimer <- true
	<-flushTimerStopped

	return nil
}

func (qfs *QuantumFs) flusher(quit chan bool, finished chan bool) {
	flusherContext := qfs.c.reqId(qlog.FlushReqId, nil)

	c := flusherContext

	// When we think we have no inodes try periodically anyways to ensure sanity
	nextExpiringInode := time.Now().Add(flushSanityTimeout)
	stop := false
	flushAll := false

	for {
		sleepTime := nextExpiringInode.Sub(time.Now())

		if sleepTime > flushSanityTimeout {
			c.elog("Overlong flusher sleepTime %s!", sleepTime)
			sleepTime = flushSanityTimeout
		}
		if sleepTime > 0 {
			c.vlog("Waiting until %s (%s)...",
				nextExpiringInode.String(), sleepTime.String())

			// If we've been directed to flushAll, use that caller's
			// context
			c = flusherContext

			stop = false
			flushAll = false

			select {
			case stop = <-quit:
				c.vlog("flusher woken up due to stop")
			case <-qfs.kickFlush:
				c.vlog("flusher woken up due to kick")
			case c = <-qfs.flushAll:
				flushAll = true
				c.vlog("flusher woken up due to syncAll")
			case <-time.After(sleepTime):
				c.vlog("flusher woken up due to timer")
			}
		}

		nextExpiringInode = func() time.Time {
			defer logRequestPanic(c)
			return qfs.flushDirtyLists(c, flushAll || stop)
		}()

		if flushAll {
			qfs.flushComplete <- struct{}{}
		}

		if stop {
			finished <- true
			return
		}
	}
}

func (qfs *QuantumFs) flushDirtyLists(c *ctx, flushAll bool) time.Time {
	defer c.FuncIn("Mux::flushDirtyLists", "flushAll %t", flushAll).out()

	defer qfs.dirtyQueueLock.Lock().Unlock()
	nextExpiringInode := time.Now().Add(flushSanityTimeout)

	for key, dirtyList := range qfs.dirtyQueue {
		func() {
			earliestNext := qfs.flushDirtyList_(c, dirtyList, flushAll)
			if earliestNext.Before(nextExpiringInode) {
				c.vlog("changing next time from %s to %s",
					nextExpiringInode.String(),
					earliestNext.String())
				nextExpiringInode = earliestNext
			}
		}()

		if dirtyList.Len() == 0 {
			delete(qfs.dirtyQueue, key)
		}
	}

	return nextExpiringInode
}

// Requires dirtyQueueLock
func (qfs *QuantumFs) flushDirtyList_(c *ctx, dirtyList *list.List,
	flushAll bool) time.Time {

	defer c.funcIn("Mux::flushDirtyList").out()

	for dirtyList.Len() > 0 {
		// Should we clean this inode?
		candidate := dirtyList.Front().Value.(*dirtyInode)

		now := time.Now()
		if !flushAll && candidate.expiryTime.After(now) {
			// We are up to date with this list
			return candidate.expiryTime
		}

		dirtyList.Remove(dirtyList.Front())

		func() {
			// We must release the dirtyQueueLock because when we flush
			// an Inode it will modify its parent and likely place that
			// parent onto the dirty queue. If we still hold that lock
			// we'll deadlock. We defer relocking in order to balance
			// against the deferred unlocking from our caller, even in
			// the case of a panic.
			qfs.dirtyQueueLock.Unlock()
			defer qfs.dirtyQueueLock.Lock()
			qfs.flushInode(c, *candidate)
		}()
	}

	// If we get here then we've emptied the dirtyList out entirely.
	return time.Now().Add(flushSanityTimeout)
}

func (qfs *QuantumFs) flushInode(c *ctx, dirtyInode dirtyInode) {
	inodeNum := dirtyInode.inode.inodeNum()
	defer c.FuncIn("Mux::flushInode", "inode %d, uninstantiate %t",
		inodeNum, dirtyInode.shouldUninstantiate).out()

	defer dirtyInode.inode.RLockTree().RUnlock()

	if !dirtyInode.inode.isOrphaned() {
		dirtyInode.inode.flush(c)
	}
	dirtyInode.inode.markClean()

	if dirtyInode.shouldUninstantiate {
		defer qfs.instantiationLock.Lock().Unlock()
		qfs.uninstantiateInode_(c, inodeNum)
	}
}

// Requires treeLock for read and the instantiationLock
func (qfs *QuantumFs) uninstantiateInode_(c *ctx, inodeNum InodeId) {
	defer c.FuncIn("Mux::uninstantiateInode_", "inode %d", inodeNum).out()

	inode := qfs.inodeNoInstantiate(c, inodeNum)
	if inode == nil || inodeNum == quantumfs.InodeIdRoot ||
		inodeNum == quantumfs.InodeIdApi {

		c.dlog("inode %d doesn't need to be forgotten", inodeNum)
		// Nothing to do
		return
	}

	qfs.uninstantiateChain_(c, inode)
}

// Don't use this method directly, use one of the semantically specific variants
// instead.
func (qfs *QuantumFs) _queueDirtyInode(c *ctx, inode Inode, shouldUninstantiate bool,
	shouldWait bool) *list.Element {

	defer c.FuncIn("Mux::_queueDirtyInode", "inode %d uninstantiate %t wait %t",
		inode.inodeNum(), shouldUninstantiate, shouldWait).out()

	defer qfs.dirtyQueueLock.Lock().Unlock()

	var dirtyNode *dirtyInode
	dirtyElement := inode.dirtyElement()
	if dirtyElement == nil {
		// This inode wasn't in the dirtyQueue so add it now
		dirtyNode = &dirtyInode{
			inode:               inode,
			shouldUninstantiate: shouldUninstantiate,
		}

		treelock := inode.treeLock()
		dirtyList, ok := qfs.dirtyQueue[treelock]
		if !ok {
			dirtyList = list.New()
			qfs.dirtyQueue[treelock] = dirtyList
		}

		if shouldWait {
			dirtyNode.expiryTime =
				time.Now().Add(qfs.config.DirtyFlushDelay)

			dirtyElement = dirtyList.PushBack(dirtyNode)
		} else {
			// dirtyInode.expiryTime will be the epoch
			dirtyElement = dirtyList.PushFront(dirtyNode)
		}
	} else {
		dirtyNode = dirtyElement.Value.(*dirtyInode)
		c.vlog("Inode was already in the dirty queue %s",
			dirtyNode.expiryTime.String())
		dirtyNode.expiryTime = time.Now()
	}

	if shouldUninstantiate {
		dirtyNode.shouldUninstantiate = true
	}

	select {
	case qfs.kickFlush <- struct{}{}:
		// We have successfully kicked the flusher
	default:
		// Somebody else had kicked the flusher already
	}

	return dirtyElement
}

// Queue an Inode to be flushed because it is dirty
func (qfs *QuantumFs) queueDirtyInode(c *ctx, inode Inode) *list.Element {
	return qfs._queueDirtyInode(c, inode, false, true)
}

// Queue an Inode because the kernel has forgotten about it
func (qfs *QuantumFs) queueInodeToForget(c *ctx, inode Inode) *list.Element {
	return qfs._queueDirtyInode(c, inode, true, false)
}

// There are several configuration knobs in the kernel which can affect FUSE
// performance. Don't depend on the system being configured correctly for QuantumFS,
// instead try to change the settings ourselves.
func (qfs *QuantumFs) adjustKernelKnobs() {
	qfs.c.funcIn("adjustKernelKnobs").out()

	mountId := findFuseConnection(&qfs.c, qfs.config.MountPath)
	if mountId == -1 {
		// We don't know where we are mounted, give up
		return
	}

	adjustBdi(&qfs.c, mountId)
}

func adjustBdi(c *ctx, mountId int) {
	defer c.funcIn("adjustBdi").out()

	// /sys/class/bdi/<mount>/read_ahead_kb indicates how much data, up to the
	// end of the file, should be speculatively read by the kernel. Setting this
	// to the block size should improve the correlation between the what the
	// kernel reads and what QuantumFS can provide most effeciently. Since this
	// is the amount in addition to the original read the kernel will read the
	// entire block containing the user's read and then some portion of the next
	// block. Thus QuantumFS will have time to fetch the next block in advance of
	// it being required.
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
func (qfs *QuantumFs) RLockTreeGetInode(c *ctx, inodeId InodeId) (Inode,
	utils.NeedReadUnlock) {

	inode := qfs.inode(c, inodeId)
	if inode == nil {
		return nil, &emptyUnlocker{}
	}

	inode.RLockTree()

	// once we have the lock, re-grab (and possibly reinstantiate) the inode
	// since it may have been just forgotten
	inode = qfs.inode(c, inodeId)
	return inode, inode.treeLock()
}

// Same as the RLockTreeGetInode, but for writes
func (qfs *QuantumFs) LockTreeGetInode(c *ctx, inodeId InodeId) (Inode,
	utils.NeedWriteUnlock) {
	inode := qfs.inode(c, inodeId)
	if inode == nil {
		return nil, &emptyUnlocker{}
	}

	inode.LockTree()

	inode = qfs.inode(c, inodeId)
	return inode, inode.treeLock()
}

func (qfs *QuantumFs) RLockTreeGetHandle(c *ctx, fh FileHandleId) (FileHandle,
	utils.NeedReadUnlock) {

	fileHandle := qfs.fileHandle(c, fh)
	if fileHandle == nil {
		return nil, &emptyUnlocker{}
	}

	fileHandle.RLockTree()

	// once we have the lock, re-grab
	fileHandle = qfs.fileHandle(c, fh)
	return fileHandle, fileHandle.treeLock()
}

func (qfs *QuantumFs) LockTreeGetHandle(c *ctx, fh FileHandleId) (FileHandle,
	utils.NeedWriteUnlock) {
	fileHandle := qfs.fileHandle(c, fh)
	if fileHandle == nil {
		return nil, &emptyUnlocker{}
	}

	fileHandle.LockTree()

	// once we have the lock, re-grab
	fileHandle = qfs.fileHandle(c, fh)
	return fileHandle, fileHandle.treeLock()
}

func (qfs *QuantumFs) inodeNoInstantiate(c *ctx, id InodeId) Inode {
	defer qfs.mapMutex.RLock().RUnlock()
	inode, _ := qfs.getInode_(c, id)
	return inode
}

// Get an inode in a thread safe way
func (qfs *QuantumFs) inode(c *ctx, id InodeId) Inode {
	// Handle the special case of invalid id
	if id == quantumfs.InodeIdInvalid {
		return nil
	}

	// First find the Inode under a cheaper lock
	inode := func() Inode {
		defer qfs.mapMutex.RLock().RUnlock()
		inode_, needsInstantiation := qfs.getInode_(c, id)
		if !needsInstantiation && inode_ != nil {
			return inode_
		} else {
			return nil
		}
	}()

	if inode != nil {
		return inode
	}

	// If we didn't find it, get the more expensive lock and check again. This
	// will instantiate the Inode if necessary and possible.
	defer qfs.instantiationLock.RLock().RUnlock()
	defer qfs.mapMutex.Lock().Unlock()

	inode = qfs.inode_(c, id)
	if inode == nil {
		msg := fmt.Sprintf("Unknown inodeId %d", id)
		panic(msg)
	}
	return inode
}

// Must hold the mapMutex for write
func (qfs *QuantumFs) inode_(c *ctx, id InodeId) Inode {
	inode, needsInstantiation := qfs.getInode_(c, id)
	if !needsInstantiation && inode != nil {
		return inode
	}

	c.vlog("Inode %d needs to be instantiated", id)

	parentId, uninstantiated := qfs.parentOfUninstantiated[id]
	if !uninstantiated {
		// We don't know anything about this Inode
		return nil
	}

	parent := qfs.inode_(c, parentId)
	if parent == nil {
		panic(fmt.Sprintf("Unable to instantiate parent required: %d",
			parentId))
	}

	inode, newUninstantiated := parent.instantiateChild(c, id)
	delete(qfs.parentOfUninstantiated, id)
	qfs.inodes[id] = inode
	qfs.addUninstantiated_(c, newUninstantiated, inode.inodeNum())

	return inode
}

// Set an inode in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setInode(c *ctx, id InodeId, inode Inode) {
	defer qfs.mapMutex.Lock().Unlock()

	if inode != nil {
		qfs.inodes[id] = inode
	} else {
		c.vlog("Clearing inode %d", id)
		delete(qfs.inodes, id)
	}
}

// Set a list of inode numbers to be uninstantiated with the given parent
func (qfs *QuantumFs) addUninstantiated(c *ctx, uninstantiated []InodeId,
	parent InodeId) {

	defer c.funcIn("Mux::addUninstantiated").out()

	if parent == 0 {
		panic("Invalid parentId in addUninstantiated")
	}

	defer qfs.mapMutex.Lock().Unlock()

	qfs.addUninstantiated_(c, uninstantiated, parent)
}

// Requires the mapMutex for writing
func (qfs *QuantumFs) addUninstantiated_(c *ctx, uninstantiated []InodeId,
	parent InodeId) {

	defer c.funcIn("Mux::addUninstantiated_").out()

	for _, inodeNum := range uninstantiated {
		qfs.parentOfUninstantiated[inodeNum] = parent
		c.vlog("Adding uninstantiated %d from %d (%d)", inodeNum, parent,
			len(qfs.parentOfUninstantiated))
	}
}

// Remove a list of inode numbers from the parentOfUninstantiated list
func (qfs *QuantumFs) removeUninstantiated(c *ctx, uninstantiated []InodeId) {
	defer c.funcIn("Mux::removeUninstantiated").out()
	defer qfs.mapMutex.Lock().Unlock()

	for _, inodeNum := range uninstantiated {
		delete(qfs.parentOfUninstantiated, inodeNum)
		c.vlog("Removing uninstantiated %d (%d)", inodeNum,
			len(qfs.parentOfUninstantiated))
	}
}

// Increase an Inode's lookup count. This must be called whenever a fuse.EntryOut is
// returned.
func (qfs *QuantumFs) increaseLookupCount(inodeId InodeId) {
	defer qfs.c.FuncIn("Mux::increaseLookupCount", "inode %d", inodeId).out()
	defer qfs.lookupCountLock.Lock().Unlock()
	prev, exists := qfs.lookupCounts[inodeId]
	if !exists {
		qfs.lookupCounts[inodeId] = 1
	} else {
		qfs.lookupCounts[inodeId] = prev + 1
	}
}

func (qfs *QuantumFs) lookupCount(inodeId InodeId) (uint64, bool) {
	defer qfs.c.FuncIn("Mux::lookupCount", "inode %d", inodeId).out()
	defer qfs.lookupCountLock.Lock().Unlock()
	lookupCount, exists := qfs.lookupCounts[inodeId]
	if !exists {
		return 0, false
	}

	return lookupCount, true
}

// Returns true if the count became zero or was previously zero
func (qfs *QuantumFs) shouldForget(inodeId InodeId, count uint64) bool {
	defer qfs.c.FuncIn("Mux::shouldForget", "inode %d count %d", inodeId,
		count).out()

	if inodeId == quantumfs.InodeIdApi || inodeId == quantumfs.InodeIdRoot {
		return false
	}

	defer qfs.lookupCountLock.Lock().Unlock()
	lookupCount, exists := qfs.lookupCounts[inodeId]
	if !exists {
		qfs.c.dlog("inode %d has not been instantiated", inodeId)
		return true
	}

	if lookupCount < count {
		qfs.c.elog("lookupCount less than zero %d %d", lookupCount, count)
	}

	lookupCount -= count
	qfs.lookupCounts[inodeId] = lookupCount
	if lookupCount == 0 {
		if count > 1 {
			qfs.c.dlog("Forgetting inode with lookupCount of %d", count)
		}
		return true
	} else {
		return false
	}
}

// Get a file handle in a thread safe way
func (qfs *QuantumFs) fileHandle(c *ctx, id FileHandleId) FileHandle {
	defer qfs.mapMutex.RLock().RUnlock()
	fileHandle := qfs.fileHandles[id]
	return fileHandle
}

// Set a file handle in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setFileHandle(c *ctx, id FileHandleId, fileHandle FileHandle) {
	defer c.funcIn("Mux::setFileHandle").out()

	defer qfs.mapMutex.Lock().Unlock()
	if fileHandle != nil {
		qfs.fileHandles[id] = fileHandle
	} else {
		// clean up any remaining response queue size from the apiFileSize
		fileHandle = qfs.fileHandles[id]
		if api, ok := fileHandle.(*ApiHandle); ok {
			api.drainResponseData(c)
		}

		delete(qfs.fileHandles, id)
	}
}

// Retrieve a unique inode number
func (qfs *QuantumFs) newInodeId() InodeId {
	return InodeId(atomic.AddUint64(&qfs.inodeNum, 1))
}

// Retrieve a unique filehandle number
func (qfs *QuantumFs) newFileHandleId() FileHandleId {
	return FileHandleId(atomic.AddUint64(&qfs.fileHandleNum, 1))
}

// Trigger all active workspaces to sync
func (qfs *QuantumFs) syncAll(c *ctx) {
	defer c.funcIn("Mux::syncAll").out()

	// Trigger the flusher goroutine to flush everything
	qfs.flushAll <- c

	<-qfs.flushComplete
}

func logRequestPanic(c *ctx) {
	exception := recover()
	if exception == nil {
		return
	}

	stackTrace := debug.Stack()

	c.elog("ERROR: PANIC serving request %d: '%s' Stacktrace: %v", c.RequestId,
		fmt.Sprintf("%v", exception), utils.BytesToString(stackTrace))
}

func (qfs *QuantumFs) Lookup(header *fuse.InHeader, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Lookup", "Inode %d Name %s", header.NodeId, name).out()
	return qfs.lookupCommon(c, InodeId(header.NodeId), name, out)
}

func (qfs *QuantumFs) lookupCommon(c *ctx, inodeId InodeId, name string,
	out *fuse.EntryOut) fuse.Status {

	defer c.FuncIn("Mux::lookupCommon", "inode %d name %s", inodeId, name).out()

	inode, unlock := qfs.RLockTreeGetInode(c, inodeId)
	defer unlock.RUnlock()
	if inode == nil {
		c.elog("Lookup failed", name)
		return fuse.ENOENT
	}

	return inode.Lookup(c, name, out)
}

// Needs treelock for read as well as the instantiationLock exclusively.
func (qfs *QuantumFs) uninstantiateChain_(c *ctx, inode Inode) {
	inodeNum := inode.inodeNum()
	defer c.FuncIn("Mux::uninstantiateChain_", "inode %d", inodeNum).out()

	inodeChildren := make([]InodeId, 0)
	initial := true
	for {
		inodeChildren = inodeChildren[:0]
		lookupCount, exists := qfs.lookupCount(inodeNum)
		if lookupCount != 0 {
			c.vlog("No forget called on inode %d yet", inodeNum)
			break
		}

		if dirtyElement := inode.dirtyElement(); dirtyElement != nil {
			c.vlog("Inode %d dirty, not uninstantiating yet",
				inodeNum)
			func() {
				defer qfs.dirtyQueueLock.Lock().Unlock()
				dirtyNode := dirtyElement.Value.(*dirtyInode)
				dirtyNode.shouldUninstantiate = true
			}()
			break
		}

		// If the loop is in the first iteration, we can treat the
		// non-existence of lookupCount as zero value and bypass the
		// if-statement
		if !exists && !initial {
			c.vlog("A inode %d with nil lookupCount "+
				"is uninstantiated by its child", inodeNum)
			break
		}
		initial = false

		if dir, isDir := inode.(inodeHolder); isDir {
			children := dir.directChildInodes()

			for _, i := range children {
				// To be fully unloaded, the child must have lookup
				// count of zero (no kernel refs) *and*
				// be uninstantiated
				lookupCount, _ = qfs.lookupCount(i)
				if lookupCount != 0 ||
					qfs.inodeNoInstantiate(c, i) != nil {

					// Not ready to forget, no more to do
					c.dlog("Not all children unloaded, %d in %d",
						i, inodeNum)
					return
				}
				c.dlog("Child %d of %d not loaded", i, inodeNum)
			}

			inodeChildren = append(inodeChildren, children...)
		}

		// Great, we want to forget this so proceed
		func() {
			defer qfs.lookupCountLock.Lock().Unlock()

			// With the lookupCountLock and instantiationLock both held
			// exclusively, no inodes may be instantiated and no lookups
			// finished. Thus we are safe to fully uninstantiate this
			// inode as long as there hasn't been a lookup between
			// starting to uninstantiate it and here.

			count, exists := qfs.lookupCounts[inodeNum]
			if exists && count == 0 {
				qfs.setInode(c, inodeNum, nil)
				delete(qfs.lookupCounts, inodeNum)
				qfs.removeUninstantiated(c, inodeChildren)
			}
		}()

		c.vlog("Set inode %d to nil", inodeNum)

		if !inode.isOrphaned() && inodeNum != quantumfs.InodeIdRoot {
			key := inode.flush(c)

			// Then check our parent and iterate again
			inode = func() (parent Inode) {
				defer inode.getParentLock().RLock().RUnlock()

				// Do nothing if we're orphaned
				if inode.isOrphaned_() {
					return nil
				}

				parent = qfs.inodeNoInstantiate(c,
					inode.parentId_())
				if parent == nil {
					panic(fmt.Sprintf("Parent was unloaded "+
						"before child! %d %d",
						inode.parentId_(), inodeNum))
				}

				parent.syncChild(c, inodeNum, key)

				qfs.addUninstantiated(c, []InodeId{inodeNum},
					inode.parentId_())

				return parent
			}()
			continue
		}
		break
	}
}

func (qfs *QuantumFs) getWorkspaceRoot(c *ctx, typespace, namespace,
	workspace string) (*WorkspaceRoot, bool) {

	defer c.FuncIn("QuantumFs::getWorkspaceRoot", "Workspace %s/%s/%s",
		typespace, namespace, workspace).out()

	// In order to run getWorkspaceRoot, we must set a proper value for the
	// variable nLookup. If the function is called internally, it needs to reduce
	// the increased lookupCount, so set nLookup to 1. Only if it is triggered by
	// kernel, should lookupCount be increased by one, and nLookup should be 0.
	// Therefore, lookupCount's in QuantumFS and kernel can match.
	//
	// For now, all getWorkspaceRoot() are called from internal functions, so
	// nLookup is always 1.
	var nLookup uint64 = 1

	// Get the WorkspaceList Inode number
	var typespaceAttr fuse.EntryOut
	result := qfs.lookupCommon(c, quantumfs.InodeIdRoot, typespace,
		&typespaceAttr)
	if result != fuse.OK {
		return nil, false
	}
	defer qfs.Forget(typespaceAttr.NodeId, nLookup)

	var namespaceAttr fuse.EntryOut
	result = qfs.lookupCommon(c, InodeId(typespaceAttr.NodeId), namespace,
		&namespaceAttr)
	if result != fuse.OK {
		return nil, false
	}
	defer qfs.Forget(namespaceAttr.NodeId, nLookup)

	// Get the WorkspaceRoot Inode number
	var workspaceRootAttr fuse.EntryOut
	result = qfs.lookupCommon(c, InodeId(namespaceAttr.NodeId), workspace,
		&workspaceRootAttr)
	if result != fuse.OK {
		return nil, false
	}
	defer qfs.Forget(workspaceRootAttr.NodeId, nLookup)

	// Fetch the WorkspaceRoot object itelf
	wsr := qfs.inode(c, InodeId(workspaceRootAttr.NodeId))

	return wsr.(*WorkspaceRoot), wsr != nil
}

func (qfs *QuantumFs) workspaceIsMutable(c *ctx, inode Inode) bool {
	defer c.FuncIn("Mux::workspaceIsMutable", "inode %d", inode.inodeNum()).out()

	var wsr *WorkspaceRoot
	switch inode.(type) {
	// The default cases will be inode such as file, symlink, hardlink etc, they
	// get workspaceroots from their parents.
	default:
		// if inode is already forgotten, the workspace doesn't process it.
		if inode.isOrphaned() {
			return true
		}
		// Otherwise, go up to its parent which must be a directory/workspace
		defer inode.getParentLock().RLock().RUnlock()
		parent := inode.parent_(c)
		switch parent.(type) {
		default:
			panic(fmt.Sprintf("The inode type is unexpected: %v",
				reflect.TypeOf(parent)))
		case *WorkspaceList:
			return true
		case *WorkspaceRoot:
			wsr = parent.(*WorkspaceRoot)
		case *Directory:
			wsr = parent.(*Directory).wsr
		}
	case *WorkspaceRoot:
		wsr = inode.(*WorkspaceRoot)
	case *Directory:
		wsr = inode.(*Directory).wsr
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

func (qfs *QuantumFs) workspaceIsMutableAtOpen(c *ctx, inode Inode,
	flags uint32) bool {

	defer c.FuncIn("Mux::workspaceIsMutableAtOpen", "flags %d", flags).out()

	// Only if the Open() requires write permission, is it blocked by the
	// read-only workspace
	if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return true
	}

	return qfs.workspaceIsMutable(c, inode)
}

func (qfs *QuantumFs) Forget(nodeID uint64, nlookup uint64) {
	defer qfs.c.funcIn("Mux::Forget").out()
	defer logRequestPanic(&qfs.c)

	qfs.c.dlog("Forget called on inode %d Looked up %d Times", nodeID, nlookup)

	if !qfs.shouldForget(InodeId(nodeID), nlookup) {
		// The kernel hasn't completely forgotten this Inode. Keep it around
		// a while longer.
		qfs.c.dlog("inode %d lookup not zero yet", nodeID)
		return
	}

	defer qfs.instantiationLock.Lock().Unlock()

	if inode := qfs.inodeNoInstantiate(&qfs.c, InodeId(nodeID)); inode != nil {
		inode.queueToForget(&qfs.c)
	} else {
		qfs.c.dlog("Forgetting uninstantiated Inode %d", nodeID)
		qfs.uninstantiateInode_(&qfs.c, InodeId(nodeID))
	}
}

func (qfs *QuantumFs) GetAttr(input *fuse.GetAttrIn,
	out *fuse.AttrOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::GetAttr", "Inode %d", input.NodeId).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.GetAttr(c, out)
}

func (qfs *QuantumFs) SetAttr(input *fuse.SetAttrIn,
	out *fuse.AttrOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::SetAttr", "Inode %d", input.NodeId).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.SetAttr(c, input, out)
}

func (qfs *QuantumFs) Mknod(input *fuse.MknodIn, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Mknod", "Inode %d Name %s", input.NodeId,
		name).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Mknod(c, name, input, out)
}

func (qfs *QuantumFs) Mkdir(input *fuse.MkdirIn, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Mkdir", "Inode %d Name %s", input.NodeId,
		name).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Mkdir(c, name, input, out)
}

func (qfs *QuantumFs) Unlink(header *fuse.InHeader,
	name string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Unlink", "Inode %d Name %s", header.NodeId,
		name).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Unlink(c, name)
}

func (qfs *QuantumFs) Rmdir(header *fuse.InHeader,
	name string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Rmdir", "Inode %d Name %s", header.NodeId,
		name).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Rmdir(c, name)
}

func (qfs *QuantumFs) Rename(input *fuse.RenameIn, oldName string,
	newName string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Rename", "Inode %d newdir %d %s -> %s",
		input.NodeId, input.Newdir, oldName, newName).out()

	srcInode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if srcInode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, srcInode) {
		return fuse.EROFS
	}

	if input.NodeId == input.Newdir {
		return srcInode.RenameChild(c, oldName, newName)
	} else {
		dstInode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.Newdir))
		defer unlock.RUnlock()

		if dstInode == nil {
			return fuse.ENOENT
		}

		if !qfs.workspaceIsMutable(c, dstInode) {
			return fuse.EROFS
		}

		return srcInode.MvChild(c, dstInode, oldName, newName)
	}
}

func (qfs *QuantumFs) Link(input *fuse.LinkIn, filename string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Link", "inode %d to name %s in dstDir %d",
		input.Oldnodeid, filename, input.NodeId).out()

	srcInode := qfs.inode(c, InodeId(input.Oldnodeid))
	if srcInode == nil {
		return fuse.ENOENT
	}

	dstInode := qfs.inode(c, InodeId(input.NodeId))
	if dstInode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, dstInode) {
		return fuse.EROFS
	}

	// Via races, srcInode and dstInode can be forgotten here

	if srcInode.treeLock() == dstInode.treeLock() {
		// If src and dst live in the same workspace, we only need one lock
		defer dstInode.LockTree().Unlock()
	} else {
		// When we have to lock multiple inodes, we must make sure that we
		// lock the inodes in correct sequence to prevent deadlock
		firstLock, lastLock := getLockOrder(dstInode, srcInode)
		defer firstLock.LockTree().Unlock()
		defer lastLock.LockTree().Unlock()
	}

	// We need to re-get these to ensure they're instantiated while we're locked
	srcInode = qfs.inode(c, InodeId(input.Oldnodeid))
	if srcInode == nil {
		return fuse.ENOENT
	}

	dstInode = qfs.inode(c, InodeId(input.NodeId))
	if dstInode == nil {
		return fuse.ENOENT
	}

	return dstInode.link_DOWN(c, srcInode, filename, out)
}

func (qfs *QuantumFs) Symlink(header *fuse.InHeader, pointedTo string,
	linkName string, out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Symlink", "Inode %d Name %s", header.NodeId,
		linkName).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Symlink(c, pointedTo, linkName, out)
}

func (qfs *QuantumFs) Readlink(header *fuse.InHeader) (out []byte,
	result fuse.Status) {

	out = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Readlink", "Inode %d", header.NodeId).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return nil, fuse.ENOENT
	}

	return inode.Readlink(c)
}

func (qfs *QuantumFs) Access(input *fuse.AccessIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Access", "Inode %d", input.NodeId).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	return inode.Access(c, input.Mask, input.Uid, input.Gid)
}

func (qfs *QuantumFs) GetXAttrSize(header *fuse.InHeader, attr string) (size int,
	result fuse.Status) {

	size = 0
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::GetXAttrSize", "Inode %d", header.NodeId).out()

	if attr == quantumfs.XAttrTypeKey {
		_, status := getQuantumfsExtendedKey(c, qfs, InodeId(header.NodeId))
		if status != fuse.OK {
			return 0, status
		}
		return quantumfs.ExtendedKeyLength, status
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return 0, fuse.ENOENT
	}

	return inode.GetXAttrSize(c, attr)
}

func getQuantumfsExtendedKey(c *ctx, qfs *QuantumFs, inodeId InodeId) ([]byte,
	fuse.Status) {

	defer c.FuncIn("getQuantumfsExtendedKey", "inode %d", inodeId).out()

	inode, unlock := qfs.LockTreeGetInode(c, inodeId)
	defer unlock.Unlock()
	if inode == nil {
		return nil, fuse.ENOENT
	}

	if inode.isWorkspaceRoot() {
		c.vlog("Parent is workspaceroot, returning")
		return nil, fuse.ENOATTR
	}

	// Update the Hash value before generating the key
	inode.Sync_DOWN(c)

	defer inode.getParentLock().RLock().RUnlock()

	var dir *Directory
	parent := inode.parent_(c)
	if parent.isWorkspaceRoot() {
		dir = &parent.(*WorkspaceRoot).Directory
	} else {
		dir = parent.(*Directory)
	}

	return dir.generateChildTypeKey_DOWN(c, inode.inodeNum())
}

func (qfs *QuantumFs) GetXAttrData(header *fuse.InHeader, attr string) (data []byte,
	result fuse.Status) {

	data = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::GetXAttrData", "Inode %d", header.NodeId).out()

	if attr == quantumfs.XAttrTypeKey {
		return getQuantumfsExtendedKey(c, qfs, InodeId(header.NodeId))
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return nil, fuse.ENOENT
	}

	return inode.GetXAttrData(c, attr)
}

func (qfs *QuantumFs) ListXAttr(header *fuse.InHeader) (attributes []byte,
	result fuse.Status) {

	attributes = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::ListXAttr", "Inode %d", header.NodeId).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return nil, fuse.ENOENT
	}

	return inode.ListXAttr(c)
}

func (qfs *QuantumFs) SetXAttr(input *fuse.SetXAttrIn, attr string,
	data []byte) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::SetXAttr", "Inode %d", input.NodeId).out()

	if attr == quantumfs.XAttrTypeKey {
		// quantumfs.key is immutable from userspace
		return fuse.EPERM
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.SetXAttr(c, attr, data)
}

func (qfs *QuantumFs) RemoveXAttr(header *fuse.InHeader,
	attr string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::RemoveXAttr", "Inode %d", header.NodeId).out()

	if attr == quantumfs.XAttrTypeKey {
		// quantumfs.key is immutable from userspace
		return fuse.EPERM
	}

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(header.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.RemoveXAttr(c, attr)
}

func (qfs *QuantumFs) Create(input *fuse.CreateIn, name string,
	out *fuse.CreateOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Create", "Inode %d Name %s", input.NodeId,
		name).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		c.elog("Create failed", input)
		return fuse.EACCES // TODO Confirm this is correct
	}

	if !qfs.workspaceIsMutable(c, inode) {
		return fuse.EROFS
	}

	return inode.Create(c, input, name, out)
}

func (qfs *QuantumFs) Open(input *fuse.OpenIn,
	out *fuse.OpenOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Open", "Inode %d", input.NodeId).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		c.elog("Open failed Inode %d", input.NodeId)
		return fuse.ENOENT
	}

	if !qfs.workspaceIsMutableAtOpen(c, inode, input.Flags) {
		return fuse.EROFS
	}

	return inode.Open(c, input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) Read(input *fuse.ReadIn, buf []byte) (readRes fuse.ReadResult,
	result fuse.Status) {

	readRes = nil
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Read", "Fh: %d", input.Fh).out()

	fileHandle, unlock := qfs.RLockTreeGetHandle(c, FileHandleId(input.Fh))
	defer unlock.RUnlock()
	if fileHandle == nil {
		c.elog("Read failed %d", fileHandle)
		return nil, fuse.ENOENT
	}

	return fileHandle.Read(c, input.Offset, input.Size,
		buf, utils.BitFlagsSet(uint(input.Flags), uint(syscall.O_NONBLOCK)))
}

func (qfs *QuantumFs) Release(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Release", "Fh: %v", input.Fh).out()

	qfs.setFileHandle(c, FileHandleId(input.Fh), nil)
}

func (qfs *QuantumFs) Write(input *fuse.WriteIn, data []byte) (written uint32,
	result fuse.Status) {

	written = 0
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Write", "Fh: %d", input.Fh).out()

	fileHandle, unlock := qfs.RLockTreeGetHandle(c, FileHandleId(input.Fh))
	defer unlock.RUnlock()
	if fileHandle == nil {
		c.elog("Write failed")
		return 0, fuse.ENOENT
	}

	return fileHandle.Write(c, input.Offset, input.Size,
		input.Flags, data)
}

func (qfs *QuantumFs) Flush(input *fuse.FlushIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Flush", "Fh: %v Context %d %d %d", input.Fh,
		input.Context.Uid, input.Context.Gid, input.Context.Pid).out()

	return fuse.OK
}

func (qfs *QuantumFs) Fsync(input *fuse.FsyncIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Fsync", "Enter Fh %d", input.Fh).out()

	fileHandle, unlock := qfs.LockTreeGetHandle(c, FileHandleId(input.Fh))
	defer unlock.Unlock()
	if fileHandle == nil {
		c.elog("Fsync failed")
		return fuse.EIO
	}

	return fileHandle.Sync_DOWN(c)
}

func (qfs *QuantumFs) Fallocate(input *fuse.FallocateIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.funcIn("Mux::Fallocate").out()

	c.elog("Unhandled request Fallocate")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) OpenDir(input *fuse.OpenIn,
	out *fuse.OpenOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::OpenDir", "Inode %d", input.NodeId).out()

	inode, unlock := qfs.RLockTreeGetInode(c, InodeId(input.NodeId))
	defer unlock.RUnlock()
	if inode == nil {
		c.elog("OpenDir failed", input)
		return fuse.ENOENT
	}

	return inode.OpenDir(c, input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) ReadDir(input *fuse.ReadIn,
	out *fuse.DirEntryList) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::ReadDir", "Fh: %d offset %d", input.Fh,
		input.Offset).out()

	c.elog("Unhandled request ReadDir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) ReadDirPlus(input *fuse.ReadIn,
	out *fuse.DirEntryList) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::ReadDirPlus", "Fh: %d offset %d",
		input.Fh, input.Offset).out()

	fileHandle, unlock := qfs.RLockTreeGetHandle(c, FileHandleId(input.Fh))
	defer unlock.RUnlock()
	if fileHandle == nil {
		c.elog("ReadDirPlus failed", fileHandle)
		return fuse.ENOENT
	}

	return fileHandle.ReadDirPlus(c, input, out)
}

func (qfs *QuantumFs) ReleaseDir(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::ReleaseDir", "Fh: %d", input.Fh).out()

	qfs.setFileHandle(&qfs.c, FileHandleId(input.Fh), nil)
}

func (qfs *QuantumFs) FsyncDir(input *fuse.FsyncIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::FsyncDir", "Fh %d", input.Fh).out()

	fileHandle, unlock := qfs.LockTreeGetHandle(c, FileHandleId(input.Fh))
	defer unlock.Unlock()
	if fileHandle == nil {
		c.elog("FsyncDir failed")
		return fuse.EIO
	}

	return fileHandle.Sync_DOWN(c)
}

func (qfs *QuantumFs) StatFs(input *fuse.InHeader,
	out *fuse.StatfsOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(input)
	defer logRequestPanic(c)
	defer c.funcIn("Mux::StatFs").out()

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
		c.elog("ERROR: PANIC Global variable %d should"+
			" be greater than zero", result)
		atomic.StoreInt64(&qfs.apiFileSize, 0)
	}
}
