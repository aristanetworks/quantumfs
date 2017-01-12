// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

// go-fuse creates a goroutine for every request. The code here simply takes these
// requests and forwards them to the correct Inode.
package daemon

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
import "github.com/hanwen/go-fuse/fuse"

const defaultCacheSize = 4096
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

	namespaceList := NewNamespaceList()
	qfs.inodes[quantumfs.InodeIdRoot] = namespaceList
	qfs.inodes[quantumfs.InodeIdApi] = NewApiInode(namespaceList.treeLock(),
		namespaceList.inodeNum())
	return qfs
}

func NewQuantumFsLogs(config QuantumFsConfig, qlogIn *qlog.Qlog) *QuantumFs {
	return NewQuantumFs_(config, qlogIn)
}

func NewQuantumFs(config QuantumFsConfig) *QuantumFs {
	return NewQuantumFs_(config, qlog.NewQlogExt(config.CachePath,
		config.MemLogBytes, qlog.PrintToStdout))
}

type QuantumFs struct {
	fuse.RawFileSystem
	server        *fuse.Server
	config        QuantumFsConfig
	inodeNum      uint64
	fileHandleNum uint64
	c             ctx

	mapMutex    sync.RWMutex
	inodes      map[InodeId]Inode
	fileHandles map[FileHandleId]FileHandle

	// This is a map from the treeLock to a list of dirty inodes. We use the
	// treelock because every Inode already has the treelock of its workspace so
	// this is an easy way to sort Inodes by workspace.
	//
	// The Front of the list are the Inodes next in line to flush.
	dirtyQueueLock DeferableMutex
	dirtyQueue     map[*sync.RWMutex]*list.List

	// Notify the flusher that there is a new entry in the dirty queue
	kickFlush chan struct{}

	// Notify the flusher that all dirty inodes should be flushed
	flushAll chan *ctx

	// Notify whoever used flushAll that flushing is complete
	flushComplete chan struct{}

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
	lookupCountLock DeferableMutex
	lookupCounts    map[InodeId]uint64
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
	for {
		sleepTime := nextExpiringInode.Sub(time.Now())

		c.vlog("Waiting until %s (%s)...", nextExpiringInode.String(),
			sleepTime.String())

		// If we've been directed to flushAll, use that caller's context
		c = flusherContext

		stop := false
		flushAll := false

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
	defer c.FuncIn("Mux::flushDirtyLists", "flushAll %t", flushAll)

	defer qfs.dirtyQueueLock.Lock().Unlock()
	nextExpiringInode := time.Now().Add(flushSanityTimeout)

	for key, dirtyList := range qfs.dirtyQueue {
		func() {
			key.RLock()
			defer key.RUnlock()
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

// Requires dirtyQueueLock and the treeLock of the workspace held read-only
func (qfs *QuantumFs) flushDirtyList_(c *ctx, dirtyList *list.List,
	flushAll bool) time.Time {

	defer c.funcIn("Mux::flushDirtyList")

	for dirtyList.Len() > 0 {
		// Should we clean this inode?
		candidate := dirtyList.Front().Value.(*dirtyInode)

		now := time.Now()
		if !flushAll && candidate.expiryTime.After(now) {
			// We are up to date with this list
			return candidate.expiryTime
		}

		dirtyList.Remove(dirtyList.Front())

		// Note: drops and regrabs the dirtyQueueLock
		qfs.flushInode_(c, *candidate)
	}

	// If we get here then we've emptied the dirtyList out entirely.
	return time.Now().Add(flushSanityTimeout)
}

// Requires the dirtyQueueLock be held.
//
// NOTE This method gives up the dirtyQueueLock lock and then regrabs it.
func (qfs *QuantumFs) flushInode_(c *ctx, dirtyInode dirtyInode) {
	inodeNum := dirtyInode.inode.inodeNum()
	defer c.FuncIn("Mux::flushInode_", "inode %d, uninstantiate %t",
		inodeNum, dirtyInode.shouldUninstantiate)

	qfs.dirtyQueueLock.Unlock()

	dirtyInode.inode.flush_DOWN(c)
	dirtyInode.inode.markClean()

	if dirtyInode.shouldUninstantiate {
		c.vlog("Starting uninstantiation at inode %d", inodeNum)
		toRemove := qfs.forgetChain(inodeNum)

		if toRemove != nil {
			// We need to remove all uninstantiated children.
			// Note: locks mapMutex
			qfs.removeUninstantiated(c, toRemove)
		}
	}

	qfs.dirtyQueueLock.Lock()
}

// Don't use this method directly, use one of the semantically specific variants
// instead.
func (qfs *QuantumFs) _queueDirtyInode(c *ctx, inode Inode, shouldUninstantiate bool,
	shouldWait bool) *list.Element {

	defer c.FuncIn("Mux::_queueDirtyInode", "inode %d su %t sw %t",
		inode.inodeNum(), shouldUninstantiate, shouldWait)

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
	c.funcIn("adjustBdi").out()

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
		c.wlog("Unable to set read_ahead_kb: %v", err)
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
		c.wlog("Unable to set bdi max_ratio: %v", err)
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

func (qfs *QuantumFs) inodeNoInstantiate(c *ctx, id InodeId) Inode {
	qfs.mapMutex.RLock()
	defer qfs.mapMutex.RUnlock()
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
		qfs.mapMutex.RLock()
		defer qfs.mapMutex.RUnlock()
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
	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()

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
	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()

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

	if parent == 0 {
		panic("Invalid parentId in addUninstantiated")
	}

	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()

	qfs.addUninstantiated_(c, uninstantiated, parent)
}

// Requires the mapMutex for writing
func (qfs *QuantumFs) addUninstantiated_(c *ctx, uninstantiated []InodeId,
	parent InodeId) {

	for _, inodeNum := range uninstantiated {
		qfs.parentOfUninstantiated[inodeNum] = parent
		c.vlog("Adding uninstantiated %v (%d)", inodeNum,
			len(qfs.parentOfUninstantiated))
	}
}

// Remove a list of inode numbers from the parentOfUninstantiated list
func (qfs *QuantumFs) removeUninstantiated(c *ctx, uninstantiated []InodeId) {
	qfs.mapMutex.Lock()
	defer qfs.mapMutex.Unlock()

	for _, inodeNum := range uninstantiated {
		delete(qfs.parentOfUninstantiated, inodeNum)
		c.vlog("Removing uninstantiated %d (%d)", inodeNum,
			len(qfs.parentOfUninstantiated))
	}
}

// Increase an Inode's lookup count. This must be called whenever a fuse.EntryOut is
// returned.
func (qfs *QuantumFs) increaseLookupCount(inodeId InodeId) {
	defer qfs.lookupCountLock.Lock().Unlock()
	prev, exists := qfs.lookupCounts[inodeId]
	if !exists {
		qfs.lookupCounts[inodeId] = 1
	} else {
		qfs.lookupCounts[inodeId] = prev + 1
	}
}

func (qfs *QuantumFs) lookupCount(inodeId InodeId) uint64 {
	defer qfs.lookupCountLock.Lock().Unlock()
	lookupCount, exists := qfs.lookupCounts[inodeId]
	if !exists {
		return 0
	}

	return lookupCount
}

// Returns true if the count became zero or was previously zero
func (qfs *QuantumFs) shouldForget(inodeId InodeId, count uint64) bool {
	defer qfs.lookupCountLock.Lock().Unlock()
	lookupCount, exists := qfs.lookupCounts[inodeId]
	if !exists {
		return true
	}

	lookupCount -= count
	if lookupCount < 0 {
		msg := fmt.Sprintf("lookupCount less than zero %d", lookupCount)
		panic(msg)
	} else if lookupCount == 0 {
		// Leave the zero entry in the map to indicate this node needs
		// to actually be forgotten (marked toForget)
		qfs.lookupCounts[inodeId] = 0
		if count > 1 {
			qfs.c.dlog("Forgetting inode with lookupCount of %d", count)
		}
		return true
	} else {
		qfs.lookupCounts[inodeId] = lookupCount
		return false
	}
}

// Get a file handle in a thread safe way
func (qfs *QuantumFs) fileHandle(c *ctx, id FileHandleId) FileHandle {
	qfs.mapMutex.RLock()
	fileHandle := qfs.fileHandles[id]
	qfs.mapMutex.RUnlock()
	return fileHandle
}

// Set a file handle in a thread safe way, set to nil to delete
func (qfs *QuantumFs) setFileHandle(c *ctx, id FileHandleId, fileHandle FileHandle) {
	defer c.funcIn("Mux::setFileHandle").out()

	qfs.mapMutex.Lock()
	if fileHandle != nil {
		qfs.fileHandles[id] = fileHandle
	} else {
		delete(qfs.fileHandles, id)
	}
	qfs.mapMutex.Unlock()
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
		fmt.Sprintf("%v", exception), BytesToString(stackTrace))
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

	c.vlog("QuantumFs::lookupCommon Enter Inode %d Name %s", inodeId, name)
	defer c.vlog("QuantumFs::lookupCommon Exit")

	inode := qfs.inode(c, inodeId)
	if inode == nil {
		c.elog("Lookup failed", name)
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Lookup(c, name, out)
}

// Needs treelock for write
func (qfs *QuantumFs) uninstantiateChain_(inode Inode) []InodeId {
	rtn := make([]InodeId, 0)
	for {
		lookupCount := qfs.lookupCount(inode.inodeNum())
		if lookupCount != 0 {
			qfs.c.vlog("No forget called on inode %d yet",
				inode.inodeNum())
			break
		}

		if dir, isDir := inode.(inodeHolder); isDir {
			children := dir.childInodes()

			for _, i := range children {
				// To be fully unloaded, the child must have lookup
				// count of zero (no kernel refs) *and*
				// be uninstantiated
				if qfs.lookupCount(InodeId(i)) != 0 ||
					qfs.inodeNoInstantiate(&qfs.c,
						InodeId(i)) != nil {

					// Not ready to forget, no more to do
					qfs.c.dlog("Not all children unloaded, %d"+
						"in %d", i,
						inode.inodeNum())
					return rtn
				}
				qfs.c.dlog("Child %d of %d not loaded", i,
					inode.inodeNum())
			}

			rtn = append(rtn, children...)
		}

		// Great, we want to forget this so proceed
		key := inode.flush_DOWN(&qfs.c)
		qfs.setInode(&qfs.c, inode.inodeNum(), nil)

		func() {
			defer qfs.lookupCountLock.Lock().Unlock()
			delete(qfs.lookupCounts, inode.inodeNum())
		}()

		qfs.c.vlog("Set inode %d to nil", inode.inodeNum())

		if !inode.isOrphaned() && inode.inodeNum() != quantumfs.InodeIdRoot {
			parentId := inode.parentId()
			parent := qfs.inodeNoInstantiate(&qfs.c, parentId)
			if parent == nil {
				panic(fmt.Sprintf("Parent was unloaded before child"+
					"! %d %d", parentId, inode.inodeNum()))
			}

			parent.syncChild(&qfs.c, inode.inodeNum(), key)

			qfs.addUninstantiated(&qfs.c,
				[]InodeId{inode.inodeNum()},
				parent.inodeNum())

			// Then check our parent and iterate again
			inode = parent
			continue
		}
		break
	}

	return rtn
}

func (qfs *QuantumFs) forgetChain(inodeNum InodeId) []InodeId {
	inode := qfs.inodeNoInstantiate(&qfs.c, inodeNum)
	if inode == nil || inodeNum == quantumfs.InodeIdRoot ||
		inodeNum == quantumfs.InodeIdApi {

		qfs.c.dlog("inode %d doesn't need to be forgotten", inodeNum)
		// Nothing to do
		return nil
	}

	defer inode.LockTree().Unlock()

	// Now that we have the tree locked, we need to re-check the inode because
	// another forgetChain could have forgotten us before we got the tree lock
	inode = qfs.inodeNoInstantiate(&qfs.c, inodeNum)
	if inode == nil || inodeNum == quantumfs.InodeIdRoot ||
		inodeNum == quantumfs.InodeIdApi {

		qfs.c.dlog("inode %d forgotten underneath us", inodeNum)
		// Nothing to do
		return nil
	}

	return qfs.uninstantiateChain_(inode)
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

	inode := qfs.inode(&qfs.c, InodeId(nodeID))
	inode.queueToForget(&qfs.c)
}

func (qfs *QuantumFs) GetAttr(input *fuse.GetAttrIn,
	out *fuse.AttrOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::GetAttr", "Enter Inode %d", input.NodeId).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.GetAttr(c, out)
}

func (qfs *QuantumFs) SetAttr(input *fuse.SetAttrIn,
	out *fuse.AttrOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::SetAttr", "Enter Inode %d", input.NodeId).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.SetAttr(c, input, out)
}

func (qfs *QuantumFs) Mknod(input *fuse.MknodIn, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Mknod", "Enter Inode %d Name %s", input.NodeId,
		name).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Mknod(c, name, input, out)
}

func (qfs *QuantumFs) Mkdir(input *fuse.MkdirIn, name string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Mkdir", "Enter Inode %d Name %s", input.NodeId,
		name).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Mkdir(c, name, input, out)
}

func (qfs *QuantumFs) Unlink(header *fuse.InHeader,
	name string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Unlink", "Enter Inode %d Name %s", header.NodeId,
		name).out()

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Unlink(c, name)
}

func (qfs *QuantumFs) Rmdir(header *fuse.InHeader,
	name string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Rmdir", "Enter Inode %d Name %s", header.NodeId,
		name).out()

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Rmdir(c, name)
}

func (qfs *QuantumFs) Rename(input *fuse.RenameIn, oldName string,
	newName string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Rename", "Enter Inode %d newdir %d %s -> %s",
		input.NodeId, input.Newdir, oldName, newName).out()

	if input.NodeId == input.Newdir {
		inode := qfs.inode(c, InodeId(input.NodeId))
		if inode == nil {
			return fuse.ENOENT
		}

		defer inode.RLockTree().RUnlock()
		return inode.RenameChild(c, oldName, newName)
	} else {
		srcInode := qfs.inode(c, InodeId(input.NodeId))
		if srcInode == nil {
			return fuse.ENOENT
		}

		dstInode := qfs.inode(c, InodeId(input.Newdir))
		if dstInode == nil {
			return fuse.ENOENT
		}

		defer srcInode.RLockTree().RUnlock()
		defer dstInode.RLockTree().RUnlock()

		return srcInode.MvChild(c, dstInode, oldName, newName)
	}
}

func (qfs *QuantumFs) Link(input *fuse.LinkIn, filename string,
	out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Link", "Enter inode %d to name %s in dstDir %d",
		input.NodeId, filename, input.Oldnodeid).out()

	srcInode := qfs.inode(c, InodeId(input.Oldnodeid))
	if srcInode == nil {
		return fuse.ENOENT
	}

	dstInode := qfs.inode(c, InodeId(input.NodeId))
	if dstInode == nil {
		return fuse.ENOENT
	}

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

	return dstInode.link_DOWN(c, srcInode, filename, out)
}

func (qfs *QuantumFs) Symlink(header *fuse.InHeader, pointedTo string,
	linkName string, out *fuse.EntryOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Symlink", "Enter Inode %d Name %s", header.NodeId,
		linkName).out()

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Symlink(c, pointedTo, linkName, out)
}

func (qfs *QuantumFs) Readlink(header *fuse.InHeader) (out []byte,
	result fuse.Status) {

	out = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Readlink", "Enter Inode %d", header.NodeId).out()

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return nil, fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Readlink(c)
}

func (qfs *QuantumFs) Access(input *fuse.AccessIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Access", "Enter Inode %d", input.NodeId).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Access(c, input.Mask, input.Uid, input.Gid)
}

func (qfs *QuantumFs) GetXAttrSize(header *fuse.InHeader, attr string) (size int,
	result fuse.Status) {

	size = 0
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::GetXAttrSize", "Enter Inode %d", header.NodeId).out()

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return 0, fuse.ENOENT
	}
	if attr == quantumfs.XAttrTypeKey {
		_, status := getQuantumfsExtendedKey(c, inode)
		if status != fuse.OK {
			return 0, status
		}
		return quantumfs.ExtendedKeyLength, status
	}
	defer inode.RLockTree().RUnlock()
	return inode.GetXAttrSize(c, attr)
}

func getQuantumfsExtendedKey(c *ctx, inode Inode) ([]byte, fuse.Status) {
	defer inode.LockTree().Unlock()
	if inode.isWorkspaceRoot() {
		c.vlog("Parent is workspaceroot, returning")
		return nil, fuse.ENOATTR
	}

	var dir *Directory
	parent := inode.parent(c)
	if parent.isWorkspaceRoot() {
		dir = &parent.(*WorkspaceRoot).Directory
	} else {
		dir = parent.(*Directory)
	}
	msg, status := dir.generateChildTypeKey_DOWN(c, inode.inodeNum())
	return msg, status
}

func (qfs *QuantumFs) GetXAttrData(header *fuse.InHeader, attr string) (data []byte,
	result fuse.Status) {

	data = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::GetXAttrData", "Enter Inode %d", header.NodeId).out()

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return nil, fuse.ENOENT
	}

	if attr == quantumfs.XAttrTypeKey {
		return getQuantumfsExtendedKey(c, inode)
	}

	defer inode.RLockTree().RUnlock()
	return inode.GetXAttrData(c, attr)
}

func (qfs *QuantumFs) ListXAttr(header *fuse.InHeader) (attributes []byte,
	result fuse.Status) {

	attributes = nil
	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::ListXAttr", "Enter Inode %d", header.NodeId).out()

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return nil, fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.ListXAttr(c)
}

func (qfs *QuantumFs) SetXAttr(input *fuse.SetXAttrIn, attr string,
	data []byte) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::SetXAttr", "Enter Inode %d", input.NodeId).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.SetXAttr(c, attr, data)
}

func (qfs *QuantumFs) RemoveXAttr(header *fuse.InHeader,
	attr string) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(header)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::RemoveXAttr", "Enter Inode %d", header.NodeId).out()

	inode := qfs.inode(c, InodeId(header.NodeId))
	if inode == nil {
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.RemoveXAttr(c, attr)
}

func (qfs *QuantumFs) Create(input *fuse.CreateIn, name string,
	out *fuse.CreateOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Create", "Enter Inode %d Name %s", input.NodeId,
		name).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("Create failed", input)
		return fuse.EACCES // TODO Confirm this is correct
	}

	defer inode.RLockTree().RUnlock()
	return inode.Create(c, input, name, out)
}

func (qfs *QuantumFs) Open(input *fuse.OpenIn,
	out *fuse.OpenOut) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Open", "Enter Inode %d", input.NodeId).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("Open failed Inode %d", input.NodeId)
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.Open(c, input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) Read(input *fuse.ReadIn, buf []byte) (readRes fuse.ReadResult,
	result fuse.Status) {

	readRes = nil
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Read", "Enter Fh: %d", input.Fh).out()

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("Read failed", fileHandle)
		return nil, fuse.ENOENT
	}

	defer fileHandle.RLockTree().RUnlock()
	return fileHandle.Read(c, input.Offset, input.Size,
		buf, BitFlagsSet(uint(input.Flags), uint(syscall.O_NONBLOCK)))
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
	defer c.FuncIn("Mux::Write", "Enter Fh: %d", input.Fh).out()

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("Write failed")
		return 0, fuse.ENOENT
	}

	defer fileHandle.RLockTree().RUnlock()
	return fileHandle.Write(c, input.Offset, input.Size,
		input.Flags, data)
}

func (qfs *QuantumFs) Flush(input *fuse.FlushIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Flush", "Enter Fh: %v Context %d %d %d", input.Fh,
		input.Context.Uid, input.Context.Gid, input.Context.Pid).out()

	return fuse.OK
}

func (qfs *QuantumFs) Fsync(input *fuse.FsyncIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::Fsync", "Enter Fh %d", input.Fh).out()

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("Fsync failed")
		return fuse.EIO
	}

	defer fileHandle.LockTree().Unlock()
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
	defer c.FuncIn("Mux::OpenDir", "Enter Inode %d", input.NodeId).out()

	inode := qfs.inode(c, InodeId(input.NodeId))
	if inode == nil {
		c.elog("OpenDir failed", input)
		return fuse.ENOENT
	}

	defer inode.RLockTree().RUnlock()
	return inode.OpenDir(c, input.Flags, input.Mode, out)
}

func (qfs *QuantumFs) ReadDir(input *fuse.ReadIn,
	out *fuse.DirEntryList) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::ReadDir", "ReadDir Enter Fh: %d offset %d", input.Fh,
		input.Offset).out()

	c.elog("Unhandled request ReadDir")
	return fuse.ENOSYS
}

func (qfs *QuantumFs) ReadDirPlus(input *fuse.ReadIn,
	out *fuse.DirEntryList) (result fuse.Status) {

	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::ReadDirPlus", "ReadDirPlus Enter Fh: %d offset %d",
		input.Fh, input.Offset).out()

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("ReadDirPlus failed", fileHandle)
		return fuse.ENOENT
	}

	defer fileHandle.RLockTree().RUnlock()
	return fileHandle.ReadDirPlus(c, input, out)
}

func (qfs *QuantumFs) ReleaseDir(input *fuse.ReleaseIn) {
	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::ReleaseDir", "Enter Fh: %d", input.Fh).out()

	qfs.setFileHandle(&qfs.c, FileHandleId(input.Fh), nil)
}

func (qfs *QuantumFs) FsyncDir(input *fuse.FsyncIn) (result fuse.Status) {
	result = fuse.EIO

	c := qfs.c.req(&input.InHeader)
	defer logRequestPanic(c)
	defer c.FuncIn("Mux::FsyncDir", "Enter Fh %d", input.Fh).out()

	fileHandle := qfs.fileHandle(c, FileHandleId(input.Fh))
	if fileHandle == nil {
		c.elog("FsyncDir failed")
		return fuse.EIO
	}

	defer fileHandle.LockTree().Unlock()
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
	qfs.c.elog("Unhandled request Init")
}

func (qfs *QuantumFs) getWorkspaceRoot(c *ctx, namespace string,
	workspace string) (*WorkspaceRoot, bool) {

	c.vlog("QuantumFs::getWorkspaceRoot %s/%s", namespace, workspace)

	// Get the WorkspaceList Inode number
	var namespaceAttr fuse.EntryOut
	result := qfs.lookupCommon(c, quantumfs.InodeIdRoot, namespace,
		&namespaceAttr)
	if result != fuse.OK {
		return nil, false
	}

	// Get the WorkspaceRoot Inode number
	var workspaceRootAttr fuse.EntryOut
	result = qfs.lookupCommon(c, InodeId(namespaceAttr.NodeId), workspace,
		&workspaceRootAttr)
	if result != fuse.OK {
		return nil, false
	}

	// Fetch the WorkspaceRoot object itelf
	wsr := qfs.inode(c, InodeId(workspaceRootAttr.NodeId))

	return wsr.(*WorkspaceRoot), wsr != nil
}
