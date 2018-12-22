// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Refer to the workspace DB API documentation to see the list
// of workspace DB APIs that rely on cached data. The CQL implementation
// of such APIs use entityCache interface.

// entityCache is a hierarchical map cache where each 'level'
// contains 'entities'. One 'entity' on a 'level' serves as a
// parent to several 'entities' on the immediate next level and
// so on. Access to any 'entity' at a 'level' is achieved by
// specifying all parent entities. Such complete specification
// of an entity is called 'entityPath'. This cache gets its data
// from local insert/delete and remote insert/delete operations
// in the CQL cluster. This cache provides consistency guarantees
// expected by workspace DB API.

// Each level of entities should be refreshed if its accessed after
// a configurable time or when a refresh from CQL has stale information
// (i.e. when a local insert/delete happens in a level while a set of entities is
// being fetched from CQL for refreshing that same level)

// The cache refreshes remotely inserted/deleted data which involves
// CQL interactions and can potentially fail. The errors seen during
// refresh are propogated to caller using WSDB API.

// Following example illustrates how the entityCache is used as
// workspace DB cache.
//
// InsertEntities('type1','namespace1','workspace1')
// DeleteEntities('type1','namespace1','workspace2')
// CountEntities('type1','namespace1')
// ListEntities('type1','namespace1')

/*
  Following shows the relationship between
  the different structs in this entity cache
  implementation

                                                entityGroup
                                              +------------+
                                              |            |
                     (all namespaces)         |            |
                         entityGroup    +---->+            |
                       +-----------+    |     |            |
   cacheEntity    +--> |           |    |     |            |
                       |           |    |     +------------+
                       |  entity +------+
                       |           |
                       |     ns2 +------+
                       |           |    |          +-----+
                       +-----------+    |          |     |
                                        +--------->+     |
                                                   +-----+
                                                    entityGroup
                                                    all workspaces for namespace ns2

*/

// arg is registered and interpreted by the consumer of entityCache
type fetchEntities func(c Ctx, arg interface{},
	entityPath ...string) (map[string]bool, error)

// entityCache maintains global cache state (eg: lock etc)
type entityCache struct {
	root *entityGroup

	// number of levels in the hierarchy of entityGroups
	levels int
	// global lock for this entity cache instance
	rwMutex sync.RWMutex

	// fetcher and fetcherArg are registered by the entity cache
	// consumer to fetch entities as part of entityGroup refresh
	fetcherArg interface{}
	fetcher    fetchEntities

	// used when cache refresh/timeouts are disabled
	neverExpires bool

	// used to setup/update expiresAt of entityGroup
	expiryDuration time.Duration
}

// -- implementation of entityCache API ---

func newEntityCache(levels int, cacheTimeout int,
	fetcherArg interface{}, fetcher fetchEntities) *entityCache {

	ec := &entityCache{}
	ec.levels = levels
	ec.fetcher = fetcher
	ec.fetcherArg = fetcherArg
	ec.neverExpires = false
	ec.expiryDuration = time.Duration(cacheTimeout) * time.Second
	if cacheTimeout == DontExpireWsdbCache {
		ec.neverExpires = true
	}
	ec.root = newEntityGroup(nil, "", ec)
	return ec
}

// InsertEntities and DeleteEntities check the parents in
// entity path. This ensures that during inserts, any entities
// that are absent are automatically created and existing entities
// are not re-created. During deletes, the deletes can cascade up
// the parents in an entity path. The checking is necessary in this
// cache implementation since there is only one instance of an entity
// InsertEntities MUST be called with leading portions
// of complete entityPath. At least one entity
// must be specified.
// InsertEntities(namespace) - Valid
// InsertEntities(namespace, workspace) - Valid
// InsertEntities() - Invalid
// InsertEntities(workspace) - Invalid
func (ec *entityCache) InsertEntities(c Ctx, entityPath ...string) {
	defer c.FuncIn("cache::InsertEntities", "%s",
		strings.Join(entityPath, "/")).Out()

	if len(entityPath) == 0 {
		panic("Invalid argument: Specify at least one entity")
	}

	ec.rwMutex.Lock()
	defer ec.rwMutex.Unlock()

	group := ec.root
	for _, entity := range entityPath {
		group.checkInsertEntity(c, entity, true)
		group = group.entities[entity]
	}
}

// DeleteEntities MUST be called with complete entityPath
// ie entity at all levels.
// DeleteEntities(namespace, workspace) - Valid
// DeleteEntities() - Invalid
// DeleteEntities(workspace) - Invalid
func (ec *entityCache) DeleteEntities(c Ctx, entityPath ...string) {
	defer c.FuncIn("cache::DeleteEntities", "%s",
		strings.Join(entityPath, "/")).Out()

	if len(entityPath) == 0 {
		panic("Invalid argument: Specify at least one entity")
	}

	ec.rwMutex.Lock()
	defer ec.rwMutex.Unlock()

	ec.checkDeleteEntity(c, ec.root, true, entityPath...)
	return
}

// Unlike InsertEntities and DeleteEntities, the CountEntities and ListEntities
// routines involve read of entityGroup (read-lock), DB-fetch (unlock)
// and update of entityGroup (write-lock). Hence the locks are not
// managed solely in the top routines
// CountEntities() - Valid
// CountEntities(namespace) - Valid
// CountEntities(workspace) - Invalid
func (ec *entityCache) CountEntities(c Ctx, entityPath ...string) (int, error) {
	defer c.FuncIn("cache::CountEntities", "%s",
		strings.Join(entityPath, "/")).Out()

	var count int
	ec.rwMutex.RLock()
	// getEntityCountListGroup releases the read lock and
	// re-acquires it when refresh is involved. However,
	// read lock is held upon return from getEntityCountListGroup
	defer ec.rwMutex.RUnlock()

	group, err := ec.getEntityCountListGroup(c, entityPath...)
	if err != nil {
		return count, err
	}
	if group != nil {
		count = group.entityCount
	}

	return count, nil
}

// same constraints on entityPath argument as CountEntities
func (ec *entityCache) ListEntities(c Ctx, entityPath ...string) ([]string, error) {
	defer c.FuncIn("cache::ListEntities", "%s",
		strings.Join(entityPath, "/")).Out()

	var list []string
	ec.rwMutex.RLock()
	// getEntityCountListGroup releases the read lock and
	// re-acquires it when refresh is involved. However,
	// read lock is held upon return from getEntityCountListGroup
	defer ec.rwMutex.RUnlock()

	group, err := ec.getEntityCountListGroup(c, entityPath...)
	if err != nil {
		return list, err
	}
	if group != nil {
		list = group.getListCopy(c)
	}

	return list, nil
}

// used by unit tests to simulate different conditions
// same constraints on entityPath argument as CountEntities
func (ec *entityCache) enableCqlRefresh(c Ctx, entityPath ...string) {
	defer c.FuncIn("cache::enableCqlRefresh", "%s",
		strings.Join(entityPath, "/")).Out()

	ec.rwMutex.Lock()
	defer ec.rwMutex.Unlock()

	var group *entityGroup
	if group = ec.getLastEntityGroup(c, ec.root, entityPath...); group == nil {
		return
	}

	group.expiresAt = time.Date(0, time.January, 0, 0, 0, 0, 0, time.UTC)
}

// used by unit tests to simulate different conditions
// same constraints on entityPath argument as CountEntities
func (ec *entityCache) disableCqlRefresh(c Ctx, maxDelay time.Duration,
	entityPath ...string) {

	defer c.FuncIn("cache::disableCqlRefresh", "delay: %s %s",
		maxDelay.String(), strings.Join(entityPath, "/")).Out()

	ec.rwMutex.Lock()
	defer ec.rwMutex.Unlock()

	var group *entityGroup
	if group = ec.getLastEntityGroup(c, ec.root, entityPath...); group == nil {
		return
	}

	group.expiresAt = time.Now().Add(maxDelay)
}

// -- helpers that operate on multiple entityGroups ---

// called under rwMutex write lock
// returns the entityGroup for the last entity in entityPath
// traversal of the entityPath starts in group argument
// if any entity is absent then traversal stops and nil is returned
func (ec *entityCache) getLastEntityGroup(c Ctx, group *entityGroup,
	entityPath ...string) *entityGroup {

	defer c.FuncIn("cache::getLastEntityGroup", "%s",
		strings.Join(entityPath, "/")).Out()

	for _, entity := range entityPath {
		if _, exists := group.entities[entity]; !exists {
			return nil
		}
		group = group.entities[entity]
	}

	return group
}

// called under rwMutex in write mode
// checkDeleteEntity implements cascaded deletes of entity groups
// first entity in entityPath starts in entityGroup
// if entityGroup is ec.root then entityPath is complete entityPath
// this routine will be invoked with at least one entity in entityPath
func (ec *entityCache) checkDeleteEntity(c Ctx, group *entityGroup, local bool,
	entityPath ...string) {

	defer c.FuncIn("cache::checkDeleteEntity", "%s local:%t",
		strings.Join(entityPath, "/"),
		local).Out()

	// Its possible to specify only some leading portion
	// of the complete entityPath. Even though its a delete
	// in-between the entityPath it frees up the
	// entityGroups along the complete entity path correctly.

	handlePartialPathDelete := false
	if len(entityPath) < ec.levels {
		handlePartialPathDelete = true
	}

	if group = ec.getLastEntityGroup(c, group, entityPath...); group == nil {
		c.Vlog("Skipping delete %s", strings.Join(entityPath, "/"))
		return
	}

	// entityGroup of the last entity in entityPath
	group = group.parent
	entity := entityPath[len(entityPath)-1]

	for {

		// navigate and mark all the child entityGroups as
		// detached if handlePartialPathDelete == true
		if handlePartialPathDelete == true {
			ec.markChildEntityGroupsDetached(c, group.entities[entity])
			// since we start at the end of entityPath and
			// we have taken care of all child entityGroups
			// off of last entity, we don't need to do more
			// such markings as part of this partial delete
			handlePartialPathDelete = false
		}
		group.entities[entity].parent = nil
		// this turns the entityGroup pointed by entity into garbage
		c.Vlog("Deleting %s", entity)
		delete(group.entities, entity)

		if local && group.fetchInProgress {
			group.concLocalDeletes[entity] = true

			// during refresh (unlocked), its possible to get insert
			// followed by delete or vice versa (each of insert and
			// delete respect locks). When merging the local updates,
			// we should pick up only the most recent action (either
			// insert or delete).
			// Since separate insert and delete logs exist,
			// this code ensures that theres only 1 of insert or delete
			// log for an entity during refresh
			delete(group.concLocalInserts, entity)
		}
		group.entityCount--

		if group.entityCount > 0 || group.parent == nil {
			// don't need to cascade the delete upto parent
			break
		}

		entity = group.parentEntity
		group = group.parent
	}
}

// invoked under rwMutex write lock
func (ec *entityCache) markChildEntityGroupsDetached(c Ctx,
	group *entityGroup) {

	defer c.FuncIn("cache::markChildEntityGroupsDetached", "").Out()
	// we don't need to maintain any concLocalDeletes logs since
	// the entire group is being deleted
	if group != nil {
		group.detached = true
		for entity := range group.entities {
			ec.markChildEntityGroupsDetached(c, group.entities[entity])
		}
	}
}

// since some callers could be blocking for refresh
// any panics during fetch should be converted to errors
func (ec *entityCache) callFetch(c Ctx,
	entityPath ...string) (m map[string]bool, e error) {

	defer func() {
		if ex := recover(); ex != nil {
			var ok bool
			e, ok = ex.(error)
			if !ok {
				e = fmt.Errorf("%v", ex)
			}
			m = nil
		}
	}()

	return ec.fetcher(c, ec.fetcherArg, entityPath...)
}

// invoked under rwMutex read lock, does a unlocked fetch, refresh under
// write lock and finally re-acquires read lock
// returns non-nil entityGroup whose list or count can be extracted
// after doing a refresh if needed.
// returns nil entityGroup if there are detachments in entityPath
func (ec *entityCache) getEntityCountListGroup(c Ctx,
	entityPath ...string) (*entityGroup, error) {

	defer c.FuncIn("cache::getEntityCountListGroup", "%s",
		strings.Join(entityPath, "/")).Out()

	detachOccured := false
	group := ec.root
	// this routine can be called without entityPath
	// eg: root level
	for pathIdx := 0; true; pathIdx++ {

		// for each entity in given entityPath
		// check if corresponding entityGroup needs to be
		// refreshed
		action := group.refreshNeeded(c)
		c.Vlog("refreshAction: %d for (%s)", action,
			strings.Join(entityPath[:pathIdx], "/"))
		switch action {
		case refreshPerform:
			err := func() error {
				ec.rwMutex.RUnlock()
				// data after refresh will be looked at under
				// a read lock
				defer ec.rwMutex.RLock()

				// fetch data from CQL without locking cache
				fetchList, err := ec.callFetch(c,
					entityPath[:pathIdx]...)
				if err != nil {
					c.Elog("Ether cache refresh (%s) failed: %s",
						strings.Join(entityPath[:pathIdx],
							"/"), err.Error())
				}
				// update the group under write lock unless the
				// fetched data has been invalidated by a local
				// insert/delete the error from fetcher is passed
				// into refresh so that proper clean ups can be done
				return group.refresh(c, fetchList, err)
			}()

			if err != nil {
				return nil, err
			}
			// in between the release of write lock in group.refresh()
			// and acquire of read lock, there can be many local inserts
			// or deletes that may happen. Since each local update also
			// maintains cache local coherency, we'll see all local
			// updates correctly.
			//
			// Possible states include:
			// - current group has got detached from parent
			//   detachment can be from immediate parent, an
			//   ancestor detachment, child detachement
			//     return count zero and empty list
			//     ancestor and child detachment scenarios can happen
			//     only when maxEntityPath in List/Count > 1
			// - next entity in the entity path has got deleted
			//     return zero count and empty list
			// - expiryDuration time has elapsed since the merge and so
			//   we might be looking at stale remote information
			//     this is no different than remote information getting
			//     in DB as soon as we completed running fetcher. We
			// just use the cache information we have.

		case refreshWait:
			// refresh is needed and there are no entities
			// currently but can't do refresh since
			// theres already some other caller doing refresh
			// so we wait until the refresh
			err := func() error {
				// all waiters need to release the read
				// lock so that the refresh under write
				// lock can happen
				group.fetchErrReapersWg.Add(1)
				ec.rwMutex.RUnlock()
				// the fetch error is deposited with write
				// lock held and so this caller can look at the
				// data after the refresh has completed
				defer ec.rwMutex.RLock()

				return group.reapFetchError()
			}()
			if err != nil {
				return nil, err
			}
		default:
		}

		// current group got detached
		if group.detached {
			detachOccured = true
			break
		}

		// we have completed walk
		if pathIdx == len(entityPath) {
			break
		}

		// continue walking entityPath
		var exists bool
		group, exists = group.entities[entityPath[pathIdx]]
		if !exists {
			detachOccured = true
			break
		}
	}

	if detachOccured {
		return nil, nil
	}
	return group, nil
}

// --- entityGroup ---

// entityGroup struct maintains state for all the entities at a certain
// level of the entityPath.  Hence one entityGroup per
// namespace which contains all workspaces within that namespace,
// one entityGroup for all namespaces etc
type entityGroup struct {
	cache *entityCache

	parent       *entityGroup
	parentEntity string

	entityCount int
	// entities[entity] points to an entity group
	entities map[string]*entityGroup

	// used to ensure that there is only 1
	// outstanding refresh per entityGroup
	refreshScheduled uint32
	// used to detect if inserts and deletes are
	// concurrent with fetch
	fetchInProgress bool

	// decides when a Count/List API call should
	// refresh the entityGroup
	expiresAt time.Time

	// used to detect detachment from parent/ancestor
	detached bool

	// log of concurrent inserts and deletes into
	// this entityGroup when fetchInProgress == true
	concLocalInserts map[string]bool
	concLocalDeletes map[string]bool

	fetchErrReapersWg sync.WaitGroup
	fetchErr          error
}

func newEntityGroup(parent *entityGroup, parentEntity string,
	ec *entityCache) *entityGroup {

	newGroup := &entityGroup{}
	newGroup.entities = make(map[string]*entityGroup, 0)
	newGroup.fetchInProgress = false
	newGroup.parent = parent
	newGroup.parentEntity = parentEntity
	newGroup.expiresAt = time.Date(0, time.January, 0, 0, 0, 0, 0, time.UTC)
	newGroup.cache = ec
	newGroup.detached = false
	newGroup.concLocalInserts = make(map[string]bool)
	newGroup.concLocalDeletes = make(map[string]bool)

	return newGroup
}

// called under rwMutex read lock
type refreshAction int

const (
	refreshPerform refreshAction = iota // caller must perform refresh
	refreshWait                  = iota // caller must wait for refresh to be done
	refreshIgnore                = iota // refresh not needed and no waiting
)

func (g *entityGroup) refreshNeeded(c Ctx) (action refreshAction) {
	if g.cache.neverExpires {
		return refreshIgnore
	}

	duration := time.Now().Sub(g.expiresAt)
	if duration >= 0 {
		c.Vlog("Refresh needed p:%s c:%d e:%s now:%s d:%t", g.parentEntity,
			g.entityCount, g.expiresAt.Format(time.RFC3339Nano),
			time.Now().Format(time.RFC3339Nano), g.detached)

		// pick a winning caller (out of multiple read-locked) to refresh
		// the group. Its normal to have multiple refreshes for different
		// entityGroups to be active at the same time. In rare scenarios,
		// its possible to have multiple refreshes active for same component
		// (but different entityGroup structs) in an entityPath.
		// For example - while workspaces for namespace1 is being refreshed,
		// its possible to delete all workspaces for namespace1 and then
		// create workspaces again under namespace1. In that scenario, two
		// different entityGroup structs for workspaces within
		// namespace1 are active at the same time (one of them is detached).
		swapped := atomic.CompareAndSwapUint32(&g.refreshScheduled, 0, 1)
		if swapped {
			// only 1 caller will see swapped = true so can modify
			// without write lock upgrade
			// possible writers: read-Locked-CAS-winner or write-Locked
			// refresh
			g.fetchErr = nil
			g.fetchInProgress = true
			return refreshPerform
		}

		if len(g.entities) == 0 {
			// wait for refresh since we don't have any data
			return refreshWait
		}
		// ok to serve stale data while refresh is in progress
	}

	return refreshIgnore
}

// called under rwMutex held in write mode
func (g *entityGroup) checkInsertEntity(c Ctx, entity string, local bool) {
	defer c.FuncIn("cache::checkInsertEntity",
		"e:%s l:%t p:%s c:%d d:%t",
		entity, local, g.parentEntity,
		g.entityCount, g.detached).Out()

	_, exists := g.entities[entity]

	if !exists {
		c.Vlog("cache::checkInsertEntity entity does not exist for "+
			"e:%s l:%t p:%s c:%d d:%t",
			entity, local, g.parentEntity,
			g.entityCount, g.detached)
		newGroup := newEntityGroup(g, entity, g.cache)
		g.entities[entity] = newGroup
		g.entityCount++
		if local && g.fetchInProgress {
			g.concLocalInserts[entity] = true

			// during refresh (unlocked), its possible to get insert
			// followed by delete or vice versa (each of insert and
			// delete respect locks). When merging the local updates,
			// we should pick up only the most recent action (either
			// insert or delete).
			// Since separate insert and delete logs exist,
			// this code ensures that theres only 1 of insert or delete
			// log for an entity during refresh
			delete(g.concLocalDeletes, entity)
		}
	}
}

// called under rwMutex read lock
func (g *entityGroup) getListCopy(c Ctx) []string {
	defer c.FuncIn("cache::getListCopy", "p:%s c:%d d:%t",
		g.parentEntity, g.entityCount,
		g.detached).Out()

	count := g.entityCount
	newList := make([]string, 0, count)
	for entity := range g.entities {
		newList = append(newList, entity)
	}

	if count != len(newList) {
		panic(fmt.Sprintf("BUG: newCount %d != len(newList) %d\n", count,
			len(newList)))
	}

	return newList
}

// invoked under rwMutex write lock
func (g *entityGroup) mergeLocalUpdates(c Ctx, fetchData map[string]bool) {
	defer c.FuncIn("cache::mergeLocalUpdates", "p:%s c:%d "+
		"d:%t lI:%d lD:%d", g.parentEntity, g.entityCount,
		g.detached, len(g.concLocalInserts),
		len(g.concLocalDeletes)).Out()

	mergedInserts := false
	mergedDeletes := false

	// an entity can only be present in either concLocalInserts
	// or concLocalDeletes log, checkDeleteEntity and
	// checkInsertEntity routines ensure this

	// ensure that all locally inserted entities are present
	// in fetchData
	for en := range g.concLocalInserts {
		if _, ok := g.concLocalDeletes[en]; ok {
			panic(fmt.Sprintf("Entity %q present in both insert "+
				" and delete logs", en))
		}
		mergedInserts = true
		fetchData[en] = true
	}

	// ensure that all locally deleted entities are absent
	// in fetchData
	for en := range g.concLocalDeletes {
		mergedDeletes = true
		delete(fetchData, en)
	}

	// discard current local updates since these have been merged
	if mergedInserts {
		g.concLocalInserts = make(map[string]bool)
	}

	if mergedDeletes {
		g.concLocalDeletes = make(map[string]bool)
	}
}

// reapFetchError is used by the callers waiting for
// refresh to reap the error from fetch. The fetch error
// is deposited into group before unsetting the refreshScheduled
// CAS and hence this wait is ok.
func (g *entityGroup) reapFetchError() error {
	defer g.fetchErrReapersWg.Done()
	for {
		val := atomic.LoadUint32(&g.refreshScheduled)
		if val == 0 {
			break
		}
		runtime.Gosched()
	}

	var err error
	if g.fetchErr != nil {
		// copy the error so that next refresh doesn't
		// clobber it in the group
		err = fmt.Errorf(g.fetchErr.Error())
	}
	return err
}

// refresh will update the group under write lock unless the fetched
// data has been invalidated by a local insert/delete. The
// error from fetcher is passed into refresh so that proper clean up can be done.
// No locks should be held when calling this function.
func (g *entityGroup) refresh(c Ctx, fetchData map[string]bool,
	ferr error) error {
	defer c.FuncIn("cache::refresh", "p:%s c:%d d:%t",
		g.parentEntity, g.entityCount,
		g.detached).Out()

	// takes the fetchList and merges it to group.entities under write lock
	g.cache.rwMutex.Lock()

	// order is important here
	// write lock is released after making sure that all the waiters
	// have reaped the error status. New waiters cannot be added since
	// write lock is held
	defer g.cache.rwMutex.Unlock()

	defer g.fetchErrReapersWg.Wait()

	defer func() { g.fetchInProgress = false }()

	// deposit the error before ack'ing the refresh
	g.fetchErr = ferr

	// refreshScheduled must be reset after depositing the fetch error
	if swapped := atomic.CompareAndSwapUint32(&g.refreshScheduled,
		1, 0); !swapped {

		g.fetchErr = fmt.Errorf("BUG: Concurrent refreshes scheduled")
		panic("BUG: EntityGroup can only have 1 refresh scheduled at any " +
			"time")
	}

	if ferr != nil {
		return ferr
	}
	// its ok to proceed if this group has been detached from parent (implies
	// all entities deleted and hence parent entity also deleted). The List or
	// Count implementation will handle this correctly

	// if local inserts and deletes happened concurrently with refresh
	// then merge them into the fetchData first. This ensures that fetchData
	// isn't stale in terms of local concurrent modifications
	g.mergeLocalUpdates(c, fetchData)

	// insert entities present in fetchData
	for entity := range fetchData {
		g.checkInsertEntity(c, entity, false)
	}

	// remove entities absent in fetchData
	for entity := range g.entities {
		if _, dbExists := fetchData[entity]; !dbExists {
			// Since entityGroup points to its parent, following
			// can cascade deletes up the parent path.
			g.cache.checkDeleteEntity(c, g, false, entity)
		}
	}

	g.expiresAt = time.Now().Add(g.cache.expiryDuration)
	return nil
}
