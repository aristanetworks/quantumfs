// Copyright (c) 2016 Arista Networks, Inc.  All rights reserved.
// Arista Networks, Inc. Confidential and Proprietary.

package cql

import (
	"fmt"
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

// Error handling strategy: TODO
// The entityCache APIs do not interact directly with CQL datastore
// and hence there are no error returns. The cache
// however refreshes remotely inserted/deleted data which involves
// CQL interactions and can potentially fail. Current error handling
// strategy is that - CQL session used to refresh cache auto-corrects and so during
// the period where the session is disconnected, the cache is stale
// in terms of remote operations. This is perfectly ok from workspace
// DB API expectations. The workspace DB API supports reporting errors.

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
type fetchEntities func(arg interface{}, entityPath ...string) map[string]bool

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

	// used to setup/update expiresAt of entityGroup
	expiryDuration time.Duration
}

// -- implementation of entityCache API ---

func newEntityCache(levels int, expiryDuration time.Duration,
	fetcherArg interface{}, fetcher fetchEntities) *entityCache {

	c := &entityCache{}
	c.levels = levels
	c.fetcher = fetcher
	c.fetcherArg = fetcherArg
	c.expiryDuration = expiryDuration

	c.root = newEntityGroup(nil, "", c)
	return c
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
func (c *entityCache) InsertEntities(entityPath ...string) {

	if len(entityPath) == 0 {
		panic("Invalid argument: Specify at least one entity")
	}

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	group := c.root
	for _, entity := range entityPath {
		group.checkInsertEntity(entity, true)
		group = group.entities[entity]
	}
}

// DeleteEntities MUST be called with complete entityPath
// ie entity at all levels.
// DeleteEntities(namespace, workspace) - Valid
// DeleteEntities() - Invalid
// DeleteEntities(workspace) - Invalid
func (c *entityCache) DeleteEntities(entityPath ...string) {

	if len(entityPath) == 0 {
		panic("Invalid argument: Specify at least one entity")
	}

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	c.checkDeleteEntity(c.root, true, entityPath...)
	return
}

// Unlike InsertEntities and DeleteEntities, the CountEntities and ListEntities
// routines involve read of entityGroup (read-lock), DB-fetch (unlock)
// and update of entityGroup (write-lock). Hence the locks are not
// managed solely in the top routines
// CountEntities() - Valid
// CountEntities(namespace) - Valid
// CountEntities(workspace) - Invalid
func (c *entityCache) CountEntities(entityPath ...string) int {
	var count int
	c.rwMutex.RLock()
	// getEntityCountListGroup releases the read lock and
	// re-acquires it when refresh is involved. However,
	// read lock is held upon return from getEntityCountListGroup
	defer c.rwMutex.RUnlock()

	group := c.getEntityCountListGroup(entityPath...)
	if group != nil {
		count = group.entityCount
	}

	return count
}

// same constraints on entityPath argument as CountEntities
func (c *entityCache) ListEntities(entityPath ...string) []string {
	var list []string
	c.rwMutex.RLock()
	// getEntityCountListGroup releases the read lock and
	// re-acquires it when refresh is involved. However,
	// read lock is held upon return from getEntityCountListGroup
	defer c.rwMutex.RUnlock()

	group := c.getEntityCountListGroup(entityPath...)
	if group != nil {
		list = group.getListCopy()
	}

	return list
}

// used by unit tests to simulate different conditions
// same constraints on entityPath argument as CountEntities
func (c *entityCache) enableCqlRefresh(entityPath ...string) {

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	var group *entityGroup
	if group = c.getLastEntityGroup(c.root, entityPath...); group == nil {
		return
	}

	group.expiresAt = time.Date(0, time.January, 0, 0, 0, 0, 0, time.UTC)
}

// used by unit tests to simulate different conditions
// same constraints on entityPath argument as CountEntities
func (c *entityCache) disableCqlRefresh(maxDelay time.Duration,
	entityPath ...string) {

	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()

	var group *entityGroup
	if group = c.getLastEntityGroup(c.root, entityPath...); group == nil {
		return
	}

	group.expiresAt = time.Now().Add(maxDelay)
}

// -- helpers that operate on multiple entityGroups ---

// called under rwMutex write lock
// returns the entityGroup for the last entity in entityPath
// traversal of the entityPath starts in group argument
// if any entity is absent then traversal stops and nil is returned
func (c *entityCache) getLastEntityGroup(group *entityGroup,
	entityPath ...string) *entityGroup {

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
// if entityGroup is c.root then entityPath is complete entityPath
// this routine will be invoked with at least one entity in entityPath
func (c *entityCache) checkDeleteEntity(group *entityGroup, local bool,
	entityPath ...string) {

	// Its possible to specify only some leading portion
	// of the complete entityPath. Even though its a delete
	// in-between the entityPath it frees up the
	// entityGroups along the complete entity path correctly.

	handlePartialPathDelete := false
	if len(entityPath) < c.levels {
		handlePartialPathDelete = true
	}

	if group = c.getLastEntityGroup(group, entityPath...); group == nil {
		return
	}

	// entityGroup of the last entity in entityPath
	group = group.parent
	entity := entityPath[len(entityPath)-1]

	for {

		// navigate and mark all the child entityGroups as
		// detached if handlePartialPathDelete == true
		if handlePartialPathDelete == true {
			c.markChildEntityGroupsDetached(group.entities[entity])
			// since we start at the end of entityPath and
			// we have taken care of all child entityGroups
			// off of last entity, we don't need to do more
			// such markings as part of this partial delete
			handlePartialPathDelete = false
		}
		group.entities[entity].parent = nil
		// this turns the entityGroup pointed by entity into garbage
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
func (c *entityCache) markChildEntityGroupsDetached(group *entityGroup) {
	// we don't need to maintain any concLocalDeletes logs since
	// the entire group is being deleted
	if group != nil {
		group.detached = true
		for entity := range group.entities {
			c.markChildEntityGroupsDetached(group.entities[entity])
		}
	}
}

// invoked under rwMutex read lock, does a unlocked fetch, refresh under
// write lock and finally re-acquires read lock
// returns non-nil entityGroup whose list or count can be extracted
// after doing a refresh if needed.
// returns nil entityGroup if there are detachments in entityPath
func (c *entityCache) getEntityCountListGroup(entityPath ...string) *entityGroup {

	detachOccured := false
	group := c.root
	// this routine can be called without entityPath
	// eg: root level
	for i := 0; true; i++ {

		// for each entity in given entityPath
		// check if corresponding entityGroup needs to be
		// refreshed
		needed := group.refreshNeeded()
		if needed {
			c.rwMutex.RUnlock()
			// fetch data from CQL without locking cache
			fetchList := c.fetcher(c.fetcherArg, entityPath[:i]...)
			// update the group under write lock unless the fetched
			// data has been invalidated by a local insert/delete
			group.refresh(fetchList)

			c.rwMutex.RLock()
			// in between the release of write lock in group.refresh()
			// and acquire of read lock here, there can be many local inserts
			// or deletes that may happen. Since each local update also
			// maintains cache local coherency, we'll see all local updates
			// correctly.
			//
			// Possible states include:
			// - current group has got detached from parent
			//   detachment can be from immediate parent, an
			//   ancestor detachment, child detachement
			//     return count zero and empty list
			//     ancestor and child detachment scenarios can happen only
			//     when maxEntityPath in List/Count > 1
			// - next entity in the entity path has got deleted
			//     return zero count and empty list
			// - expiryDuration time has elapsed since the merge and so
			//   we might be looking at stale remote information
			//     this is no different than remote information getting
			//     in DB as soon as we completed running fetcher. We just use
			//     the cache information we have.
		}

		// current group got detached
		if group.detached {
			detachOccured = true
			break
		}

		// we have completed walk
		if i == len(entityPath) {
			break
		}

		// continue walking entityPath
		var exists bool
		// entity = entityPath[i] removed from group
		group, exists = group.entities[entityPath[i]]
		if !exists {
			detachOccured = true
			break
		}
	}

	if detachOccured {
		return nil
	}
	return group
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
}

func newEntityGroup(parent *entityGroup, parentEntity string,
	c *entityCache) *entityGroup {

	newGroup := &entityGroup{}
	newGroup.entities = make(map[string]*entityGroup, 0)
	newGroup.fetchInProgress = false
	newGroup.parent = parent
	newGroup.parentEntity = parentEntity
	newGroup.expiresAt = time.Now()
	newGroup.cache = c
	newGroup.detached = false
	newGroup.concLocalInserts = make(map[string]bool)
	newGroup.concLocalDeletes = make(map[string]bool)

	return newGroup
}

// called under rwMutex read lock
func (g *entityGroup) refreshNeeded() bool {

	duration := time.Now().Sub(g.expiresAt)
	if duration >= 0 {
		// pick a winning caller (out of multiple read-locked) to refresh the group
		// Its normal to have multiple refreshes for different entityGroups to be
		// active at the same time. In rare scenarios, its possible to have multiple refreshes
		// active for same component (but different entityGroup structs) in an entityPath.
		// For example - while workspaces for namespace1 is being refreshed, its possible
		// to delete all workspaces for namespace1 and then create workspaces again under
		// namespace1. In that scenario, two different entityGroup structs for workspaces within
		// namespace1 are active at the same time (one of them is detached).
		swapped := atomic.CompareAndSwapUint32(&g.refreshScheduled, 0, 1)
		if swapped {
			// only 1 caller will see swapped = true so can modify without write lock
			// upgrade
			g.fetchInProgress = true
		}
		return swapped
	}

	return false
}

// called under rwMutex held in write mode
func (g *entityGroup) checkInsertEntity(entity string, local bool) {
	_, exists := g.entities[entity]

	if !exists {
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
func (g *entityGroup) getListCopy() []string {
	count := g.entityCount
	newList := make([]string, 0, count)
	for entity := range g.entities {
		newList = append(newList, entity)
	}

	if count != len(newList) {
		panic(fmt.Sprintf("BUG: newCount %d != len(newList) %d\n", count, len(newList)))
	}

	return newList
}

// invoked under rwMutex write lock
func (g *entityGroup) mergeLocalUpdates(fetchData map[string]bool) {

	mergedInserts := false
	mergedDeletes := false

	// an entity can only be present in either concLocalInserts
	// or concLocalDeletes log, checkDeleteEntity and
	// checkInsertEntity routines ensure this

	// ensure that all locally inserted entities are present
	// in fetchData
	for en := range g.concLocalInserts {
		if _, ok := g.concLocalDeletes[en]; ok {
			panic(fmt.Sprintf("Entity %q present in both insert and delete logs",
				en))
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

func (g *entityGroup) refresh(fetchData map[string]bool) {

	// takes the fetchList and merges it to group.entities under write lock
	g.cache.rwMutex.Lock()
	defer g.cache.rwMutex.Unlock()

	g.fetchInProgress = false
	if swapped := atomic.CompareAndSwapUint32(&g.refreshScheduled, 1, 0); !swapped {
		panic("BUG: EntityGroup can only have 1 refresh scheduled at any time")
	}

	// its ok to proceed if this group has been detached from parent (implies
	// all entities deleted and hence parent entity also deleted). The List or
	// Count implementation will handle this correctly

	// if local inserts and deletes happened concurrently with refresh
	// then merge them into the fetchData first. This ensures that fetchData
	// isn't stale in terms of local concurrent modifications
	g.mergeLocalUpdates(fetchData)

	// insert entities present in fetchData
	for entity := range fetchData {
		g.checkInsertEntity(entity, false)
	}

	// remove entities absent in fetchData
	for entity := range g.entities {
		if _, dbExists := fetchData[entity]; !dbExists {
			// Since entityGroup points to its parent, following
			// can cascade deletes up the parent path.
			g.cache.checkDeleteEntity(g, false, entity)
		}
	}

	g.expiresAt = time.Now().Add(g.cache.expiryDuration)
	return
}
