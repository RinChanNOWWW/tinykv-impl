// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"fmt"
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeSlice []*core.StoreInfo

func (s storeSlice) Len() int {
	return len(s)
}
func (s storeSlice) Less(i, j int) bool {
	// desc ord
	return s[i].GetRegionSize() > s[j].GetRegionSize()
}
func (s storeSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// First, the Scheduler will select all suitable stores.
	// Then sort them according to their region size.
	// Then the Scheduler tries to find regions to move from the store with the biggest region size.
	maxDownTime := cluster.GetMaxStoreDownTime()
	suitableStores := make(storeSlice, 0)
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() <= maxDownTime {
			log.Infof("suitable store: %+v, size: %d", store.GetID(), store.GetRegionSize())
			suitableStores = append(suitableStores, store)
		}
	}
	sort.Sort(suitableStores)
	var suitableRegion *core.RegionInfo
	i := 0
	for i = 0; i < suitableStores.Len(); i += 1 {
		// The scheduler will try to find the region most suitable for moving in the store.
		// First, it will try to select a pending region because pending may mean the disk is overloaded.
		cluster.GetPendingRegionsWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			suitableRegion = rc.RandomRegion(nil, nil)
		})
		if suitableRegion != nil {
			break
		}
		// If there isn’t a pending region, it will try to find a follower region.
		cluster.GetFollowersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			suitableRegion = rc.RandomRegion(nil, nil)
		})
		if suitableRegion != nil {
			break
		}
		// If it still cannot pick out one region, it will try to pick leader regions.
		cluster.GetLeadersWithLock(suitableStores[i].GetID(), func(rc core.RegionsContainer) {
			suitableRegion = rc.RandomRegion(nil, nil)
		})
		if suitableRegion != nil {
			break
		}
		// Finally, it will select out the region to move,
		// or the Scheduler will try the next store which has a smaller region size until all stores will have been tried.
	}
	if suitableRegion == nil {
		log.Warn("suitable region not found")
		return nil
	}
	suitableStore := suitableStores[i]
	// After you pick up one region to move, the Scheduler will select a store as the target.
	// Actually, the Scheduler will select the store with the smallest region size.
	// Then the Scheduler will judge whether this movement is valuable,
	// by checking the difference between region sizes of the original store and the target store.
	// If the difference is big enough (the difference has to be bigger than two times the approximate size of the region),
	storeIds := suitableRegion.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		log.Warnf("<region %d> not has enough replicas", suitableRegion.GetID())
		return nil
	}
	var targetStore *core.StoreInfo
	// begin at the smallest store
	for j := suitableStores.Len() - 1; j > i; j -= 1 {
		// region not in the store
		if _, ok := storeIds[suitableStores[j].GetID()]; !ok {
			targetStore = suitableStores[j]
			break
		}
	}
	if targetStore == nil {
		log.Warn("target store not found")
		return nil
	}
	if suitableStore.GetRegionSize()-targetStore.GetRegionSize() < 2*suitableRegion.GetApproximateSize() {
		log.Warnf("target <store %d> not valuable", targetStore.GetID())
		return nil
	}
	// the Scheduler should allocate a new peer on the target store and create a move peer operator.
	newPeer, err := cluster.AllocPeer(targetStore.GetID())
	if err != nil {
		log.Errorf("<store %d> alloc new peer failed && err=%+v", targetStore.GetID(), err)
		return nil
	}
	op, err := operator.CreateMovePeerOperator(
		fmt.Sprintf(
			"<region %d> move from <store %d> to <store %d>",
			suitableRegion.GetID(),
			suitableStore.GetID(),
			targetStore.GetID(),
		),
		cluster,
		suitableRegion,
		operator.OpBalance,
		suitableStore.GetID(),
		targetStore.GetID(),
		newPeer.GetId(),
	)
	if err != nil {
		log.Errorf("create move peer operator failed && err=%+v", err)
		return nil
	}
	return op
}
