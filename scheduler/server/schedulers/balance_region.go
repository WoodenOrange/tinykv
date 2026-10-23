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
	"sort"

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

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	//限制同时进行的迁移数量
	if !s.IsScheduleAllowed(cluster) {
		return nil
	}
	//选出待调度store
	var stores []*core.StoreInfo
	for _, store := range cluster.GetStores() {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			stores = append(stores, store)
		}
	}
	//按大小升序排序
	sort.Slice(stores, func(i, j int) bool {
		if stores[i].GetRegionSize() == stores[j].GetRegionSize() {
			return stores[i].GetID() < stores[j].GetID()
		}
		return stores[i].GetRegionSize() < stores[j].GetRegionSize()
	})

	//优先迁移pending，然后follower，最后leader
	getters := []func(uint64, func(core.RegionsContainer)){
		cluster.GetPendingRegionsWithLock,
		cluster.GetFollowersWithLock,
		cluster.GetLeadersWithLock,
	}
	//对每个store，从三类各最多随机选取10次
	for i := len(stores) - 1; i >= 0; i-- {
		source := stores[i]
		for _, getRegions := range getters {
			for retry := 0; retry < balanceRegionRetryLimit; retry++ {
				var region *core.RegionInfo
				getRegions(source.GetID(), func(regions core.RegionsContainer) {
					if regions != nil {
						region = regions.RandomRegion(nil, nil)
					}
				})
				if region == nil {
					break
				}
				// 先退出子树读锁，再访问集群和创建 operator，避免锁重入。
				if op := s.createBalanceRegionOperator(cluster, stores, source, region); op != nil {
					return op
				}
			}
		}
	}
	return nil
}

// +++
// 检查迁移收益，向最小且未持有该 Region 的 Store 创建移动副本任务
func (s *balanceRegionScheduler) createBalanceRegionOperator(cluster opt.Cluster, stores []*core.StoreInfo, source *core.StoreInfo, region *core.RegionInfo) *operator.Operator {
	//检查有效leader，源store是否持有该region，副本数是否满足期望，是否已有operator在调度该region
	if region.GetLeader().GetId() == 0 || region.GetStorePeer(source.GetID()) == nil ||
		len(region.GetPeers()) != cluster.GetMaxReplicas() || s.opController.GetOperator(region.GetID()) != nil {
		return nil
	}
	//从已排序中选取目标store
	for _, target := range stores {
		//跳过已有副本的store
		if region.GetStorePeer(target.GetID()) != nil {
			continue
		}
		//检测大小，迁移后源仍比目标大，避免来回搬迁
		if source.GetRegionSize()-target.GetRegionSize() <= 2*region.GetApproximateSize() {
			return nil
		}
		//分配新peer
		peer, err := cluster.AllocPeer(target.GetID())
		if err != nil {
			return nil
		}
		//创建operator
		op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance,
			source.GetID(), target.GetID(), peer.GetId())
		if err != nil {
			return nil
		}
		return op
	}
	return nil
}
