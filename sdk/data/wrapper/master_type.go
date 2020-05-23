// Copyright 2020 The CFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package wrapper

const AdminGetCluster = "/admin/getCluster"

type ClusterView struct {
	Name                string
	LeaderAddr          string
	CompactStatus       bool
	DisableAutoAlloc    bool
	Applied             uint64
	MaxDataPartitionID  uint64
	MaxMetaNodeID       uint64
	MaxMetaPartitionID  uint64
	DataNodeStat        *DataNodeSpaceStat
	MetaNodeStat        *MetaNodeSpaceStat
	VolStat             []*VolSpaceStat
	MetaNodes           []MetaNodeView
	DataNodes           []DataNodeView
	BadPartitionIDs     []BadPartitionView
	BadMetaPartitionIDs []BadPartitionView
}

type DataNodeSpaceStat struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB int64
	UsedRatio  string
}

type MetaNodeSpaceStat struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB int64
	UsedRatio  string
}

type VolSpaceStat struct {
	Name      string
	TotalGB   uint64
	UsedGB    uint64
	UsedRatio string
}

type DataNodeView struct {
	Addr   string
	Status bool
}

type MetaNodeView struct {
	ID     uint64
	Addr   string
	Status bool
}

type BadPartitionView struct {
	Path         string
	PartitionIDs []uint64
}
