package checktool

import (
	"github.com/chubaofs/chubaofs/proto"
)

type ClusterView struct {
	Name               string
	LeaderAddr         string
	CompactStatus      bool
	DisableAutoAlloc   bool
	Applied            uint64
	MaxDataPartitionID uint64
	MaxMetaNodeID      uint64
	MaxMetaPartitionID uint64
	DataNodeStat       *DataNodeSpaceStat
	MetaNodeStat       *MetaNodeSpaceStat
	VolStat            []*VolSpaceStat
	MetaNodes          []MetaNodeView
	DataNodes          []DataNodeView
	BadPartitionIDs    []BadPartitionView
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
	DiskPath     string
	PartitionIDs []uint64
}

// SimpleVolView defines the simple view of a volume
type SimpleVolView struct {
	ID                  uint64
	Name                string
	Owner               string
	DpReplicaNum        uint8
	MpReplicaNum        uint8
	Status              uint8
	Capacity            uint64 // GB
	RwDpCnt             int
	MpCnt               int
	DpCnt               int
	AvailSpaceAllocated uint64 //GB
}

type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
}

type MetaPartition struct {
	PartitionID      uint64
	Start            uint64
	End              uint64
	MaxNodeID        uint64
	IsManual         bool
	Replicas         []*MetaReplica
	ReplicaNum       uint8
	Status           int8
	volName          string
	PersistenceHosts []string
	Peers            []proto.Peer
	MissNodes        map[string]int64
}

type MetaReplica struct {
	Addr       string
	start      uint64
	end        uint64
	nodeId     uint64
	ReportTime int64
	Status     int8
	IsLeader   bool
}
