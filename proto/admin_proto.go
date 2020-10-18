// Copyright 2018 The CFS Authors.
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

package proto

/*
 this struct is used to master send command to metanode
  or send command to datanode
*/

const (
	ReadOnlyToken  = 1
	ReadWriteToken = 2
)

type RegisterMetaNodeResp struct {
	ID uint64
}

type ClusterInfo struct {
	Cluster string
	Ip      string
}

type CreateDataPartitionRequest struct {
	PartitionType string
	PartitionId   uint64
	PartitionSize int
	VolumeId      string
}

type CreateDataPartitionResponse struct {
	PartitionId uint64
	Status      uint8
	Result      string
}

type DeleteDataPartitionRequest struct {
	DataPartitionType string
	PartitionId       uint64
	PartitionSize     int
}

type DeleteDataPartitionResponse struct {
	Status      uint8
	Result      string
	PartitionId uint64
}

type LoadDataPartitionRequest struct {
	PartitionType string
	PartitionId   uint64
}

type LoadDataPartitionResponse struct {
	PartitionType     string
	PartitionId       uint64
	VolName           string
	Used              uint64
	PartitionSnapshot []*File
	Status            uint8
	PartitionStatus   int
	Result            string
}

type File struct {
	Name     string
	Crc      uint32
	Size     uint32
	Modified int64
}

type LoadMetaPartitionMetricRequest struct {
	PartitionID uint64
	Start       uint64
	End         uint64
}

type LoadMetaPartitionMetricResponse struct {
	Start       uint64
	End         uint64
	Status      uint8
	Result      string
	PartitionID uint64
	DoCompare   bool
	ApplyID     uint64
	MaxInode    uint64
	DentryCount uint64
	Addr        string
}

type HeartBeatRequest struct {
	CurrTime   int64
	MasterAddr string
}

type PartitionReport struct {
	PartitionID          uint64
	PartitionStatus      int
	Total                uint64
	Used                 uint64
	DiskPath             string
	ExtentCount          int
	NeedCompare          bool
	AvaliTinyExtentCnt   int
	UnavaliTinyExtentCnt int
	VolName              string
}

type DataNodeHeartBeatResponse struct {
	Total                           uint64
	Used                            uint64
	Available                       uint64
	CreatedPartitionWeights         uint64 //volCnt*volsize
	RemainWeightsForCreatePartition uint64 //all-usedvolsWieghts
	CreatedPartitionCnt             uint32
	MaxWeightsForCreatePartition    uint64
	RackName                        string
	PartitionInfo                   []*PartitionReport
	Status                          uint8
	Result                          string
}

type MetaPartitionReport struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Status      int
	MaxInodeID  uint64
	IsLeader    bool
	VolName     string
	InodeCount  uint64
	DentryCount uint64
}

type MetaNodeHeartbeatResponse struct {
	RackName          string
	Total             uint64
	Used              uint64
	MetaPartitionInfo []*MetaPartitionReport
	Status            uint8
	Result            string
}

type DeleteFileRequest struct {
	VolId uint64
	Name  string
}

type DeleteFileResponse struct {
	Status uint8
	Result string
	VolId  uint64
	Name   string
}

type DeleteMetaPartitionRequest struct {
	PartitionID uint64
}

type DeleteMetaPartitionResponse struct {
	PartitionID uint64
	Status      uint8
	Result      string
}

type UpdateMetaPartitionRequest struct {
	PartitionID uint64
	VolName     string
	Start       uint64
	End         uint64
}

type UpdateMetaPartitionResponse struct {
	PartitionID uint64
	VolName     string
	End         uint64
	Status      uint8
	Result      string
}

type MetaPartitionOfflineRequest struct {
	PartitionID uint64
	VolName     string
	RemovePeer  Peer
	AddPeer     Peer
}

type MetaPartitionOfflineResponse struct {
	PartitionID uint64
	VolName     string
	Status      uint8
	Result      string
}

// AddMetaPartitionRaftMemberRequest defines the request of add raftMember a meta partition.
type AddMetaPartitionRaftMemberRequest struct {
	PartitionId uint64
	AddPeer     Peer
}

// RemoveMetaPartitionRaftMemberRequest defines the request of add raftMember a meta partition.
type RemoveMetaPartitionRaftMemberRequest struct {
	PartitionId uint64
	RemovePeer  Peer
}

type Token struct {
	TokenType int8
	Value     string
	VolName   string
}
