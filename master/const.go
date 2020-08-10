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

package master

const (
	ParaHosts                 = "hosts"
	ParaDestAddr              = "destAddr"
	ParaNodeAddr              = "addr"
	ParaDiskPath              = "disk"
	ParaName                  = "name"
	ParaToken                 = "token"
	ParaTokenType             = "tokenType"
	ParaId                    = "id"
	ParaCount                 = "count"
	ParaReplicas              = "replicas"
	ParaDataPartitionType     = "type"
	ParaStart                 = "start"
	ParaEnable                = "enable"
	ParaThreshold             = "threshold"
	ParaVolCapacity           = "capacity"
	ParaVolOwner              = "owner"
	ParaAuthKey               = "authKey"
	ParaIsManual              = "isManual"
	ParaMetaPartitionCountKey = "mpCount"
	ParaEnableToken           = "enableToken"
	ParaVolMinWritableDPNum   = "minWritableDp"
	ParaVolMinWritableMPNum   = "minWritableMp"
)

const (
	DeleteExcessReplicationErr     = "DeleteExcessReplicationErr "
	AddLackReplicationErr          = "AddLackReplicationErr "
	CheckDataPartitionDiskErrorErr = "CheckDataPartitionDiskErrorErr  "
	GetAvailDataNodeHostsErr       = "GetAvailDataNodeHostsErr "
	GetAvailMetaNodeHostsErr       = "GetAvailMetaNodeHostsErr "
	GetDataReplicaFileCountInfo    = "GetDataReplicaFileCountInfo "
	DataNodeOfflineInfo            = "dataNodeOfflineInfo"
	DiskOfflineInfo                = "DiskOfflineInfo"
	HandleDataPartitionOfflineErr  = "HandleDataPartitionOffLineErr "
)

const (
	UnderlineSeparator = "_"
	CommaSeparator     = ","
)

const (
	defaultInitMetaPartitionCount                 = 1
	defaultMaxInitMetaPartitionCount              = 10
	defaultMaxMetaPartitionInodeID        uint64  = 1<<63 - 1
	defaultMetaPartitionInodeIDStep       uint64  = 1 << 24
	DefaultMetaNodeReservedMem            uint64  = 1 << 32
	RuntimeStackBufSize                           = 4096
	NodesAliveRate                        float32 = 0.5
	SpaceAvailRate                                = 0.90
	CheckMissFileReplicaTime                      = 600
	MinReadWriteDataPartitions                    = 20
	DefaultInitDataPartitions                     = 10
	EmptyCrcValue                         uint32  = 4045511210
	DefaultVolCapacity                            = 200
	LoadDataPartitionPeriod                       = 12 * 60 * 60
	VolExpandDataPartitionStepRatio               = 0.1
	VolMaxExpandDataPartitionCount                = 100
	VolWarningRatio                               = 0.7
	VolMinAvailSpaceRatio                         = 0.1
	VolReadWriteDataPartitionRatio                = 0.1
	DefaultRackName                               = "default"
	defaultMinusOfMaxInodeID                      = 1000
	defaultRangeOfCountDifferencesAllowed         = 50
	DefaultVolMinWritableDPNum                    = 5
	DefaultVolMinWritableMPNum                    = 1
	defaultOfflineChannelBufferCapacity           = 1000
)

const (
	OK     = iota
	Failed
)

const (
	VolNormal     uint8 = 0
	VolMarkDelete uint8 = 1
)
