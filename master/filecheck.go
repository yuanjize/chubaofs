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

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/storage"
	"github.com/chubaofs/chubaofs/util/log"
	"sort"
	"strconv"
	"time"
)

/*check File: recover File,if File lack or timeOut report or crc bad*/
func (partition *DataPartition) checkFile(clusterID string) {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicas(DefaultDataPartitionTimeOutSec)
	if len(liveReplicas) == 0 {
		return
	}

	if len(liveReplicas) < int(partition.ReplicaNum) {
		liveAddrs := make([]string, 0)
		for _, replica := range liveReplicas {
			liveAddrs = append(liveAddrs, replica.Addr)
		}
		unliveAddrs := make([]string, 0)
		for _, host := range partition.PersistenceHosts {
			if !contains(liveAddrs, host) {
				unliveAddrs = append(unliveAddrs, host)
			}
		}
		Warn(clusterID, fmt.Sprintf("vol[%v],partitionId[%v],liveAddrs[%v],unliveAddrs[%v]", partition.VolName, partition.PartitionID, liveAddrs, unliveAddrs))
	}

	partition.checkFileInternal(liveReplicas, clusterID)
	return
}

func (partition *DataPartition) checkFileInternal(liveReplicas []*DataReplica, clusterID string) {
	for _, fc := range partition.FileInCoreMap {
		extentId, err := strconv.ParseUint(fc.Name, 10, 64)
		if err != nil {
			continue
		}
		//history reason,don't check extentId =1
		if extentId == 1 {
			continue
		}
		if storage.IsTinyExtent(extentId) {
			partition.checkChunkFile(fc, liveReplicas, clusterID)
		} else {
			partition.checkExtentFile(fc, liveReplicas, clusterID)
		}
	}
}

func (partition *DataPartition) checkChunkFile(fc *FileInCore, liveReplicas []*DataReplica, clusterID string) {
	if fc.isCheckCrc() == false {
		return
	}
	fms, needRepair := fc.needCrcRepair(liveReplicas, proto.TinyPartition)

	if !needRepair {
		return
	}
	if !isSameSize(fms) {
		msg := fmt.Sprintf("CheckFileError size not match,cluster[%v],dpID[%v],", clusterID, partition.PartitionID)
		for _, fm := range fms {
			msg = msg + fmt.Sprintf("fm[%v]:size[%v]\n", fm.locIndex, fm.Size)
		}
		log.LogWarn(msg)
		return
	}
	msg := fmt.Sprintf("CheckFileError crc not match,cluster[%v],dpID[%v],", clusterID, partition.PartitionID)
	for _, fm := range fms {
		msg = msg + fmt.Sprintf("fm[%v]:%v\n", fm.locIndex, fm.ToString())
	}
	Warn(clusterID, msg)
	return
}

func (partition *DataPartition) checkExtentFile(fc *FileInCore, liveReplicas []*DataReplica, clusterID string) {
	if fc.isCheckCrc() == false {
		return
	}

	fms, needRepair := fc.needCrcRepair(liveReplicas, proto.ExtentPartition)

	if len(fms) < len(liveReplicas) && (time.Now().Unix()-fc.LastModify) > CheckMissFileReplicaTime {
		fileMissReplicaTime, ok := partition.FileMissReplica[fc.Name]
		if len(partition.FileMissReplica) > 400 {
			Warn(clusterID, fmt.Sprintf("partitionid[%v] has [%v] file missed replica", partition.PartitionID, len(partition.FileMissReplica)))
			return
		}
		if !ok {
			partition.FileMissReplica[fc.Name] = time.Now().Unix()
			return
		}
		if time.Now().Unix()-fileMissReplicaTime < CheckMissFileReplicaTime {
			return
		}
		liveAddrs := make([]string, 0)
		for _, replica := range liveReplicas {
			liveAddrs = append(liveAddrs, replica.Addr)
		}
		Warn(clusterID, fmt.Sprintf("partitionid[%v],file[%v],fms[%v],liveAddr[%v]", partition.PartitionID, fc.Name, fc.getFileMetaAddrs(), liveAddrs))
	}
	if !needRepair {
		return
	}
	if !isSameSize(fms) {
		return
	}

	fileCrcArr := fc.calculateCrcCount(fms)
	sort.Sort((FileCrcSorterByCount)(fileCrcArr))
	maxCountFileCrcIndex := len(fileCrcArr) - 1
	if fileCrcArr[maxCountFileCrcIndex].count == 1 {
		msg := fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v  File:%v  ExtentOffset diffrent between all Node  "+
			" it can not repair it ", clusterID, partition.PartitionID, fc.Name)
		msg += (FileCrcSorterByCount)(fileCrcArr).log()
		Warn(clusterID, msg)
		return
	}

	for index, crc := range fileCrcArr {
		if index != maxCountFileCrcIndex {
			badNode := crc.meta
			msg := fmt.Sprintf("checkFileCrcTaskErr clusterID[%v] partitionID:%v  File:%v  badCrc On :%v  ",
				clusterID, partition.PartitionID, fc.Name, badNode.getLocationAddr())
			msg += (FileCrcSorterByCount)(fileCrcArr).log()
			Warn(clusterID, msg)
		}
	}
	return
}
