// Copyright 2018 The Containerfs Authors.
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

package datanode

import (
	"fmt"
	"hash/crc32"
	"net"
	"sync"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/log"
)

// DoStreamExtentFixRepair executed on follower node of data partition.
// It receive from leader notifyRepair command extent file repair.
func (dp *dataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.FileInfo) {
	defer wg.Done()
	err := dp.streamRepairExtent(remoteExtentInfo)
	if err != nil {
		localExtentInfo, opErr := dp.GetExtentStore().GetWatermark(uint64(remoteExtentInfo.FileId), false)
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "partition[%v] remote[%v] local[%v]",
			dp.partitionId, remoteExtentInfo, localExtentInfo)
		log.LogErrorf("action[doStreamExtentFixRepair] err[%v].", err)
	}
}

//extent file repair function,do it on follower host
func (dp *dataPartition) streamRepairExtent(remoteExtentInfo *storage.FileInfo) (err error) {
	store := dp.GetExtentStore()
	if !store.IsExistExtent(uint64(remoteExtentInfo.FileId)) {
		return
	}

	// Get local extent file info
	localExtentInfo, err := store.GetWatermark(uint64(remoteExtentInfo.FileId), false)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}

	// Get need fix size for this extent file
	needFixSize := remoteExtentInfo.Size - localExtentInfo.Size

	// Create streamRead packet, it offset is local extentInfoSize, size is needFixSize
	request := NewStreamReadPacket(dp.ID(), remoteExtentInfo.FileId, int(localExtentInfo.Size), int(needFixSize))
	var conn *net.TCPConn

	// Get a connection to leader host
	conn, err = gConnPool.Get(remoteExtentInfo.Source)
	if err != nil {
		gConnPool.Put(conn, true)
		return errors.Annotatef(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	defer gConnPool.Put(conn, true)

	// Write OpStreamRead command to leader
	if err = request.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "streamRepairExtent send streamRead to host[%v] error", remoteExtentInfo.Source)
		log.LogErrorf("action[streamRepairExtent] err[%v].", err)
		return
	}
	currFixOffset := localExtentInfo.Size
	for currFixOffset < remoteExtentInfo.Size {
		// If local extent size has great remoteExtent file size ,then break
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}
		localExtentInfo, err = store.GetWatermark(uint64(remoteExtentInfo.FileId), false)
		if err != nil {
			err = errors.Annotatef(err, "streamRepairExtent GetWatermark error")
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return err
		}
		if localExtentInfo.Size > currFixOffset {
			err = errors.Annotatef(err, "streamRepairExtent unavali fix localSize(%v) "+
				"remoteSize(%v) want fixOffset(%v) data error", localExtentInfo.Size, remoteExtentInfo.Size, currFixOffset)
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return err
		}

		reply := NewPacket()
		// Read 64k stream repair packet
		if err = reply.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent receive data error")
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Annotatef(err, "streamRepairExtent receive opcode error(%v) ", string(reply.Data[:reply.Size]))
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID || reply.FileID != request.FileID {
			err = errors.Annotatef(err, "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return
		}

		log.LogInfof("action[streamRepairExtent] partition(%v) extent(%v) start fix from (%v)"+
			" remoteSize(%v) localSize(%v).", dp.ID(), remoteExtentInfo.FileId,
			remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset)

		if reply.Crc != crc32.ChecksumIEEE(reply.Data[:reply.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch partition(%v) extent(%v) start fix from (%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v)", dp.ID(), remoteExtentInfo.FileId,
				remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return errors.Annotatef(err, "streamRepairExtent receive data error")
		}
		// Write it to local extent file
		if err = store.Write(uint64(localExtentInfo.FileId), int64(currFixOffset), int64(reply.Size), reply.Data, reply.Crc); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent repair data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}
		currFixOffset += uint64(reply.Size)

	}
	return

}
