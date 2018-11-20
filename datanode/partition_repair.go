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
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/log"
)

//every  datapartion  file metas used for auto repairt
type MembersFileMetas struct {
	Index                  int                       //index on data partionGroup
	files                  map[int]*storage.FileInfo //storage file on datapartiondisk meta
	NeedDeleteExtentsTasks []*storage.FileInfo       //generator delete extent file task
	NeedAddExtentsTasks    []*storage.FileInfo       //generator add extent file task
	NeedFixFileSizeTasks   []*storage.FileInfo       //generator fixSize file task
}

func NewMemberFileMetas() (mf *MembersFileMetas) {
	mf = &MembersFileMetas{
		files:                  make(map[int]*storage.FileInfo),
		NeedDeleteExtentsTasks: make([]*storage.FileInfo, 0),
		NeedAddExtentsTasks:    make([]*storage.FileInfo, 0),
		NeedFixFileSizeTasks:   make([]*storage.FileInfo, 0),
	}
	return
}

//files repair check
func (dp *dataPartition) fileRepair() {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[fileRepair] partition[%v] start.",
		dp.partitionId)

	// Get all data partition group member about file metas
	allMembers, err := dp.getAllMemberFileMetas()
	if err != nil {
		log.LogErrorf("action[fileRepair] partition[%v] err[%v].",
			dp.partitionId, err)
		log.LogErrorf(errors.ErrorStack(err))
		return
	}
	dp.generatorFilesRepairTasks(allMembers) //generator file repair task
	err = dp.NotifyRepair(allMembers)        //notify host to fix it
	if err != nil {
		log.LogErrorf("action[fileRepair] partition[%v] err[%v].",
			dp.partitionId, err)
		log.LogError(errors.ErrorStack(err))
	}
	for _, fixExtentFile := range allMembers[0].NeedFixFileSizeTasks {
		dp.streamRepairExtent(fixExtentFile) //fix leader filesize
	}
	finishTime := time.Now().UnixNano()
	log.LogInfof("action[fileRepair] partition[%v] finish cost[%vms].",
		dp.partitionId, (finishTime-startTime)/int64(time.Millisecond))
}

func (dp *dataPartition) getLocalFileMetas() (fileMetas *MembersFileMetas, err error) {
	var (
		extentFiles []*storage.FileInfo
	)
	if extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter()); err != nil {
		return
	}
	files := make([]*storage.FileInfo, 0)
	files = append(files, extentFiles...)

	fileMetas = NewMemberFileMetas()
	for _, file := range files {
		fileMetas.files[file.FileId] = file
	}
	return
}

// Get all data partition group ,about all files meta
func (dp *dataPartition) getAllMemberFileMetas() (allMemberFileMetas []*MembersFileMetas, err error) {
	allMemberFileMetas = make([]*MembersFileMetas, len(dp.replicaHosts))
	files := make([]*storage.FileInfo, 0)
	// get local extent file metas
	files, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter())
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberFileMetas extent dataPartition[%v] GetAllWaterMark", dp.partitionId)
		return
	}
	// write tiny files meta to extent files meta
	leaderFileMetas := NewMemberFileMetas()
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberFileMetas dataPartition[%v] GetAllWaterMark", dp.partitionId)
		return
	}
	for _, fi := range files {
		leaderFileMetas.files[fi.FileId] = fi
	}
	allMemberFileMetas[0] = leaderFileMetas
	// leader files meta has ready

	// get remote files meta by opGetAllWaterMarker cmd
	p := NewGetAllWaterMarker(dp.partitionId)
	for i := 1; i < len(dp.replicaHosts); i++ {
		var conn *net.TCPConn
		target := dp.replicaHosts[i]
		conn, err = gConnPool.Get(target) //get remote connect
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberFileMetas  dataPartition[%v] get host[%v] connect", dp.partitionId, target)
			return
		}
		err = p.WriteToConn(conn) //write command to remote host
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberFileMetas dataPartition[%v] write to host[%v]", dp.partitionId, target)
			return
		}
		err = p.ReadFromConn(conn, proto.NoReadDeadlineTime) //read it response
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberFileMetas dataPartition[%v] read from host[%v]", dp.partitionId, target)
			return
		}
		fileInfos := make([]*storage.FileInfo, 0)
		err = json.Unmarshal(p.Data[:p.Size], &fileInfos)
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberFileMetas dataPartition[%v] unmarshal json[%v]", dp.partitionId, string(p.Data[:p.Size]))
			return
		}
		gConnPool.Put(conn, true)
		slaverFileMetas := NewMemberFileMetas()
		for _, fileInfo := range fileInfos {
			slaverFileMetas.files[fileInfo.FileId] = fileInfo
		}
		allMemberFileMetas[i] = slaverFileMetas
	}
	return
}

//generator file task
func (dp *dataPartition) generatorFilesRepairTasks(allMembers []*MembersFileMetas) {
	dp.generatorAddExtentsTasks(allMembers) //add extentTask
	dp.generatorFixFileSizeTasks(allMembers)
	dp.generatorDeleteExtentsTasks(allMembers)

}

/* pasre all extent,select maxExtentSize to member index map
 */
func (dp *dataPartition) mapMaxSizeExtentToIndex(allMembers []*MembersFileMetas) (maxSizeExtentMap map[int]int) {
	leader := allMembers[0]
	maxSizeExtentMap = make(map[int]int)
	for fileId := range leader.files { //range leader all extentFiles
		maxSizeExtentMap[fileId] = 0
		var maxFileSize uint64
		for index := 0; index < len(allMembers); index++ {
			member := allMembers[index]
			_, ok := member.files[fileId]
			if !ok {
				continue
			}
			if maxFileSize < member.files[fileId].Size {
				maxFileSize = member.files[fileId].Size
				maxSizeExtentMap[fileId] = index //map maxSize extentId to allMembers index
			}
		}
	}
	return
}

/*generator add extent if follower not have this extent*/
func (dp *dataPartition) generatorAddExtentsTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	leaderAddr := dp.replicaHosts[0]
	for fileId, leaderFile := range leader.files {
		if fileId <= storage.TinyChunkCount {
			continue
		}
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.files[fileId]; !ok {
				addFile := &storage.FileInfo{Source: leaderAddr, FileId: fileId, Size: leaderFile.Size, Inode: leaderFile.Inode}
				follower.NeedAddExtentsTasks = append(follower.NeedAddExtentsTasks, addFile)
				log.LogInfof("action[generatorAddExtentsTasks] partition[%v] addFile[%v].", dp.partitionId, addFile)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorFixFileSizeTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	maxSizeExtentMap := dp.mapMaxSizeExtentToIndex(allMembers) //map maxSize extentId to allMembers index
	for fileId, leaderFile := range leader.files {
		maxSizeExtentIdIndex := maxSizeExtentMap[fileId]
		maxSize := allMembers[maxSizeExtentIdIndex].files[fileId].Size
		sourceAddr := dp.replicaHosts[maxSizeExtentIdIndex]
		inode := leaderFile.Inode
		for index := 0; index < len(allMembers); index++ {
			if index == maxSizeExtentIdIndex {
				continue
			}
			extentInfo, ok := allMembers[index].files[fileId]
			if !ok {
				continue
			}
			if extentInfo.Size < maxSize {
				fixExtent := &storage.FileInfo{Source: sourceAddr, FileId: fileId, Size: maxSize, Inode: inode}
				allMembers[index].NeedFixFileSizeTasks = append(allMembers[index].NeedFixFileSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixFileSizeTasks] partition[%v] fixExtent[%v].", dp.partitionId, fixExtent)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorDeleteExtentsTasks(allMembers []*MembersFileMetas) {
	store := dp.extentStore
	deletes := store.GetDelObjects()
	leaderAddr := dp.replicaHosts[0]
	for _, deleteFileId := range deletes {
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.files[int(deleteFileId)]; ok {
				deleteFile := &storage.FileInfo{Source: leaderAddr, FileId: int(deleteFileId), Size: 0}
				follower.NeedDeleteExtentsTasks = append(follower.NeedDeleteExtentsTasks, deleteFile)
				log.LogInfof("action[generatorDeleteExtentsTasks] partition[%v] deleteFile[%v].", dp.partitionId, deleteFile)
			}
		}
	}
}

/*notify follower to repair dataPartition extentStore*/
func (dp *dataPartition) NotifyRepair(members []*MembersFileMetas) (err error) {
	var wg sync.WaitGroup
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := NewNotifyRepair(dp.partitionId) //notify all follower to repairt task,send opnotifyRepair command
			var conn *net.TCPConn
			target := dp.replicaHosts[index]
			conn, err = gConnPool.Get(target)
			if err != nil {
				return
			}
			p.Data, err = json.Marshal(members[index])
			p.Size = uint32(len(p.Data))
			err = p.WriteToConn(conn)
			if err != nil {
				gConnPool.Put(conn, true)
				return
			}
			err = p.ReadFromConn(conn, proto.NoReadDeadlineTime)
			if err != nil {
				gConnPool.Put(conn, true)
				return
			}
			gConnPool.Put(conn, true)
		}(i)
	}
	wg.Wait()
	return
}
