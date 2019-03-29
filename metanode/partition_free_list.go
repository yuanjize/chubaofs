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

package metanode

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/cfs/proto"
	"github.com/chubaofs/cfs/third_party/juju/errors"
	"github.com/chubaofs/cfs/util/log"
	"os"
	"path"
	"runtime"
	"sync"
	"time"
)

const (
	AsyncDeleteInterval = 10 * time.Second
	UpdateVolTicket     = 5 * time.Minute
	BatchCounts         = 100
	TempFileValidTime   = 86400 //units: sec
	MarkDeleteInodeFile = "DELETE_INODE"
	MarkDeleteOptFlag   = os.O_RDWR | os.O_APPEND | os.O_CREATE
)

func (mp *metaPartition) startFreeList() (err error) {
	mp.deleteFp, err = os.OpenFile(path.Join(mp.config.RootDir,
		MarkDeleteInodeFile), MarkDeleteOptFlag, 0644)
	if err != nil {
		return
	}
	// start vol update ticket
	go mp.updateVolWorker()

	go mp.deleteWorker()
	go mp.checkFreelistWorker()
	return
}

func (mp *metaPartition) updateVolWorker() {
	t := time.NewTicker(UpdateVolTicket)
	reqURL := fmt.Sprintf("%s?name=%s", DataPartitionViewUrl, mp.config.VolName)
	for {
		select {
		case <-mp.stopC:
			t.Stop()
			return
		case <-t.C:
			// Get dataPartitionView
			respBody, err := postToMaster("GET", reqURL, nil)
			if err != nil {
				log.LogErrorf("[updateVol] %s", err.Error())
				break
			}
			dataView := new(DataPartitionsView)
			if err = json.Unmarshal(respBody, dataView); err != nil {
				log.LogErrorf("[updateVol] %s", err.Error())
				break
			}
			mp.vol.UpdatePartitions(dataView)
			log.LogDebugf("[updateVol] %v", dataView)
		}
	}
}

func (mp *metaPartition) deleteWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]*Inode, 0, BatchCounts)
	var tempFileSlice []*Inode
Begin:
	time.Sleep(AsyncDeleteInterval)
	for {
		buffSlice = buffSlice[:0]
		tempFileSlice = tempFileSlice[:0]
		select {
		case <-mp.stopC:
			return
		default:
		}
		if _, isLeader = mp.IsLeader(); !isLeader {
			goto Begin
		}
		curTime := time.Now()
		for idx = 0; idx < BatchCounts; idx++ {
			// batch get free inode from freeList
			ino := mp.freeList.Pop()
			if ino == nil {
				break
			}
			if ino.MarkDelete != 1 && ino.NLink == 0 {
				if ino.ModifyTime >= (curTime.Unix() - TempFileValidTime) {
					tempFileSlice = append(tempFileSlice, ino)
					continue
				}
			}
			buffSlice = append(buffSlice, ino)
		}
		for _, ino := range tempFileSlice {
			mp.freeList.Push(ino)
		}
		if len(buffSlice) == 0 {
			goto Begin
		}
		mp.storeDeletedInode(buffSlice...)
		mp.deleteExtent(buffSlice)
		if len(buffSlice) < BatchCounts {
			goto Begin
		}
		runtime.Gosched()
	}
}

func (mp *metaPartition) checkFreelistWorker() {
	var (
		idx      int
		isLeader bool
	)
	buffSlice := make([]*Inode, 0, BatchCounts)
	for {
		time.Sleep(time.Second)
		buffSlice = buffSlice[:0]
		select {
		case <-mp.stopC:
			return
		default:
		}
		if _, isLeader = mp.IsLeader(); isLeader {
			continue
		}
		for idx = 0; idx < BatchCounts; idx++ {
			// get the first item of list
			ino := mp.freeList.Pop()
			if ino == nil {
				break
			}
			buffSlice = append(buffSlice, ino)
		}
		if len(buffSlice) == 0 {
			continue
		}
		for _, ino := range buffSlice {
			if mp.internalHasInode(ino) {
				mp.freeList.Push(ino)
				continue
			}
			mp.storeDeletedInode(ino)
		}

	}
}

func (mp *metaPartition) deleteExtent(inoSlice []*Inode) {
	stepFunc := func(ext proto.ExtentKey) (err error) {
		// get dataNode View
		dp := mp.vol.GetPartition(ext.PartitionId)
		if dp == nil {
			err = errors.Errorf("unknown dataPartitionID=%d in vol",
				ext.PartitionId)
			return
		}
		// delete dataNode
		conn, err := mp.config.ConnPool.Get(dp.Hosts[0])
		defer mp.config.ConnPool.Put(conn, ForceCloseConnect)
		if err != nil {
			err = errors.Errorf("get conn from pool %s, "+
				"extents partitionId=%d, extentId=%d",
				err.Error(), ext.PartitionId, ext.ExtentId)
			return
		}
		p := NewExtentDeletePacket(dp, &ext)
		if err = p.WriteToConn(conn); err != nil {
			err = errors.Errorf("write to dataNode %s, %s", p.GetUniqueLogId(),
				err.Error())
			return
		}
		if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			err = errors.Errorf("read response from dataNode %s, %s",
				p.GetUniqueLogId(), err.Error())
			return
		}
		if p.ResultCode!=proto.OpOk{
			err = errors.Errorf("read response from dataNode %s, %s",
				p.GetUniqueLogId(), string(p.Data[:p.Size]))
			return
		}
		log.LogDebugf("[deleteExtent] %v", p.GetUniqueLogId())
		return
	}

	shouldCommit := make([]*Inode, 0, BatchCounts)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, ino := range inoSlice {
		wg.Add(1)
		go func(ino *Inode) {
			defer wg.Done()
			var reExt []proto.ExtentKey
			ino.Extents.Range(func(i int, v proto.ExtentKey) bool {
				if err := stepFunc(v); err != nil {
					reExt = append(reExt, v)
					log.LogWarnf("[deleteExtent] extentKey: %s, "+
						"err: %v", v.String(), err)
				}
				return true
			})
			if len(reExt) == 0 {
				mu.Lock()
				shouldCommit = append(shouldCommit, ino)
				mu.Unlock()
			} else {
				newIno := NewInode(ino.Inode, ino.Type)
				for _, ext := range reExt {
					newIno.Extents.Put(ext)
				}
				mp.freeList.Push(newIno)
			}
		}(ino)
	}
	wg.Wait()
	if len(shouldCommit) > 0 {
		bufSlice := make([]byte, 0, 8*len(shouldCommit))
		for _, ino := range shouldCommit {
			bufSlice = append(bufSlice, ino.MarshalKey()...)
		}
		// raft Commit
		_, err := mp.Put(opFSMInternalDeleteInode, bufSlice)
		if err != nil {
			for _, ino := range shouldCommit {
				mp.freeList.Push(ino)
			}
			log.LogWarnf("[deleteInodeTree] raft commit inode list: %v, "+
				"response %s", shouldCommit, err.Error())
		}
		log.LogDebugf("[deleteInodeTree] inode list: %v", shouldCommit)
	}

}

func (mp *metaPartition) storeDeletedInode(inos ...*Inode) {
	for _, ino := range inos {
		if _, err := mp.deleteFp.Write(ino.MarshalKey()); err != nil {
			log.LogWarnf("[storeDeletedInode] failed store inode=%d", ino.Inode)
			mp.freeList.Push(ino)
		}
	}
}
