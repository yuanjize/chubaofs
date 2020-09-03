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
	"github.com/chubaofs/chubaofs/master"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http"
	"os"
	"strings"
	"time"
)

func (mp *metaPartition) initInode(ino *Inode) {
	for {
		time.Sleep(10 * time.Nanosecond)
		select {
		case <-mp.stopC:
			return
		default:
			// check first root inode
			if mp.hasInode(ino) {
				return
			}
			if !mp.raftPartition.IsLeader() {
				continue
			}
			data, err := ino.Marshal()
			if err != nil {
				log.LogFatalf("[initInode] marshal: %s", err.Error())
			}
			// put first root inode
			resp, err := mp.Put(opCreateInode, data)
			if err != nil {
				log.LogFatalf("[initInode] raft sync: %s", err.Error())
			}
			p := &Packet{}
			p.ResultCode = resp.(uint8)
			log.LogDebugf("[initInode] raft sync: response status = %v.",
				p.GetResultMesg())
			return
		}
	}
}

func (mp *metaPartition) openFile(ino *Inode) (status uint8) {
	item := mp.inodeTree.Get(ino)
	if item == nil {
		status = proto.OpNotExistErr
		return
	}
	item.(*Inode).AccessTime = ino.AccessTime
	status = proto.OpOk
	return
}

func (mp *metaPartition) offlinePartition() (err error) {
	return
}

func (mp *metaPartition) updatePartition(end uint64) (status uint8, err error) {
	status = proto.OpOk
	oldEnd := mp.config.End
	mp.config.End = end
	defer func() {
		if err != nil {
			mp.config.End = oldEnd
			status = proto.OpDiskErr
		}
	}()
	err = mp.StoreMeta()
	return
}

func (mp *metaPartition) deletePartition() (status uint8) {
	mp.Stop()
	os.RemoveAll(mp.config.RootDir)
	return
}

func (mp *metaPartition) confAddNode(req *proto.
	MetaPartitionOfflineRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicatePort int
	)
	if heartbeatPort, replicatePort, err = mp.getRaftPort(); err != nil {
		return
	}

	findAddPeer := false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			findAddPeer = true
			break
		}
	}
	updated = !findAddPeer
	if !updated {
		return
	}
	mp.config.Peers = append(mp.config.Peers, req.AddPeer)
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	mp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicatePort)
	return
}

func (m *metaPartition) getMetaPartitionFromMaster(id uint64) (partition *master.MetaPartition, err error) {
	params := make(map[string]string)
	params["id"] = fmt.Sprintf("%v", id)
	var data interface{}
	for i := 0; i < 3; i++ {
		data, err = masterHelper.Request(http.MethodGet, GetMetaPartition, params, nil)
		if err != nil {
			log.LogErrorf("getMetaPartitionFromMaster error %v", err)
			continue
		}
		break
	}
	partition = new(master.MetaPartition)
	if err = json.Unmarshal(data.([]byte), partition); err != nil {
		err = fmt.Errorf("getMetaPartitionFromMaster jsonUnmarsh failed %v", err)
		log.LogErrorf(err.Error())
		return
	}
	return
}

func (mp *metaPartition) canRemoveSelf() (canRemove bool, err error) {
	var partition *master.MetaPartition
	if partition, err = mp.getMetaPartitionFromMaster(mp.config.PartitionId); err != nil {
		log.LogErrorf("action[canRemoveSelf] err[%v]", err)
		return
	}
	canRemove = false
	var existInPeers bool
	for _, peer := range partition.Peers {
		if mp.config.NodeId == peer.ID {
			existInPeers = true
		}
	}
	if !existInPeers {
		canRemove = true
		return
	}
	if mp.config.NodeId == partition.OfflinePeerID {
		canRemove = true
		return
	}
	return
}

func (mp *metaPartition) confRemoveNode(req *proto.MetaPartitionOfflineRequest,
	index uint64) (updated bool, err error) {
	var canRemoveSelf bool
	if canRemoveSelf, err = mp.canRemoveSelf(); err != nil {
		return
	}
	peerIndex := -1
	for i, peer := range mp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			updated = true
			peerIndex = i
			break
		}
	}
	if !updated {
		return
	}
	if req.RemovePeer.ID == mp.config.NodeId && canRemoveSelf {
		go func(index uint64) {
			for {
				time.Sleep(time.Millisecond)
				if mp.raftPartition != nil {
					if mp.raftPartition.AppliedIndex() < index {
						continue
					}
					mp.raftPartition.Delete()
				}
				mp.Stop()
				os.RemoveAll(mp.config.RootDir)
				log.LogDebugf("[confRemoveNode]: remove self end.")
				return
			}
		}(index)
		updated = false
		log.LogDebugf("[confRemoveNode]: begin remove self.")
		return
	}
	mp.config.Peers = append(mp.config.Peers[:peerIndex], mp.config.Peers[peerIndex+1:]...)
	log.LogDebugf("[confRemoveNode]: remove peer.")
	return
}

func (mp *metaPartition) confUpdateNode(req *proto.MetaPartitionOfflineRequest,
	index uint64) (updated bool, err error) {
	return
}


func (mp *metaPartition) CanRemoveRaftMember(peer proto.Peer) error {
	downReplicas := mp.config.RaftStore.RaftServer().GetDownReplicas(mp.config.PartitionId)
	hasExsit := false
	for _, p := range mp.config.Peers {
		if p.ID == peer.ID {
			hasExsit = true
			break
		}
	}
	if !hasExsit {
		return nil
	}

	hasDownReplicasExcludePeer := make([]uint64, 0)
	for _, nodeID := range downReplicas {
		if nodeID.NodeID == peer.ID {
			continue
		}
		hasDownReplicasExcludePeer = append(hasDownReplicasExcludePeer, nodeID.NodeID)
	}

	sumReplicas := len(mp.config.Peers)
	if sumReplicas%2 == 1 {
		if sumReplicas-len(hasDownReplicasExcludePeer) > (sumReplicas/2 + 1) {
			return nil
		}
	} else {
		if sumReplicas-len(hasDownReplicasExcludePeer) >= (sumReplicas/2 + 1) {
			return nil
		}
	}

	return fmt.Errorf("downReplicas(%v) too much,so donnot offline (%v), the peers are (%v)", downReplicas, peer, mp.config.Peers)
}
