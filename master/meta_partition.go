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
	"sync"

	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"math"
	"strings"
	"time"
)

type MetaReplica struct {
	Addr       string
	start      uint64
	end        uint64
	nodeId     uint64
	MaxInodeID uint64
	ReportTime int64
	Status     int8
	IsLeader   bool
	metaNode   *MetaNode
}

type MetaPartition struct {
	PartitionID      uint64
	Start            uint64
	End              uint64
	MaxInodeID       uint64
	IsManual         bool
	Replicas         []*MetaReplica
	ReplicaNum       uint8
	Status           int8
	IsRecover        bool
	VolName          string
	PersistenceHosts []string
	Peers            []proto.Peer
	MissNodes        map[string]int64
	LoadResponse     []*proto.LoadMetaPartitionMetricResponse
	offlineMutex     sync.RWMutex
	sync.RWMutex
}

func NewMetaReplica(start, end uint64, metaNode *MetaNode) (mr *MetaReplica) {
	mr = &MetaReplica{start: start, end: end, nodeId: metaNode.ID, Addr: metaNode.Addr}
	mr.metaNode = metaNode
	mr.ReportTime = time.Now().Unix()
	return
}

func NewMetaPartition(partitionID, start, end uint64, replicaNum uint8, volName string) (mp *MetaPartition) {
	mp = &MetaPartition{PartitionID: partitionID, Start: start, End: end, VolName: volName}
	mp.ReplicaNum = replicaNum
	mp.Replicas = make([]*MetaReplica, 0)
	mp.Status = proto.ReadOnly
	mp.MissNodes = make(map[string]int64, 0)
	mp.Peers = make([]proto.Peer, 0)
	mp.PersistenceHosts = make([]string, 0)
	return
}

func (mp *MetaPartition) toJson() (body []byte, err error) {
	mp.RLock()
	defer mp.RUnlock()
	return json.Marshal(mp)
}

func (mp *MetaPartition) setPeers(peers []proto.Peer) {
	mp.Peers = peers
}

func (mp *MetaPartition) setPersistenceHosts(hosts []string) {
	mp.PersistenceHosts = hosts
}

func (mp *MetaPartition) hostsToString() (hosts string) {
	return strings.Join(mp.PersistenceHosts, UnderlineSeparator)
}

func (mp *MetaPartition) addReplica(mr *MetaReplica) {
	for _, m := range mp.Replicas {
		if m.Addr == mr.Addr {
			return
		}
	}
	mp.Replicas = append(mp.Replicas, mr)
	return
}

func (mp *MetaPartition) removeReplica(mr *MetaReplica) {
	var newReplicas []*MetaReplica
	for _, m := range mp.Replicas {
		if m.Addr == mr.Addr {
			continue
		}
		newReplicas = append(newReplicas, m)
	}
	mp.Replicas = newReplicas
	return
}

func (mp *MetaPartition) removeReplicaByAddr(addr string) {
	var newReplicas []*MetaReplica
	for _, m := range mp.Replicas {
		if m.Addr == addr {
			continue
		}
		newReplicas = append(newReplicas, m)
	}
	mp.Replicas = newReplicas
	return
}

func (mp *MetaPartition) updateAllReplicasEnd() {
	for _, mr := range mp.Replicas {
		mr.end = mp.End
	}
}

//caller add vol lock
func (mp *MetaPartition) updateEnd(c *Cluster, end uint64) (err error) {
	if end <= mp.MaxInodeID {
		err = errors.Errorf("next meta partition start must be larger than %v", mp.MaxInodeID)
		return
	}
	//to prevent overflow
	if end > (defaultMaxMetaPartitionInodeID - defaultMetaPartitionInodeIDStep) {
		msg := fmt.Sprintf("action[updateEnd] clusterID[%v] partitionID[%v] nextStart[%v] "+
			"to prevent overflow ,not update end", c.Name, mp.PartitionID, end)
		log.LogWarn(msg)
		err = fmt.Errorf(msg)
		return
	}
	if _, err = mp.getLeaderMetaReplica(); err != nil {
		log.LogWarnf("action[updateEnd] vol[%v] id[%v] no leader", mp.VolName, mp.PartitionID)
		return
	}

	oldEnd := mp.End
	mp.End = end

	if err = c.syncUpdateMetaPartition(mp.VolName, mp); err != nil {
		mp.End = oldEnd
		log.LogErrorf("action[updateEnd] partitionID[%v] err[%v]", mp.PartitionID, err)
		return
	}
	mp.updateAllReplicasEnd()
	tasks := make([]*proto.AdminTask, 0)
	t := mp.generateUpdateMetaReplicaTask(c.Name, mp.PartitionID, end)
	//if no leader,don't update end
	if t == nil {
		mp.End = oldEnd
		err = NoLeader
		return
	}
	tasks = append(tasks, t)
	c.putMetaNodeTasks(tasks)
	log.LogWarnf("action[updateEnd] partitionID[%v] end[%v] success", mp.PartitionID, mp.End)
	return
}

func (mp *MetaPartition) checkEnd(c *Cluster, maxPartitionID uint64) {

	if mp.PartitionID < maxPartitionID {
		return
	}
	vol, err := c.getVol(mp.VolName)
	if err != nil {
		log.LogWarnf("action[checkEnd] vol[%v] not exist", mp.VolName)
		return
	}
	vol.createMpLock.Lock()
	defer vol.createMpLock.Unlock()
	curMaxPartitionID := vol.getMaxPartitionID()
	if mp.PartitionID != curMaxPartitionID {
		log.LogWarnf("action[checkEnd] partition[%v] not max partition[%v]", mp.PartitionID, curMaxPartitionID)
		return
	}
	mp.Lock()
	defer mp.Unlock()
	if mp.End == defaultMaxMetaPartitionInodeID {
		return
	}
	if mp.End != defaultMaxMetaPartitionInodeID {
		oldEnd := mp.End
		mp.End = defaultMaxMetaPartitionInodeID
		if err := c.syncUpdateMetaPartition(mp.VolName, mp); err != nil {
			mp.End = oldEnd
			log.LogErrorf("action[checkEnd] partitionID[%v] err[%v]", mp.PartitionID, err)
			return
		}
		t := mp.generateUpdateMetaReplicaTask(c.Name, mp.PartitionID, mp.End)
		//if no leader,don't update end
		if t == nil {
			mp.End = oldEnd
			err = NoLeader
			return
		}
		tasks := make([]*proto.AdminTask, 0)
		tasks = append(tasks, t)
		c.putMetaNodeTasks(tasks)
	}
	log.LogDebugf("action[checkEnd] partitionID[%v] end[%v]", mp.PartitionID, mp.End)
}

func (mp *MetaPartition) getMetaReplica(addr string) (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.Addr == addr {
			return
		}
	}
	return nil, metaReplicaNotFound(addr)
}

func (mp *MetaPartition) checkAndRemoveMissMetaReplica(addr string) {
	if _, ok := mp.MissNodes[addr]; ok {
		delete(mp.MissNodes, addr)
	}
}

func (mp *MetaPartition) checkReplicaLeader() {
	mp.Lock()
	defer mp.Unlock()
	for _, mr := range mp.Replicas {
		if !mr.isActive() {
			mr.IsLeader = false
		}
	}
	return
}

func (mp *MetaPartition) checkStatus(writeLog bool, replicaNum int, maxPartitionID uint64) (doSplit bool) {
	mp.Lock()
	defer mp.Unlock()
	liveReplicas := mp.getLiveReplica()
	if mp.IsManual {
		mp.Status = proto.ReadOnly
		goto record
	}
	if len(liveReplicas) <= replicaNum/2 {
		mp.Status = proto.NoLeader
	} else {
		mr, err := mp.getLeaderMetaReplica()
		if err != nil {
			mp.Status = proto.NoLeader
		}
		if mr != nil {
			mp.Status = mr.Status
		}
		for _, mr := range mp.Replicas {
			if mr.metaNode == nil {
				continue
			}
			if !mr.metaNode.isArriveThreshold() {
				continue
			}
			if mp.PartitionID == maxPartitionID {
				doSplit = true
			} else {
				mp.Status = proto.ReadOnly
			}
		}
	}
record:
	if writeLog {
		log.LogInfof("action[checkMPStatus],id:%v,status:%v,replicaNum:%v,liveReplicas:%v persistenceHosts:%v,isManual[%v]",
			mp.PartitionID, mp.Status, mp.ReplicaNum, len(liveReplicas), mp.PersistenceHosts, mp.IsManual)
	}
	return
}

func (mp *MetaPartition) getLeaderMetaReplica() (mr *MetaReplica, err error) {
	for _, mr = range mp.Replicas {
		if mr.IsLeader {
			return
		}
	}
	err = NoLeader
	return
}

func (mp *MetaPartition) checkReplicaNum(c *Cluster, volName string, replicaNum uint8) {
	mp.RLock()
	defer mp.RUnlock()
	if mp.ReplicaNum != replicaNum {
		msg := fmt.Sprintf("FIX MetaPartition replicaNum clusterID[%v] vol[%v] replica num[%v],current num[%v]",
			c.Name, volName, replicaNum, mp.ReplicaNum)
		Warn(c.Name, msg)
	}
}

func (mp *MetaPartition) deleteExcessReplication() (excessAddr string, t *proto.AdminTask, err error) {
	mp.RLock()
	defer mp.RUnlock()
	for _, mr := range mp.Replicas {
		if !contains(mp.PersistenceHosts, mr.Addr) {
			t = mr.generateDeleteReplicaTask(mp.PartitionID)
			err = MetaReplicaExcessError
			break
		}
	}
	return
}

func (mp *MetaPartition) getLackReplication() (lackAddrs []string) {
	mp.RLock()
	defer mp.RUnlock()
	var liveReplicas []string
	for _, mr := range mp.Replicas {
		liveReplicas = append(liveReplicas, mr.Addr)
	}
	for _, host := range mp.PersistenceHosts {
		if !contains(liveReplicas, host) {
			lackAddrs = append(lackAddrs, host)
			break
		}
	}
	return
}

func (mp *MetaPartition) UpdateMetaPartition(mgr *proto.MetaPartitionReport, metaNode *MetaNode) {

	if !contains(mp.PersistenceHosts, metaNode.Addr) {
		return
	}
	mp.Lock()
	defer mp.Unlock()
	mr, err := mp.getMetaReplica(metaNode.Addr)
	if err != nil {
		mr = NewMetaReplica(mp.Start, mp.End, metaNode)
		mp.addReplica(mr)
	}
	mr.updateMetric(mgr)
	mp.setMaxInodeID()
	mp.checkAndRemoveMissMetaReplica(metaNode.Addr)
}

func (mp *MetaPartition) canOffline(nodeAddr string, replicaNum int) (err error) {
	liveReplicas := mp.getLiveReplica()
	if !mp.hasMajorityReplicas(len(liveReplicas), replicaNum) {
		err = NoHaveMajorityReplica
		return
	}
	liveAddrs := mp.getLiveReplicasAddr(liveReplicas)
	if len(liveReplicas) == (replicaNum/2+1) && contains(liveAddrs, nodeAddr) {
		err = fmt.Errorf("live replicas num will be less than majority after offline nodeAddr: %v", nodeAddr)
		return
	}
	return
}

func (mp *MetaPartition) hasMajorityReplicas(liveReplicas int, replicaNum int) bool {
	return liveReplicas >= int(mp.ReplicaNum/2+1)
}

func (mp *MetaPartition) getLiveReplicasAddr(liveReplicas []*MetaReplica) (addrs []string) {
	addrs = make([]string, 0)
	for _, mr := range liveReplicas {
		addrs = append(addrs, mr.Addr)
	}
	return
}
func (mp *MetaPartition) getLiveReplica() (liveReplicas []*MetaReplica) {
	liveReplicas = make([]*MetaReplica, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveReplicas = append(liveReplicas, mr)
		}
	}
	return
}

func (mp *MetaPartition) updateInfoToStore(actionName string, newHosts []string, newPeers []proto.Peer, volName string, c *Cluster) (err error) {
	oldHosts := make([]string, len(mp.PersistenceHosts))
	copy(oldHosts, mp.PersistenceHosts)
	oldPeers := make([]proto.Peer, len(mp.Peers))
	copy(oldPeers, mp.Peers)
	mp.PersistenceHosts = newHosts
	mp.Peers = newPeers
	if len(mp.PersistenceHosts) < 1 || len(mp.Peers) < 1 {
		mp.PersistenceHosts = oldHosts
		mp.Peers = oldPeers
		return fmt.Errorf("action[%v] failed,partition[%v],hosts[%v],peers[%v]", actionName, mp.PartitionID, mp.PersistenceHosts, mp.Peers)

	}
	if err = c.syncUpdateMetaPartition(volName, mp); err != nil {
		mp.PersistenceHosts = oldHosts
		mp.Peers = oldPeers
		log.LogWarnf("action[%v] failed,partitionID:%v  old hosts:%v new hosts:%v oldPeers:%v  newPeers:%v",
			actionName, mp.PartitionID, mp.PersistenceHosts, newHosts, mp.Peers, newPeers)
		return
	}
	log.LogWarnf("action[%v] success,partitionID:%v  old hosts:%v  new hosts:%v oldPeers:%v  newPeers:%v ",
		actionName, mp.PartitionID, oldHosts, mp.PersistenceHosts, oldPeers, mp.Peers)
	return
}

func (mp *MetaPartition) getLiveAddrs() (liveAddrs []string) {
	liveAddrs = make([]string, 0)
	for _, mr := range mp.Replicas {
		if mr.isActive() {
			liveAddrs = append(liveAddrs, mr.Addr)
		}
	}
	return liveAddrs
}

func (mp *MetaPartition) missedReplica(addr string) bool {
	return !contains(mp.getLiveAddrs(), addr)
}

func (mp *MetaPartition) needWarnMissReplica(addr string, warnInterval int64) (isWarn bool) {
	lastWarnTime, ok := mp.MissNodes[addr]
	if !ok {
		isWarn = true
		mp.MissNodes[addr] = time.Now().Unix()
	} else if (time.Now().Unix() - lastWarnTime) > warnInterval {
		isWarn = true
		mp.MissNodes[addr] = time.Now().Unix()
	}
	return false
}

func (mp *MetaPartition) checkReplicaMiss(clusterID, leaderAddr string, partitionMissSec, warnInterval int64) {
	mp.Lock()
	defer mp.Unlock()
	//has report
	for _, replica := range mp.Replicas {
		if contains(mp.PersistenceHosts, replica.Addr) && replica.isMissed() && mp.needWarnMissReplica(replica.Addr, warnInterval) {
			metaNode := replica.metaNode
			var (
				lastReportTime time.Time
			)
			isActive := true
			if metaNode != nil {
				lastReportTime = metaNode.ReportTime
				isActive = metaNode.IsActive
			}
			msg := fmt.Sprintf("action[checkReplicaMiss], clusterID[%v] volName[%v] partition:%v  on Node:%v  "+
				"miss time > :%v  vlocLastRepostTime:%v   dnodeLastReportTime:%v  nodeisActive:%v",
				clusterID, mp.VolName, mp.PartitionID, replica.Addr, partitionMissSec, replica.ReportTime, lastReportTime, isActive)
			Warn(clusterID, msg)
			msg = fmt.Sprintf("http://%v/metaPartition/offline?name=%v&id=%v&addr=%v",
				leaderAddr, mp.VolName, mp.PartitionID, replica.Addr)
			log.LogRead(msg)
		}
	}
	// never report
	for _, addr := range mp.PersistenceHosts {
		if mp.missedReplica(addr) && mp.needWarnMissReplica(addr, warnInterval) {
			msg := fmt.Sprintf("action[checkReplicaMiss],clusterID[%v] volName[%v] partition:%v  on Node:%v  "+
				"miss time  > %v ",
				clusterID, mp.VolName, mp.PartitionID, addr, DefaultMetaPartitionTimeOutSec)
			Warn(clusterID, msg)
			msg = fmt.Sprintf("http://%v/metaPartition/offline?name=%v&id=%v&addr=%v",
				leaderAddr, mp.VolName, mp.PartitionID, addr)
			log.LogRead(msg)
		}
	}
}

func (mp *MetaPartition) GenerateReplicaTask(clusterID, volName string) (tasks []*proto.AdminTask) {
	var msg string
	tasks = make([]*proto.AdminTask, 0)
	if excessAddr, task, excessErr := mp.deleteExcessReplication(); excessErr != nil {
		msg = fmt.Sprintf("action[%v],clusterID[%v] metaPartition:%v  excess replication"+
			" on :%v  err:%v  persistenceHosts:%v",
			DeleteExcessReplicationErr, clusterID, mp.PartitionID, excessAddr, excessErr.Error(), mp.PersistenceHosts)
		log.LogWarn(msg)
		tasks = append(tasks, task)
	}
	if lackAddrs := mp.getLackReplication(); lackAddrs != nil {
		msg = fmt.Sprintf("action[getLackReplication],clusterID[%v] metaPartition:%v  lack replication"+
			" on :%v PersistenceHosts:%v",
			clusterID, mp.PartitionID, lackAddrs, mp.PersistenceHosts)
		Warn(clusterID, msg)
	}

	return
}

func (mp *MetaPartition) generateCreateMetaPartitionTasks(specifyAddrs []string, peers []proto.Peer, volName string) (tasks []*proto.AdminTask) {
	tasks = make([]*proto.AdminTask, 0)
	hosts := make([]string, 0)
	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     peers,
		VolName:     volName,
	}
	if specifyAddrs == nil {
		hosts = mp.PersistenceHosts
	} else {
		hosts = specifyAddrs
	}

	for _, addr := range hosts {
		t := proto.NewAdminTask(proto.OpCreateMetaPartition, addr, req)
		resetMetaPartitionTaskID(t, mp.PartitionID)
		tasks = append(tasks, t)
	}
	return
}

func (mp *MetaPartition) generateAddLackMetaReplicaTask(addrs []string, volName string) (tasks []*proto.AdminTask) {
	return mp.generateCreateMetaPartitionTasks(addrs, mp.Peers, volName)
}

func (mp *MetaPartition) generateOfflineTask(volName string, removePeer proto.Peer, addPeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getLeaderMetaReplica()
	if err != nil {
		return nil, errors.Trace(err)
	}
	req := &proto.MetaPartitionOfflineRequest{PartitionID: mp.PartitionID, VolName: volName, RemovePeer: removePeer, AddPeer: addPeer}
	t = proto.NewAdminTask(proto.OpOfflineMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func resetMetaPartitionTaskID(t *proto.AdminTask, partitionID uint64) {
	t.ID = fmt.Sprintf("%v_pid[%v]", t.ID, partitionID)
	t.PartitionID = partitionID
}

func (mp *MetaPartition) generateUpdateMetaReplicaTask(clusterID string, partitionID uint64, end uint64) (t *proto.AdminTask) {
	mr, err := mp.getLeaderMetaReplica()
	if err != nil {
		msg := fmt.Sprintf("action[generateUpdateMetaReplicaTask] clusterID[%v] meta partition %v no leader",
			clusterID, mp.PartitionID)
		Warn(clusterID, msg)
		return
	}
	req := &proto.UpdateMetaPartitionRequest{PartitionID: partitionID, End: end, VolName: mp.VolName}
	t = proto.NewAdminTask(proto.OpUpdateMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mr *MetaReplica) generateDeleteReplicaTask(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaPartitionRequest{PartitionID: partitionID}
	t = proto.NewAdminTask(proto.OpDeleteMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}

func (mr *MetaReplica) createTaskToLoadMetaPartition(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.LoadMetaPartitionMetricResponse{PartitionID: partitionID}
	t = proto.NewAdminTask(proto.OpLoadMetaPartition, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}

func (mr *MetaReplica) isMissed() (miss bool) {
	return time.Now().Unix()-mr.ReportTime > DefaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) isActive() (active bool) {
	return mr.metaNode.IsActive && mr.Status != proto.NoLeader &&
		time.Now().Unix()-mr.ReportTime < DefaultMetaPartitionTimeOutSec
}

func (mr *MetaReplica) setLastReportTime() {
	mr.ReportTime = time.Now().Unix()
}

func (mr *MetaReplica) updateMetric(mgr *proto.MetaPartitionReport) {
	mr.Status = (int8)(mgr.Status)
	mr.IsLeader = mgr.IsLeader
	mr.MaxInodeID = mgr.MaxInodeID
	mr.setLastReportTime()
}

// the caller must add lock
func (mp *MetaPartition) createPartitionSuccessTriggerOperator(nodeAddr string, c *Cluster) (err error) {
	metaNode, err := c.getMetaNode(nodeAddr)
	if err != nil {
		return err
	}
	mr := NewMetaReplica(mp.Start, mp.End, metaNode)
	mr.Status = proto.ReadWrite
	mr.ReportTime = time.Now().Unix()
	mp.addReplica(mr)
	mp.checkAndRemoveMissMetaReplica(mr.Addr)
	return
}

func (mp *MetaPartition) getMinusOfMaxInodeID() (minus float64) {
	mp.RLock()
	defer mp.RUnlock()
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.MaxInodeID)
			continue
		}
		diff := math.Abs(float64(replica.MaxInodeID) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mp *MetaPartition) setMaxInodeID() {
	var maxUsed uint64
	for _, r := range mp.Replicas {
		if r.MaxInodeID > maxUsed {
			maxUsed = r.MaxInodeID
		}
	}
	mp.MaxInodeID = maxUsed
}

func (mp *MetaPartition) isLatestAddr(addr string) (ok bool) {
	if len(mp.PersistenceHosts) <= 1 {
		return
	}
	lastHost := mp.PersistenceHosts[0]
	return lastHost == addr
}

func (mp *MetaPartition) isNotLatestAddr(addr string) bool {
	return !mp.isLatestAddr(addr)
}

func (mp *MetaPartition) addOrReplaceLoadResponse(response *proto.LoadMetaPartitionMetricResponse) {
	mp.Lock()
	defer mp.Unlock()
	loadResponse := make([]*proto.LoadMetaPartitionMetricResponse, 0)
	for _, lr := range mp.LoadResponse {
		if lr.Addr == response.Addr {
			continue
		}
		loadResponse = append(loadResponse, lr)
	}
	loadResponse = append(loadResponse, response)
	mp.LoadResponse = loadResponse
}

// Check if there is a replica missing or not.
func (mp *MetaPartition) hasMissingOneReplica(offlineAddr string, replicaNum int) (err error) {
	curHostCount := len(mp.PersistenceHosts)
	for _, host := range mp.PersistenceHosts {
		if host == offlineAddr {
			curHostCount = curHostCount - 1
		}
	}
	curReplicaCount := len(mp.Replicas)
	for _, r := range mp.Replicas {
		if r.Addr == offlineAddr {
			curReplicaCount = curReplicaCount - 1
		}
	}
	if curHostCount < replicaNum-1 || curReplicaCount < replicaNum-1 {
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissingOneReplica", mp.PartitionID, MetaReplicaHasMissOneError))
		err = MetaReplicaHasMissOneError
	}
	return
}

func (mp *MetaPartition) createTaskToRemoveRaftMember(removePeer proto.Peer) (t *proto.AdminTask, err error) {
	mr, err := mp.getLeaderMetaReplica()
	if err != nil {
		return nil, err
	}
	req := &proto.RemoveMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, RemovePeer: removePeer}
	t = proto.NewAdminTask(proto.OpRemoveMetaPartitionRaftMember, mr.Addr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) tryToChangeLeader(c *Cluster, metaNode *MetaNode) (err error) {
	task, err := mp.createTaskToTryToChangeLeader(metaNode.Addr)
	if err != nil {
		return
	}
	if _, err = metaNode.Sender.syncSendAdminTask(task); err != nil {
		return
	}
	return
}

func (mp *MetaPartition) createTaskToTryToChangeLeader(addr string) (task *proto.AdminTask, err error) {
	task = proto.NewAdminTask(proto.OpMetaPartitionTryToLeader, addr, nil)
	resetMetaPartitionTaskID(task, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToCreateReplica(host string) (t *proto.AdminTask, err error) {
	req := &proto.CreateMetaPartitionRequest{
		Start:       mp.Start,
		End:         mp.End,
		PartitionID: mp.PartitionID,
		Members:     mp.Peers,
		VolName:     mp.VolName,
	}
	t = proto.NewAdminTask(proto.OpCreateMetaPartition, host, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}

func (mp *MetaPartition) createTaskToAddRaftMember(addPeer proto.Peer, leaderAddr string) (t *proto.AdminTask, err error) {
	req := &proto.AddMetaPartitionRaftMemberRequest{PartitionId: mp.PartitionID, AddPeer: addPeer}
	t = proto.NewAdminTask(proto.OpAddMetaPartitionRaftMember, leaderAddr, req)
	resetMetaPartitionTaskID(t, mp.PartitionID)
	return
}
