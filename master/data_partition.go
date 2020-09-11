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
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"math"
	"strings"
	"sync"
	"time"
)

type UpdateType int32
const (
	Update_AddMember UpdateType = 0
	Update_RemoveMember UpdateType = 1
)

type BadDiskDataPartition struct {
	dp          *DataPartition
	diskErrAddr string
}

type DataPartition struct {
	PartitionID      uint64
	LastLoadTime     int64
	ReplicaNum       uint8
	Status           int8
	isRecover        bool
	IsManual         bool
	Replicas         []*DataReplica
	PartitionType    string
	PersistenceHosts []string
	offlineMutex   sync.RWMutex
	sync.RWMutex
	total           uint64
	used            uint64
	VolName         string
	modifyTime      int64
	createTime      int64
	FileInCoreMap   map[string]*FileInCore
	MissNodes       map[string]int64
	FileMissReplica map[string]int64
}

func newDataPartition(ID uint64, replicaNum uint8, partitionType, volName string) (partition *DataPartition) {
	partition = new(DataPartition)
	partition.ReplicaNum = replicaNum
	partition.PartitionID = ID
	partition.PartitionType = partitionType
	partition.PersistenceHosts = make([]string, 0)
	partition.Replicas = make([]*DataReplica, 0)
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	partition.MissNodes = make(map[string]int64)
	partition.FileMissReplica = make(map[string]int64)
	partition.Status = proto.ReadOnly
	partition.VolName = volName
	partition.modifyTime = time.Now().Unix()
	partition.createTime = time.Now().Unix()
	return
}

func (partition *DataPartition) AddMember(replica *DataReplica) {
	for _, r := range partition.Replicas {
		if replica.Addr == r.Addr {
			return
		}
	}
	partition.Replicas = append(partition.Replicas, replica)
}

func (partition *DataPartition) generateCreateTask(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpCreateDataPartition, addr, newCreateDataPartitionRequest(partition.PartitionType, partition.VolName, partition.PartitionID))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) GenerateDeleteTask(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpDeleteDataPartition, addr, newDeleteDataPartitionRequest(partition.PartitionID))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) resetTaskID(t *proto.AdminTask) {
	t.ID = fmt.Sprintf("%v_DataPartitionID[%v]", t.ID, partition.PartitionID)
}

func (partition *DataPartition) hasMissOne(offlineAddr string, replicaNum int) (err error) {
	curHostCount := len(partition.PersistenceHosts)
	for _, host := range partition.PersistenceHosts {
		if host == offlineAddr {
			curHostCount = curHostCount - 1
		}
	}
	curReplicaCount := len(partition.Replicas)
	for _, r := range partition.Replicas {
		if r.Addr == offlineAddr {
			curReplicaCount = curReplicaCount - 1
		}
	}
	if curReplicaCount < replicaNum-1 || curHostCount < replicaNum-1 {
		err = DataReplicaHasMissOneError
		log.LogError(fmt.Sprintf("action[%v],partitionID:%v,err:%v",
			"hasMissingOneReplica", partition.PartitionID, err))
	}
	return
}

func (partition *DataPartition) canOffLine(offlineAddr string) (err error) {
	msg := fmt.Sprintf("action[canOffLine],partitionID:%v  RocksDBHost:%v  offLine:%v ",
		partition.PartitionID, partition.PersistenceHosts, offlineAddr)
	liveReplicas := partition.getLiveReplicas(DefaultDataPartitionTimeOutSec)
	otherLiveReplicas := partition.removeOfflineAddr(liveReplicas, offlineAddr)
	if len(otherLiveReplicas) < int(partition.ReplicaNum/2) {
		msg = fmt.Sprintf(msg+" err:%v  liveReplicas:%v ", CannotOffLineErr, len(liveReplicas))
		log.LogError(msg)
		err = fmt.Errorf(msg)
	}
	return
}

func (partition *DataPartition) removeOfflineAddr(liveReplicas []*DataReplica, offlineAddr string) (otherLiveReplicas []*DataReplica) {
	otherLiveReplicas = make([]*DataReplica, 0)
	for i := 0; i < len(liveReplicas); i++ {
		replica := liveReplicas[i]
		if replica.Addr != offlineAddr {
			otherLiveReplicas = append(otherLiveReplicas, replica)
		}
	}

	return
}

func (partition *DataPartition) generatorOffLineLog(offlineAddr string) (msg string) {
	msg = fmt.Sprintf("action[generatorOffLineLog],data partition:%v  offlineaddr:%v  ",
		partition.PartitionID, offlineAddr)
	replicas := partition.GetAvailableDataReplicas()
	for i := 0; i < len(replicas); i++ {
		replica := replicas[i]
		msg += fmt.Sprintf(" addr:%v  dataReplicaStatus:%v  FileCount :%v ", replica.Addr,
			replica.Status, replica.FileCount)
	}
	log.LogWarn(msg)

	return
}

/*获取该副本目前有效的node,即Node在汇报心跳正常，并且该Node不是unavailable*/
func (partition *DataPartition) GetAvailableDataReplicas() (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if replica.CheckLocIsAvailContainsDiskError() == true && partition.isInPersistenceHosts(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

func (partition *DataPartition) offLineInMem(addr string) {
	delIndex := -1
	var replica *DataReplica
	for i := 0; i < len(partition.Replicas); i++ {
		replica = partition.Replicas[i]
		if replica.Addr == addr {
			delIndex = i
			break
		}
	}
	msg := fmt.Sprintf("action[offLineInMem],data partition[%v]  on Node[%v]  OffLine,the node is in replicas:%v", partition.PartitionID, addr, replica != nil)
	log.LogWarnf(msg)
	if delIndex == -1 {
		return
	}
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	partition.DeleteReplicaByIndex(delIndex)
	partition.modifyTime = time.Now().Unix()

	return
}

func (partition *DataPartition) DeleteReplicaByIndex(index int) {
	var replicaAddrs []string
	for _, replica := range partition.Replicas {
		replicaAddrs = append(replicaAddrs, replica.Addr)
	}
	msg := fmt.Sprintf("DeleteReplicaByIndex replica:%v  index:%v  locations :%v ", partition.PartitionID, index, replicaAddrs)
	log.LogInfo(msg)
	replicasAfter := partition.Replicas[index+1:]
	partition.Replicas = partition.Replicas[:index]
	partition.Replicas = append(partition.Replicas, replicasAfter...)
}

func (partition *DataPartition) generateLoadTasks() (tasks []*proto.AdminTask) {

	partition.Lock()
	defer partition.Unlock()
	for _, addr := range partition.PersistenceHosts {
		replica, err := partition.getReplica(addr)
		if err != nil || replica.IsLive(DefaultDataPartitionTimeOutSec) == false {
			continue
		}
		replica.LoadPartitionIsResponse = false
		tasks = append(tasks, partition.generateLoadTask(addr))
	}
	partition.LastLoadTime = time.Now().Unix()
	return
}

func (partition *DataPartition) generateLoadTask(addr string) (task *proto.AdminTask) {
	task = proto.NewAdminTask(proto.OpLoadDataPartition, addr, newLoadDataPartitionMetricRequest(partition.PartitionType, partition.PartitionID))
	partition.resetTaskID(task)
	return
}

func (partition *DataPartition) getReplica(addr string) (replica *DataReplica, err error) {
	for index := 0; index < len(partition.Replicas); index++ {
		replica = partition.Replicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[getReplica],partitionID:%v,locations:%v,err:%v",
		partition.PartitionID, addr, DataReplicaNotFound))
	return nil, errors.Annotatef(DataReplicaNotFound, "%v not found", addr)
}

func (partition *DataPartition) convertToDataPartitionResponse() (dpr *DataPartitionResponse) {
	dpr = new(DataPartitionResponse)
	partition.Lock()
	defer partition.Unlock()
	dpr.PartitionID = partition.PartitionID
	dpr.Status = partition.Status
	dpr.ReplicaNum = partition.ReplicaNum
	dpr.PartitionType = partition.PartitionType
	dpr.Hosts = make([]string, len(partition.PersistenceHosts))
	copy(dpr.Hosts, partition.PersistenceHosts)
	return
}

func (partition *DataPartition) checkLoadResponse(timeOutSec int64) (isResponse bool) {
	partition.RLock()
	defer partition.RUnlock()
	for _, addr := range partition.PersistenceHosts {
		replica, err := partition.getReplica(addr)
		if err != nil {
			return
		}
		timePassed := time.Now().Unix() - partition.LastLoadTime
		if replica.LoadPartitionIsResponse == false && timePassed > LoadDataPartitionWaitTime {
			msg := fmt.Sprintf("action[checkLoadResponse], partitionID:%v on Node:%v no response, spent time %v s",
				partition.PartitionID, addr, timePassed)
			log.LogWarn(msg)
			return
		}
		if replica.IsLive(timeOutSec) == false || replica.LoadPartitionIsResponse == false {
			return
		}
	}
	isResponse = true

	return
}

func (partition *DataPartition) getReplicaByIndex(index uint8) (replica *DataReplica) {
	return partition.Replicas[int(index)]
}

func (partition *DataPartition) ReleaseDataPartition() {
	partition.Lock()
	defer partition.Unlock()
	liveReplicas := partition.getLiveReplicasByPersistenceHosts(DefaultDataPartitionTimeOutSec)
	for _, replica := range liveReplicas {
		replica.LoadPartitionIsResponse = false
	}
	partition.FileInCoreMap = make(map[string]*FileInCore, 0)
	for name, fileMissReplicaTime := range partition.FileMissReplica {
		if time.Now().Unix()-fileMissReplicaTime > 2*LoadDataPartitionPeriod {
			delete(partition.FileMissReplica, name)
		}
	}

}

func (partition *DataPartition) IsInReplicas(host string) (replica *DataReplica, ok bool) {
	for _, replica = range partition.Replicas {
		if replica.Addr == host {
			ok = true
			break
		}
	}
	return
}

func (partition *DataPartition) checkReplicaNum(c *Cluster, volName string) {
	partition.RLock()
	defer partition.RUnlock()
	if int(partition.ReplicaNum) != len(partition.PersistenceHosts) {
		msg := fmt.Sprintf("FIX DataPartition replicaNum,clusterID[%v] volName[%v] partitionID:%v orgReplicaNum:%v",
			c.Name, volName, partition.PartitionID, partition.ReplicaNum)
		Warn(c.Name, msg)
	}
}

func (partition *DataPartition) HostsToString() (hosts string) {
	return strings.Join(partition.PersistenceHosts, UnderlineSeparator)
}

func (partition *DataPartition) setToNormal() {
	partition.Lock()
	defer partition.Unlock()
	partition.isRecover = false
}

func (partition *DataPartition) setStatus(status int8) {
	partition.Lock()
	defer partition.Unlock()
	partition.Status = status
}

func (partition *DataPartition) isInPersistenceHosts(addr string) (ok bool) {
	for _, host := range partition.PersistenceHosts {
		if host == addr {
			ok = true
			break
		}
	}
	return
}

func (partition *DataPartition) getLiveReplicas(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if replica.IsLive(timeOutSec) == true && partition.isInPersistenceHosts(replica.Addr) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

//live replica that host is in the persistenceHosts, and replica location is alive
func (partition *DataPartition) getLiveReplicasByPersistenceHosts(timeOutSec int64) (replicas []*DataReplica) {
	replicas = make([]*DataReplica, 0)
	for _, host := range partition.PersistenceHosts {
		replica, ok := partition.IsInReplicas(host)
		if !ok {
			continue
		}
		if replica.IsLive(timeOutSec) == true {
			replicas = append(replicas, replica)
		}
	}

	return
}

func (partition *DataPartition) checkAndRemoveMissReplica(addr string) {
	if _, ok := partition.MissNodes[addr]; ok {
		delete(partition.MissNodes, addr)
	}
}

func (partition *DataPartition) LoadFile(dataNode *DataNode, resp *proto.LoadDataPartitionResponse) {
	partition.Lock()
	defer partition.Unlock()

	index, err := partition.getReplicaIndex(dataNode.Addr)
	if err != nil {
		msg := fmt.Sprintf("LoadFile partitionID:%v  on Node:%v  don't report :%v ", partition.PartitionID, dataNode.Addr, err)
		log.LogWarn(msg)
		return
	}
	replica := partition.Replicas[index]
	for _, dpf := range resp.PartitionSnapshot {
		if dpf == nil {
			continue
		}
		fc, ok := partition.FileInCoreMap[dpf.Name]
		if !ok {
			fc = NewFileInCore(dpf.Name, dpf.Modified)
			partition.FileInCoreMap[dpf.Name] = fc
		}
		fc.updateFileInCore(partition.PartitionID, dpf, replica, index)
	}
	replica.LoadPartitionIsResponse = true
}

func (partition *DataPartition) getReplicaIndex(addr string) (index int, err error) {
	for index = 0; index < len(partition.Replicas); index++ {
		replica := partition.Replicas[index]
		if replica.Addr == addr {
			return
		}
	}
	log.LogError(fmt.Sprintf("action[getReplicaIndex],partitionID:%v,location:%v,err:%v",
		partition.PartitionID, addr, DataReplicaNotFound))
	return -1, errors.Annotatef(DataReplicaNotFound, "%v not found ", addr)
}

//func (partition *DataPartition) update(action, offlineAddr, newAddr, volName string, c *Cluster) (err error) {
func (partition *DataPartition) update(action, volName, host string,  upType UpdateType, c *Cluster) (err error) {
	orgHosts := make([]string, len(partition.PersistenceHosts))
	copy(orgHosts, partition.PersistenceHosts)

	switch upType {
	case Update_AddMember:
		partition.PersistenceHosts = append(partition.PersistenceHosts, host)

	case Update_RemoveMember:
		newHosts := make([]string, 0)
		for index, addr := range partition.PersistenceHosts {
			if addr == host {
				after := partition.PersistenceHosts[index+1:]
				newHosts = partition.PersistenceHosts[:index]
				newHosts = append(newHosts, after...)
				break
			}
		}
		partition.PersistenceHosts = newHosts
	default:
	}

	if err = c.syncUpdateDataPartition(volName, partition); err != nil {
		partition.PersistenceHosts = orgHosts
		return errors.Annotatef(err, "update partition[%v] failed", partition.PartitionID)
	}
	msg := fmt.Sprintf("action[%v]  partitionID:%v hostAddr::%v oldHosts:%v newHosts:%v",
		action, partition.PartitionID, host, orgHosts, partition.PersistenceHosts)
	log.LogInfo(msg)
	return
}

func (partition *DataPartition) UpdateMetric(vr *proto.PartitionReport, dataNode *DataNode, c *Cluster) {

	if !partition.isInPersistenceHosts(dataNode.Addr) {
		return
	}
	partition.Lock()
	defer partition.Unlock()
	replica, err := partition.getReplica(dataNode.Addr)
	if err != nil {
		replica = NewDataReplica(dataNode)
		partition.AddMember(replica)
	}
	partition.total = vr.Total
	replica.Status = int8(vr.PartitionStatus)
	replica.Total = vr.Total
	replica.Used = vr.Used
	partition.setMaxUsed()
	replica.FileCount = uint32(vr.ExtentCount)
	replica.NeedCompare = vr.NeedCompare
	if vr.DiskPath != replica.DiskPath {
		oldDiskPath := replica.DiskPath
		replica.DiskPath = vr.DiskPath
		err = c.syncUpdateDataPartition(partition.VolName, partition)
		if err != nil {
			replica.DiskPath = oldDiskPath
		}
	}
	replica.SetAlive()
	partition.checkAndRemoveMissReplica(dataNode.Addr)
}

func (partition *DataPartition) setMaxUsed() {
	var maxUsed uint64
	for _, r := range partition.Replicas {
		if r.Used > maxUsed {
			maxUsed = r.Used
		}
	}
	partition.used = maxUsed
}

func (partition *DataPartition) toJson() (body []byte, err error) {
	partition.RLock()
	defer partition.RUnlock()
	return json.Marshal(partition)
}

func (partition *DataPartition) getMaxUsedSize() (used uint64) {
	return partition.used
}

func (partition *DataPartition) isNeedCompareData() (needCompare bool) {
	partition.Lock()
	defer partition.Unlock()
	if partition.isRecover {
		return false
	}
	needCompare = true
	for _, replica := range partition.Replicas {
		if !replica.NeedCompare {
			needCompare = false
			break
		}
	}
	return
}

// the caller must add lock
func (partition *DataPartition) afterCreation(nodeAddr, diskPath string, c *Cluster) (err error) {
	dataNode, err := c.getDataNode(nodeAddr)
	if err != nil {
		return err
	}
	replica := NewDataReplica(dataNode)
	replica.Status = proto.ReadWrite
	replica.DiskPath = diskPath
	replica.ReportTime = time.Now().Unix()
	replica.Total = util.DefaultDataPartitionSize
	partition.AddMember(replica)
	partition.checkAndRemoveMissReplica(replica.Addr)
	return
}

func (partition *DataPartition) getMinus() (minus float64) {
	partition.RLock()
	defer partition.RUnlock()
	used := partition.Replicas[0].Used
	for _, replica := range partition.Replicas {
		if math.Abs(float64(replica.Used)-float64(used)) > minus {
			minus = math.Abs(float64(replica.Used) - float64(used))
		}
	}
	return minus
}

func (partition *DataPartition) containsBadDisk(diskPath string, nodeAddr string) bool {
	partition.RLock()
	defer partition.RUnlock()
	for _, replica := range partition.Replicas {
		if nodeAddr == replica.Addr && diskPath == replica.DiskPath {
			return true
		}
	}
	return false
}

func (partition *DataPartition) isLatestAddr(addr string) (ok bool) {
	if len(partition.PersistenceHosts) <= 1 {
		return
	}
	lastHost := partition.PersistenceHosts[len(partition.PersistenceHosts)-1]
	return lastHost == addr
}

func (partition *DataPartition) isNotLatestAddr(addr string) bool {
	return !partition.isLatestAddr(addr)
}

