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
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
	"time"
)

type Cluster struct {
	Name                string
	DisableAutoAlloc    bool
	vols                map[string]*Vol
	dataNodes           sync.Map
	metaNodes           sync.Map
	createDpLock        sync.Mutex
	createVolLock       sync.Mutex
	volsLock            sync.RWMutex
	leaderInfo          *LeaderInfo
	cfg                 *ClusterConfig
	fsm                 *MetadataFsm
	partition           raftstore.Partition
	retainLogs          uint64
	idAlloc             *IDAllocator
	t                   *Topology
	compactStatus       bool
	dataNodeSpace       *DataNodeSpaceStat
	metaNodeSpace       *MetaNodeSpaceStat
	volSpaceStat        sync.Map
	BadDataPartitionIds *sync.Map
}

func newCluster(name string, leaderInfo *LeaderInfo, fsm *MetadataFsm, partition raftstore.Partition, cfg *ClusterConfig) (c *Cluster) {
	c = new(Cluster)
	c.Name = name
	c.leaderInfo = leaderInfo
	c.vols = make(map[string]*Vol, 0)
	c.cfg = cfg
	c.fsm = fsm
	c.partition = partition
	c.idAlloc = newIDAllocator(c.fsm.store, c.partition)
	c.t = NewTopology()
	c.dataNodeSpace = new(DataNodeSpaceStat)
	c.metaNodeSpace = new(MetaNodeSpaceStat)
	c.BadDataPartitionIds = new(sync.Map)
	c.startCheckDataPartitions()
	c.startCheckBackendLoadDataPartitions()
	c.startCheckReleaseDataPartitions()
	c.startCheckHeartbeat()
	c.startCheckMetaPartitions()
	c.startCheckAvailSpace()
	c.startCheckCreateDataPartitions()
	c.startCheckVolStatus()
	c.startCheckBadDiskRecovery()
	return
}

func (c *Cluster) getMasterAddr() (addr string) {
	return c.leaderInfo.addr
}

func (c *Cluster) startCheckAvailSpace() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkAvailSpace()
			}
			time.Sleep(time.Second * DefaultCheckHeartbeatIntervalSeconds)
		}
	}()

}

func (c *Cluster) startCheckCreateDataPartitions() {
	go func() {
		//check vols after switching leader two minutes
		time.Sleep(2 * time.Minute)
		for {
			if c.partition.IsLeader() {
				c.checkCreateDataPartitions()
			}
			time.Sleep(2 * time.Minute)
		}
	}()
}

func (c *Cluster) startCheckVolStatus() {
	go func() {
		//check vols after switching leader two minutes
		for {
			if c.partition.IsLeader() {
				vols := c.copyVols()
				for _, vol := range vols {
					vol.checkStatus(c)
				}
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) startCheckDataPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkDataPartitions()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) checkCreateDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkCreateDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"checkCreateDataPartitions occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		vol.checkNeedAutoCreateDataPartitions(c)
	}
}

func (c *Cluster) checkDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"checkDataPartitions occurred panic")
		}
	}()
	vols := c.getAllNormalVols()
	for _, vol := range vols {
		readWrites := vol.checkDataPartitions(c)
		vol.dataPartitions.updateDataPartitionResponseCache(true, 0)
		vol.dataPartitions.setReadWriteDataPartitions(readWrites, c.Name)
		msg := fmt.Sprintf("action[checkDataPartitions],vol[%v] can readWrite dataPartitions:%v  ", vol.Name, vol.dataPartitions.readWriteDataPartitions)
		log.LogInfo(msg)
	}
}

func (c *Cluster) startCheckBackendLoadDataPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.backendLoadDataPartitions()
			}
			time.Sleep(5 * time.Second)
		}
	}()
}

func (c *Cluster) backendLoadDataPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("backendLoadDataPartitions occurred panic,err[%v]", r)
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"backendLoadDataPartitions occurred panic")
		}
	}()
	vols := c.getAllNormalVols()
	for _, vol := range vols {
		vol.LoadDataPartition(c)
	}
}

func (c *Cluster) startCheckReleaseDataPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.releaseDataPartitionAfterLoad()
			}
			time.Sleep(time.Second * DefaultReleaseDataPartitionInternalSeconds)
		}
	}()
}

func (c *Cluster) releaseDataPartitionAfterLoad() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("releaseDataPartitionAfterLoad occurred panic,err[%v]", r)
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"releaseDataPartitionAfterLoad occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		vol.ReleaseDataPartitions(c.cfg.everyReleaseDataPartitionCount, c.cfg.releaseDataPartitionAfterLoadSeconds)
	}
}

func (c *Cluster) startCheckHeartbeat() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkLeaderAddr()
				c.checkDataNodeHeartbeat()
			}
			time.Sleep(time.Second * DefaultCheckHeartbeatIntervalSeconds)
		}
	}()

	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkMetaNodeHeartbeat()
			}
			time.Sleep(time.Second * DefaultCheckHeartbeatIntervalSeconds)
		}
	}()
}

func (c *Cluster) checkLeaderAddr() {
	leaderId, _ := c.partition.LeaderTerm()
	c.leaderInfo.addr = AddrDatabase[leaderId]
}

func (c *Cluster) checkDataNodeHeartbeat() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDataNodeHeartbeat occurred panic,err[%v]", r)
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"checkDataNodeHeartbeat occurred panic")
		}
	}()
	tasks := make([]*proto.AdminTask, 0)
	c.dataNodes.Range(func(addr, dataNode interface{}) bool {
		node := dataNode.(*DataNode)
		node.checkHeartBeat()
		task := node.generateHeartbeatTask(c.getMasterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.putDataNodeTasks(tasks)
}

func (c *Cluster) checkMetaNodeHeartbeat() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMetaNodeHeartbeat occurred panic,err[%v]", r)
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"checkMetaNodeHeartbeat occurred panic")
		}
	}()
	tasks := make([]*proto.AdminTask, 0)
	c.metaNodes.Range(func(addr, metaNode interface{}) bool {
		node := metaNode.(*MetaNode)
		node.checkHeartbeat()
		task := node.generateHeartbeatTask(c.getMasterAddr())
		tasks = append(tasks, task)
		return true
	})
	c.putMetaNodeTasks(tasks)
}

func (c *Cluster) startCheckMetaPartitions() {
	go func() {
		for {
			if c.partition.IsLeader() {
				c.checkMetaPartitions()
			}
			time.Sleep(time.Second * time.Duration(c.cfg.CheckDataPartitionIntervalSeconds))
		}
	}()
}

func (c *Cluster) checkMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMetaPartitions occurred panic,err[%v]", r)
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"checkMetaPartitions occurred panic")
		}
	}()
	vols := c.getAllNormalVols()
	for _, vol := range vols {
		vol.checkMetaPartitions(c)
	}
}

func (c *Cluster) addMetaNode(nodeAddr string) (id uint64, err error) {
	var (
		metaNode *MetaNode
	)
	if value, ok := c.metaNodes.Load(nodeAddr); ok {
		metaNode = value.(*MetaNode)
		return metaNode.ID, nil
	}
	metaNode = NewMetaNode(nodeAddr, c.Name)

	if id, err = c.idAlloc.allocateMetaNodeID(); err != nil {
		goto errDeal
	}
	metaNode.ID = id
	if err = c.syncAddMetaNode(metaNode); err != nil {
		goto errDeal
	}
	c.metaNodes.Store(nodeAddr, metaNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],clusterID[%v] metaNodeAddr:%v err:%v ",
		c.Name, nodeAddr, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) addDataNode(nodeAddr string) (err error) {
	var dataNode *DataNode
	if _, ok := c.dataNodes.Load(nodeAddr); ok {
		return
	}

	dataNode = NewDataNode(nodeAddr, c.Name)
	if err = c.syncAddDataNode(dataNode); err != nil {
		goto errDeal
	}
	c.dataNodes.Store(nodeAddr, dataNode)
	return
errDeal:
	err = fmt.Errorf("action[addMetaNode],clusterID[%v] dataNodeAddr:%v err:%v ", c.Name, nodeAddr, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return err
}

func (c *Cluster) deleteDataPartition(partitionID uint64) (err error) {
	var (
		vol *Vol
		dp  *DataPartition
	)
	if dp, err = c.getDataPartitionByID(partitionID); err != nil {
		goto errDeal
	}
	if vol, err = c.getVol(dp.VolName); err != nil {
		goto errDeal
	}
	if err = c.syncDeleteDataPartition(dp.VolName, dp); err != nil {
		goto errDeal
	}
	vol.deleteDataPartitionsFromCache(dp)
	log.LogWarnf("action[deleteDataPartition],clusterID[%v] vol[%v] partitionId[%v] delete success ", c.Name, dp.VolName, partitionID)
	return
errDeal:
	err = fmt.Errorf("action[deleteDataPartition],clusterID[%v] paritionId:%v err:%v ", c.Name, partitionID, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return err
}

func (c *Cluster) getDataPartitionByID(partitionID uint64) (dp *DataPartition, err error) {
	vols := c.copyVols()
	for _, vol := range vols {
		if dp, err = vol.getDataPartitionByID(partitionID); err == nil {
			return
		}
	}
	err = dataPartitionNotFound(partitionID)
	return
}

func (c *Cluster) getDataPartitionByIDAndVol(partitionID uint64, volName string) (dp *DataPartition, err error) {
	vol, err := c.getVol(volName)
	if err != nil {
		return nil, VolNotFound
	}
	return vol.getDataPartitionByID(partitionID)
}

func (c *Cluster) getMetaPartitionByID(id uint64) (mp *MetaPartition, err error) {
	vols := c.copyVols()
	for _, vol := range vols {
		if mp, err = vol.getMetaPartition(id); err == nil {
			return
		}
	}
	err = metaPartitionNotFound(id)
	return
}

func (c *Cluster) updateMetaPartition(mp *MetaPartition, isManual bool) (err error) {
	oldIsManual := mp.IsManual
	mp.IsManual = isManual
	if err = c.syncUpdateMetaPartition(mp.volName, mp); err != nil {
		mp.IsManual = oldIsManual
		return
	}
	if mp.IsManual {
		mp.Status = proto.ReadOnly
	}
	return
}

func (c *Cluster) updateDataPartition(dp *DataPartition, isManual bool) (err error) {
	oldIsManual := dp.IsManual
	dp.IsManual = isManual
	if err = c.syncUpdateDataPartition(dp.VolName, dp); err != nil {
		dp.IsManual = oldIsManual
		return
	}
	if dp.IsManual {
		dp.Status = proto.ReadOnly
	}
	return
}

func (c *Cluster) putVol(vol *Vol) {
	c.volsLock.Lock()
	defer c.volsLock.Unlock()
	if _, ok := c.vols[vol.Name]; !ok {
		c.vols[vol.Name] = vol
	}
}

func (c *Cluster) getVol(volName string) (vol *Vol, err error) {
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	vol, ok := c.vols[volName]
	if !ok {
		err = errors.Annotatef(VolNotFound, "%v not found", volName)
	}
	return
}

func (c *Cluster) deleteVol(name string) {
	c.volsLock.Lock()
	defer c.volsLock.Unlock()
	delete(c.vols, name)
	return
}

func (c *Cluster) markDeleteVol(name, authKey string) (err error) {
	var (
		vol           *Vol
		serverAuthKey string
	)
	if vol, err = c.getVol(name); err != nil {
		return
	}
	if vol.Owner != "" {
		serverAuthKey = vol.Owner
	} else {
		serverAuthKey = vol.Name
	}
	if !matchKey(serverAuthKey, authKey) {
		return VolAuthKeyNotMatch
	}
	vol.Status = VolMarkDelete
	if err = c.syncUpdateVol(vol); err != nil {
		vol.Status = VolNormal
		return
	}
	log.LogWarnf("action[markDeleteVol] delete vol[%v] success", name)
	return
}

func (c *Cluster) createDataPartition(volName, partitionType string) (dp *DataPartition, err error) {
	var (
		vol         *Vol
		partitionID uint64
		targetHosts []string
		wg          sync.WaitGroup
	)
	if vol, err = c.getVol(volName); err != nil {
		return
	}
	vol.createDpLock.Lock()
	defer vol.createDpLock.Unlock()
	errChannel := make(chan error, vol.dpReplicaNum)
	if targetHosts, err = c.ChooseTargetDataHosts(int(vol.dpReplicaNum)); err != nil {
		goto errDeal
	}
	if partitionID, err = c.idAlloc.allocateDataPartitionID(); err != nil {
		goto errDeal
	}
	dp = newDataPartition(partitionID, vol.dpReplicaNum, partitionType, volName)
	dp.PersistenceHosts = targetHosts
	for _, host := range targetHosts {
		wg.Add(1)
		go func(host string) {
			defer func() {
				wg.Done()
			}()
			var diskPath string
			if diskPath, err = c.syncCreateDataPartitionToDataNode(host, dp); err != nil {
				errChannel <- err
				return
			}
			dp.Lock()
			defer dp.Unlock()
			if err = dp.afterCreation(host, diskPath, c); err != nil {
				errChannel <- err
			}
		}(host)
	}
	wg.Wait()
	select {
	case err = <-errChannel:
		goto errDeal
		for _, host := range targetHosts {
			wg.Add(1)
			go func(host string) {
				defer func() {
					wg.Done()
				}()
				task := dp.GenerateDeleteTask(host)
				tasks := make([]*proto.AdminTask, 0)
				tasks = append(tasks, task)
				c.putDataNodeTasks(tasks)
			}(host)
		}
	default:
		dp.Status = proto.ReadWrite
	}
	if err = c.syncAddDataPartition(volName, dp); err != nil {
		goto errDeal
	}
	vol.dataPartitions.putDataPartition(dp)
	log.LogInfof("action[syncCreatePartition] success,volName[%v],partition[%v]", volName, partitionID)
	return
errDeal:
	err = fmt.Errorf("action[syncCreatePartition],clusterID[%v] vol[%v] partitonID[%v] Err:%v ", c.Name, volName, partitionID, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) syncCreateDataPartitionToDataNode(host string, dp *DataPartition) (diskPath string, err error) {
	task := dp.generateCreateTask(host)
	dataNode, err := c.getDataNode(host)
	if err != nil {
		return
	}
	replicaDiskPath, err := dataNode.Sender.syncSendAdminTask(task)
	if err != nil {
		return
	}
	return string(replicaDiskPath), nil
}

func (c *Cluster) ChooseTargetDataHosts(replicaNum int) (hosts []string, err error) {
	var (
		masterAddr []string
		addrs      []string
		racks      []*Rack
		rack       *Rack
	)
	hosts = make([]string, 0)
	if c.t.isSingleRack() {
		var newHosts []string
		if rack, err = c.t.getRack(c.t.racks[0]); err != nil {
			return nil, errors.Trace(err)
		}
		if newHosts, err = rack.getAvailDataNodeHosts(hosts, replicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = newHosts
		return
	}

	if racks, err = c.t.allocRacks(replicaNum, nil); err != nil {
		return nil, errors.Trace(err)
	}

	if len(racks) == 1 {
		var newHosts []string
		if newHosts, err = racks[0].getAvailDataNodeHosts(hosts, replicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = newHosts
		return
	}

	if len(racks) == 2 {
		masterRack := racks[0]
		slaveRack := racks[1]
		masterReplicaNum := replicaNum/2 + 1
		slaveReplicaNum := replicaNum - masterReplicaNum
		if masterAddr, err = masterRack.getAvailDataNodeHosts(hosts, masterReplicaNum); err != nil {
			return nil, errors.Trace(err)
		}
		hosts = append(hosts, masterAddr...)
		if slaveReplicaNum > 0 {
			if addrs, err = slaveRack.getAvailDataNodeHosts(hosts, slaveReplicaNum); err != nil {
				return nil, errors.Trace(err)
			}
		}
		hosts = append(hosts, addrs...)
	} else if len(racks) == replicaNum {
		for index := 0; index < replicaNum; index++ {
			rack := racks[index]
			if addrs, err = rack.getAvailDataNodeHosts(hosts, 1); err != nil {
				return nil, errors.Trace(err)
			}
			hosts = append(hosts, addrs...)
		}
	}
	if len(hosts) != replicaNum {
		return nil, NoAnyDataNodeForCreateDataPartition
	}
	return
}

func (c *Cluster) getAllDataPartitionIDByDatanode(addr string) (partitionIDs []uint64) {
	partitionIDs = make([]uint64, 0)
	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.dataPartitions {
			for _, host := range dp.PersistenceHosts {
				if host == addr {
					partitionIDs = append(partitionIDs, dp.PartitionID)
					break
				}
			}
		}
	}

	return
}

func (c *Cluster) getAllmetaPartitionIDByMetaNode(addr string) (partitionIDs []uint64) {
	partitionIDs = make([]uint64, 0)
	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			for _, host := range mp.PersistenceHosts {
				if host == addr {
					partitionIDs = append(partitionIDs, mp.PartitionID)
					break
				}
			}
		}
	}

	return
}

func (c *Cluster) getDataNode(addr string) (dataNode *DataNode, err error) {
	value, ok := c.dataNodes.Load(addr)
	if !ok {
		err = errors.Annotatef(DataNodeNotFound, "%v not found", addr)
		return
	}
	dataNode = value.(*DataNode)
	return
}

func (c *Cluster) getMetaNode(addr string) (metaNode *MetaNode, err error) {
	value, ok := c.metaNodes.Load(addr)
	if !ok {
		err = errors.Annotatef(MetaNodeNotFound, "%v not found", addr)
		return
	}
	metaNode = value.(*MetaNode)
	return
}

func (c *Cluster) dataNodeOffLine(dataNode *DataNode, destAddr string) (err error) {
	msg := fmt.Sprintf("action[dataNodeOffLine], Node[%v] OffLine", dataNode.Addr)
	log.LogWarn(msg)

	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.dataPartitions {
			if err = c.dataPartitionOffline(dataNode.Addr, destAddr, vol.Name, dp, DataNodeOfflineInfo); err != nil {
				return
			}
		}
	}
	if err = c.syncDeleteDataNode(dataNode); err != nil {
		msg = fmt.Sprintf("action[dataNodeOffLine],clusterID[%v] Node[%v] OffLine failed,err[%v]",
			c.Name, dataNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.delDataNodeFromCache(dataNode)
	msg = fmt.Sprintf("action[dataNodeOffLine],clusterID[%v] Node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
	return
}

func (c *Cluster) delDataNodeFromCache(dataNode *DataNode) {
	c.dataNodes.Delete(dataNode.Addr)
	c.t.removeDataNode(dataNode)
	go dataNode.clean()
}

func (c *Cluster) dataPartitionOffline(offlineAddr, destAddr, volName string, dp *DataPartition, errMsg string) (err error) {
	var (
		newHosts []string
		newAddr  string
		msg      string
		diskPath string
		tasks    []*proto.AdminTask
		task     *proto.AdminTask
		dataNode *DataNode
		rack     *Rack
		vol      *Vol
		replica  *DataReplica
	)
	dp.Lock()
	defer dp.Unlock()
	if ok := dp.isInPersistenceHosts(offlineAddr); !ok {
		return
	}

	if vol, err = c.getVol(volName); err != nil {
		goto errDeal
	}

	if err = dp.hasMissOne(int(vol.dpReplicaNum)); err != nil {
		goto errDeal
	}
	if err = dp.canOffLine(offlineAddr); err != nil {
		goto errDeal
	}
	dp.generatorOffLineLog(offlineAddr)

	if dataNode, err = c.getDataNode(offlineAddr); err != nil {
		goto errDeal
	}

	if dataNode.RackName == "" {
		return
	}
	if rack, err = c.t.getRack(dataNode.RackName); err != nil {
		goto errDeal
	}
	if destAddr != "" {
		if contains(dp.PersistenceHosts, destAddr) {
			err = errors.Errorf("destinationAddr[%v] must be a new data node addr,oldHosts[%v]", destAddr, dp.PersistenceHosts)
			goto errDeal
		}
		_, err = c.getDataNode(destAddr)
		if err != nil {
			goto errDeal
		}
		newHosts = append(newHosts, destAddr)
	} else if newHosts, err = rack.getAvailDataNodeHosts(dp.PersistenceHosts, 1); err != nil {
		goto errDeal
	}
	newAddr = newHosts[0]
	if diskPath, err = c.syncCreateDataPartitionToDataNode(newAddr, dp); err != nil {
		goto errDeal
	}
	if err = dp.afterCreation(newAddr, diskPath, c); err != nil {
		goto errDeal
	}
	if err = dp.updateForOffline(offlineAddr, newAddr, volName, c); err != nil {
		goto errDeal
	}
	if replica, err = dp.getReplica(offlineAddr); err != nil {
		goto errDeal
	}
	dp.isRecover = true
	c.putBadDataPartitionIDs(replica, dataNode, dp.PartitionID)
	dp.offLineInMem(offlineAddr)
	dp.checkAndRemoveMissReplica(offlineAddr)
	task = dp.GenerateDeleteTask(offlineAddr)
	tasks = make([]*proto.AdminTask, 0)
	tasks = append(tasks, task)
	c.putDataNodeTasks(tasks)
errDeal:
	msg = fmt.Sprintf(errMsg + " clusterID[%v] partitionID:%v  on Node:%v  "+
		"Then Fix It on newHost:%v   Err:%v , PersistenceHosts:%v  ",
		c.Name, dp.PartitionID, offlineAddr, newAddr, err, dp.PersistenceHosts)
	if err != nil {
		Warn(c.Name, msg)
	}
	log.LogWarn(msg)
	return
}

func (c *Cluster) putBadDataPartitionIDs(replica *DataReplica, dataNode *DataNode, partitionID uint64) {
	dataNode.Lock()
	defer dataNode.Unlock()
	var key string
	newBadPartitionIDs := make([]uint64, 0)
	if replica != nil {
		key = fmt.Sprintf("%s:%s", dataNode.Addr, replica.DiskPath)
	} else {
		key = fmt.Sprintf("%s:%s", dataNode.Addr, "")
	}
	badPartitionIDs, ok := c.BadDataPartitionIds.Load(key)
	if ok {
		newBadPartitionIDs = badPartitionIDs.([]uint64)
	}
	newBadPartitionIDs = append(newBadPartitionIDs, partitionID)
	c.BadDataPartitionIds.Store(key, newBadPartitionIDs)
}

func (c *Cluster) metaNodeOffLine(metaNode *MetaNode, destAddr string) {
	msg := fmt.Sprintf("action[metaNodeOffLine],clusterID[%v] Node[%v] OffLine", c.Name, metaNode.Addr)
	log.LogWarn(msg)

	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, mp := range vol.MetaPartitions {
			c.metaPartitionOffline(vol.Name, metaNode.Addr, destAddr, mp.PartitionID)
		}
	}
	if err := c.syncDeleteMetaNode(metaNode); err != nil {
		msg = fmt.Sprintf("action[metaNodeOffLine],clusterID[%v] Node[%v] OffLine failed,err[%v]",
			c.Name, metaNode.Addr, err)
		Warn(c.Name, msg)
		return
	}
	c.delMetaNodeFromCache(metaNode)
	msg = fmt.Sprintf("action[metaNodeOffLine],clusterID[%v] Node[%v] OffLine success", c.Name, metaNode.Addr)
	Warn(c.Name, msg)
}

func (c *Cluster) delMetaNodeFromCache(metaNode *MetaNode) {
	c.metaNodes.Delete(metaNode.Addr)
	go metaNode.clean()
}

func (c *Cluster) updateVol(name, authKey string, capacity int) (err error) {
	var (
		vol           *Vol
		serverAuthKey string
	)
	if vol, err = c.getVol(name); err != nil {
		goto errDeal
	}
	if vol.Owner != "" {
		serverAuthKey = vol.Owner
	} else {
		serverAuthKey = vol.Name
	}
	if !matchKey(serverAuthKey, authKey) {
		return VolAuthKeyNotMatch
	}
	if uint64(capacity) < vol.Capacity {
		err = fmt.Errorf("capacity[%v] less than old capacity[%v]", capacity, vol.Capacity)
		goto errDeal
	}
	vol.setCapacity(uint64(capacity))
	if err = c.syncUpdateVol(vol); err != nil {
		goto errDeal
	}
	return
errDeal:
	err = fmt.Errorf("action[updateVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) createVol(name, owner, volType string, replicaNum uint8, capacity, mpCount int) (err error) {
	var (
		vol                     *Vol
		readWriteDataPartitions int
	)
	if volType == proto.TinyPartition {
		err = fmt.Errorf("vol type must be extent")
		goto errDeal
	}
	if vol, err = c.createVolInternal(name, owner, volType, replicaNum, capacity); err != nil {
		goto errDeal
	}
	if err = vol.batchCreateMetaPartition(c, mpCount); err != nil {
		if err = c.syncDeleteVol(vol); err != nil {
			log.LogErrorf("action[createVol] failed,vol[%v] err[%v]", vol.Name, err)
		}
		c.deleteVol(name)
		err = fmt.Errorf("action[createVol] initMetaPartitions failed")
		goto errDeal
	}
	for retryCount := 0; readWriteDataPartitions < DefaultInitDataPartitions && retryCount < 3; retryCount++ {
		vol.initDataPartitions(c)
		readWriteDataPartitions = len(vol.dataPartitions.dataPartitionMap)
	}
	vol.dataPartitions.readWriteDataPartitions = readWriteDataPartitions
	vol.updateViewCache(c)
	log.LogInfof("action[createVol] vol[%v],readWriteDataPartitions[%v]", name, readWriteDataPartitions)
	return
errDeal:
	err = fmt.Errorf("action[createVol], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) createVolInternal(name, owner, volType string, replicaNum uint8, capacity int) (vol *Vol, err error) {
	c.createVolLock.Lock()
	defer c.createVolLock.Unlock()
	if _, err = c.getVol(name); err == nil {
		err = hasExist(name)
		goto errDeal
	}
	vol = NewVol(name, owner, volType, replicaNum, uint64(capacity))
	if err = c.syncAddVol(vol); err != nil {
		goto errDeal
	}
	c.putVol(vol)
	return
errDeal:
	err = fmt.Errorf("action[createVolInternal], clusterID[%v] name:%v, err:%v ", c.Name, name, err.Error())
	log.LogError(errors.ErrorStack(err))
	Warn(c.Name, err.Error())
	return
}

func (c *Cluster) CreateMetaPartitionForManual(volName string, start uint64) (err error) {

	var (
		maxPartitionID uint64
		vol            *Vol
		partition      *MetaPartition
	)

	if vol, err = c.getVol(volName); err != nil {
		return errors.Annotatef(err, "get vol [%v] err", volName)
	}
	maxPartitionID = vol.getMaxPartitionID()
	if partition, err = vol.getMetaPartition(maxPartitionID); err != nil {
		return errors.Annotatef(err, "get meta partition [%v] err", maxPartitionID)
	}
	return vol.splitMetaPartition(c, partition, start)
}

func (c *Cluster) syncCreateMetaPartitionToMetaNode(host string, mp *MetaPartition) (err error) {
	hosts := make([]string, 0)
	hosts = append(hosts, host)
	tasks := mp.generateCreateMetaPartitionTasks(hosts, mp.Peers, mp.volName)
	return c.doSyncCreateMetaPartitionToMetaNode(host, tasks)
}

func (c *Cluster) doSyncCreateMetaPartitionToMetaNode(host string, tasks []*proto.AdminTask) (err error) {
	metaNode, err := c.getMetaNode(host)
	if err != nil {
		return
	}
	_, err = metaNode.Sender.syncSendAdminTask(tasks[0])
	return
}

func (c *Cluster) ChooseTargetMetaHosts(replicaNum int) (hosts []string, peers []proto.Peer, err error) {
	var (
		masterAddr []string
		slaveAddrs []string
		masterPeer []proto.Peer
		slavePeers []proto.Peer
	)
	hosts = make([]string, 0)
	if masterAddr, masterPeer, err = c.getAvailMetaNodeHosts(hosts, 1); err != nil {
		return nil, nil, errors.Trace(err)
	}
	peers = append(peers, masterPeer...)
	hosts = append(hosts, masterAddr[0])
	otherReplica := replicaNum - 1
	if otherReplica == 0 {
		return
	}
	if slaveAddrs, slavePeers, err = c.getAvailMetaNodeHosts(hosts, otherReplica); err != nil {
		return nil, nil, errors.Trace(err)
	}
	hosts = append(hosts, slaveAddrs...)
	peers = append(peers, slavePeers...)
	if len(hosts) != replicaNum {
		return nil, nil, NoAnyMetaNodeForCreateMetaPartition
	}
	return
}

func (c *Cluster) MetaNodeCount() (len int) {

	c.metaNodes.Range(func(key, value interface{}) bool {
		len++
		return true
	})
	return
}

func (c *Cluster) getAllDataNodes() (dataNodes []DataNodeView) {
	dataNodes = make([]DataNodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		dataNodes = append(dataNodes, DataNodeView{Addr: dataNode.Addr, Status: dataNode.isActive})
		return true
	})
	return
}

func (c *Cluster) getLiveDataNodesRate() (rate float32) {
	dataNodes := make([]DataNodeView, 0)
	liveDataNodes := make([]DataNodeView, 0)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		view := DataNodeView{Addr: dataNode.Addr, Status: dataNode.isActive}
		dataNodes = append(dataNodes, view)
		if dataNode.isActive && time.Since(dataNode.ReportTime) < time.Second*time.Duration(2*DefaultCheckHeartbeatIntervalSeconds) {
			liveDataNodes = append(liveDataNodes, view)
		}
		return true
	})
	return float32(len(liveDataNodes)) / float32(len(dataNodes))
}

func (c *Cluster) getLiveMetaNodesRate() (rate float32) {
	metaNodes := make([]MetaNodeView, 0)
	liveMetaNodes := make([]MetaNodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		view := MetaNodeView{Addr: metaNode.Addr, Status: metaNode.IsActive, ID: metaNode.ID}
		metaNodes = append(metaNodes, view)
		if metaNode.IsActive && time.Since(metaNode.ReportTime) < time.Second*time.Duration(2*DefaultCheckHeartbeatIntervalSeconds) {
			liveMetaNodes = append(liveMetaNodes, view)
		}
		return true
	})
	return float32(len(liveMetaNodes)) / float32(len(metaNodes))
}

func (c *Cluster) getAllMetaNodes() (metaNodes []MetaNodeView) {
	metaNodes = make([]MetaNodeView, 0)
	c.metaNodes.Range(func(addr, node interface{}) bool {
		metaNode := node.(*MetaNode)
		metaNodes = append(metaNodes, MetaNodeView{ID: metaNode.ID, Addr: metaNode.Addr, Status: metaNode.IsActive})
		return true
	})
	return
}

func (c *Cluster) getAllVols() (vols []string) {
	vols = make([]string, 0)
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	for name := range c.vols {
		vols = append(vols, name)
	}
	return
}

func (c *Cluster) copyVols() (vols map[string]*Vol) {
	vols = make(map[string]*Vol, 0)
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	for name, vol := range c.vols {
		vols[name] = vol
	}
	return
}

func (c *Cluster) getAllNormalVols() (vols map[string]*Vol) {
	vols = make(map[string]*Vol, 0)
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	for name, vol := range c.vols {
		if vol.Status == VolNormal {
			vols[name] = vol
		}
	}
	return
}

func (c *Cluster) getDataPartitionCount() (count int) {
	c.volsLock.RLock()
	defer c.volsLock.RUnlock()
	for _, vol := range c.vols {
		count = count + len(vol.dataPartitions.dataPartitions)
	}
	return
}

func (c *Cluster) syncCompactStatus(status bool) (err error) {
	oldCompactStatus := c.compactStatus
	c.compactStatus = status
	if err = c.syncPutCluster(); err != nil {
		c.compactStatus = oldCompactStatus
		log.LogErrorf("action[syncCompactStatus] failed,err:%v", err)
		return
	}
	return
}

func (c *Cluster) clearVols() {
	c.volsLock.Lock()
	defer c.volsLock.Unlock()
	c.vols = make(map[string]*Vol, 0)
}

func (c *Cluster) clearTopology() {
	c.t.clear()
}

func (c *Cluster) clearDataNodes() {
	c.dataNodes.Range(func(key, value interface{}) bool {
		dataNode := value.(*DataNode)
		dataNode.clean()
		c.dataNodes.Delete(key)
		return true
	})
}

func (c *Cluster) clearMetaNodes() {
	c.metaNodes.Range(func(key, value interface{}) bool {
		metaNode := value.(*MetaNode)
		metaNode.clean()
		c.dataNodes.Delete(key)
		return true
	})
}
