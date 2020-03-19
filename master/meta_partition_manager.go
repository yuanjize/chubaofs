package master

import (
	"fmt"
	"time"
	"github.com/chubaofs/chubaofs/util/log"
	"math"
	"strconv"
)

func (c *Cluster) startCheckMetaPartitionRecoveryProgress() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				if c.vols != nil {
					c.checkMetaPartitionRecoveryProgress()
				}
			}
			time.Sleep(time.Second * DefaultCheckDataPartitionIntervalSeconds)
		}
	}()
}

func (c *Cluster) checkMetaPartitionRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMetaPartitionRecoveryProgress occurred panic,err[%v]", r)
			Warn(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"checkMetaPartitionRecoveryProgress occurred panic")
		}
	}()

	var diff float64
	c.BadMetaPartitionIds.Range(func(key, value interface{}) bool {
		badMetaPartitionIds := value.([]uint64)
		newBadMpIds := make([]uint64, 0)
		for _, partitionID := range badMetaPartitionIds {
			partition, err := c.getMetaPartitionByID(partitionID)
			if err != nil {
				continue
			}
			vol, err := c.getVol(partition.VolName)
			if err != nil {
				continue
			}
			if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.mpReplicaNum) {
				continue
			}
			diff = partition.getMinusOfMaxInodeID()
			if diff < defaultMinusOfMaxInodeID {
				partition.IsRecover = false
				Warn(c.Name, fmt.Sprintf("clusterID[%v],vol[%v] partitionID[%v] has recovered success", c.Name, partition.VolName, partitionID))
			} else {
				newBadMpIds = append(newBadMpIds, partitionID)
			}
		}

		if len(newBadMpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("clusterID[%v],node[%v] has recovered success", c.Name, key))
			c.BadMetaPartitionIds.Delete(key)
		} else {
			c.BadMetaPartitionIds.Store(key, newBadMpIds)
		}

		return true
	})
}

func (c *Cluster) startCheckLoadMetaPartitions() {
	go func() {
		for {
			if c.partition != nil && c.partition.IsLeader() {
				if c.vols != nil {
					c.checkLoadMetaPartitions()
				}
			}
			time.Sleep(2 * time.Second * DefaultCheckDataPartitionIntervalSeconds)
		}
	}()
}

func (c *Cluster) checkLoadMetaPartitions() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkDiskRecoveryProgress occurred panic,err[%v]", r)
			Warn(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"checkDiskRecoveryProgress occurred panic")
		}
	}()
	vols := c.copyVols()
	for _, vol := range vols {
		mps := vol.cloneMetaPartitionMap()
		for _, mp := range mps {
			c.doLoadMetaPartition(mp)
		}
	}
}

func (mp *MetaPartition) checkSnapshot(clusterID string) {
	if len(mp.LoadResponse) == 0 {
		return
	}
	if !mp.doCompare() {
		return
	}
	if !mp.isSameApplyID() {
		return
	}
	mp.checkInodeCount(clusterID)
	mp.checkDentryCount(clusterID)
}

func (mp *MetaPartition) doCompare() bool {
	for _, lr := range mp.LoadResponse {
		if !lr.DoCompare {
			return false
		}
	}
	return true
}

func (mp *MetaPartition) isSameApplyID() bool {
	rst := true
	applyID := mp.LoadResponse[0].ApplyID
	for _, loadResponse := range mp.LoadResponse {
		if applyID != loadResponse.ApplyID {
			rst = false
		}
	}
	return rst
}

func (mp *MetaPartition) checkInodeCount(clusterID string) {
	isEqual := true
	maxInode := mp.LoadResponse[0].MaxInode
	for _, loadResponse := range mp.LoadResponse {
		diff := math.Abs(float64(loadResponse.MaxInode) - float64(maxInode))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
		}
	}

	if !isEqual {
		msg := fmt.Sprintf("inode count is not equal,vol[%v],mpID[%v],", mp.VolName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			inodeCountStr := strconv.FormatUint(lr.MaxInode, 10)
			applyIDStr := strconv.FormatUint(uint64(lr.ApplyID), 10)
			msg = msg + lr.Addr + " applyId[" + applyIDStr + "] maxInode[" + inodeCountStr + "],"
		}
		Warn(clusterID, msg)
	}
}

func (mp *MetaPartition) checkDentryCount(clusterID string) {
	isEqual := true
	dentryCount := mp.LoadResponse[0].DentryCount
	for _, loadResponse := range mp.LoadResponse {
		diff := math.Abs(float64(loadResponse.DentryCount) - float64(dentryCount))
		if diff > defaultRangeOfCountDifferencesAllowed {
			isEqual = false
		}
	}

	if !isEqual {
		msg := fmt.Sprintf("dentry count is not equal,vol[%v],mpID[%v],", mp.VolName, mp.PartitionID)
		for _, lr := range mp.LoadResponse {
			dentryCountStr := strconv.FormatUint(lr.DentryCount, 10)
			applyIDStr := strconv.FormatUint(uint64(lr.ApplyID), 10)
			msg = msg + lr.Addr + " applyId[" + applyIDStr + "] dentryCount[" + dentryCountStr + "],"
		}
		Warn(clusterID, msg)
	}
}
