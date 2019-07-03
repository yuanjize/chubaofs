package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"time"
)

func (c *Cluster) startCheckBadDiskRecovery() {
	go func() {
		for {
			if c.partition.IsLeader() {
				if c.vols != nil {
					c.checkBadDiskRecovery()
				}
			}
			time.Sleep(time.Second * DefaultCheckDataPartitionIntervalSeconds)
		}
	}()
}

func (c *Cluster) checkBadDiskRecovery() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkBadDiskRecovery occurred panic,err[%v]", r)
			WarnBySpecialUmpKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, UmpModuleName),
				"checkBadDiskRecovery occurred panic")
		}
	}()
	var minus float64
	c.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		newBadDpIds := make([]uint64, 0)
		for _, partitionID := range badDataPartitionIds {
			partition, err := c.getDataPartitionByID(partitionID)
			if err != nil {
				newBadDpIds = append(newBadDpIds, partitionID)
				continue
			}
			vol, err := c.getVol(partition.VolName)
			if err != nil {
				newBadDpIds = append(newBadDpIds, partitionID)
				continue
			}
			if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.dpReplicaNum) {
				newBadDpIds = append(newBadDpIds, partitionID)
				continue
			}
			minus = partition.getMinus()
			if minus < util.GB {
				Warn(c.Name, fmt.Sprintf("clusterID[%v],partitionID[%v] has recovered success", c.Name, partitionID))
				partition.isRecover = false
			} else {
				newBadDpIds = append(newBadDpIds, partitionID)
			}
		}
		if len(newBadDpIds) == 0 {
			Warn(c.Name, fmt.Sprintf("clusterID[%v],node:disk[%v] has recovered success", c.Name, key))
			c.BadDataPartitionIds.Delete(key)
		} else {
			c.BadDataPartitionIds.Store(key, newBadDpIds)
		}

		return true
	})
}

func (c *Cluster) diskOffLine(dataNode *DataNode, destAddr, badDiskPath string, badPartitionIds []uint64) {
	msg := fmt.Sprintf("action[diskOffLine], Node[%v] OffLine,disk[%v]", dataNode.Addr, badDiskPath)
	log.LogWarn(msg)
	safeVols := c.getAllNormalVols()
	for _, vol := range safeVols {
		for _, dp := range vol.dataPartitions.dataPartitions {
			for _, bad := range badPartitionIds {
				if bad == dp.PartitionID {
					c.dataPartitionOffline(dataNode.Addr, destAddr, vol.Name, dp, DiskOfflineInfo)
				}
			}
		}
	}
	msg = fmt.Sprintf("action[diskOffLine],clusterID[%v] Node[%v] OffLine success",
		c.Name, dataNode.Addr)
	Warn(c.Name, msg)
}
