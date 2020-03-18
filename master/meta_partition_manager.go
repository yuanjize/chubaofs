package master

import (
	"fmt"
	"time"
	"github.com/chubaofs/chubaofs/util/log"
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
			vol, err := c.getVol(partition.volName)
			if err != nil {
				continue
			}
			if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.mpReplicaNum) {
				continue
			}
			diff = partition.getMinusOfMaxInodeID()
			if diff < defaultMinusOfMaxInodeID {
				partition.IsRecover = false
				Warn(c.Name, fmt.Sprintf("clusterID[%v],vol[%v] partitionID[%v] has recovered success", c.Name, partition.volName, partitionID))
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
