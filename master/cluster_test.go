package master

import (
	"testing"
	"sync"
)

func TestCheckDataPartitions(t *testing.T) {
	server.cluster.checkDataPartitions()
}

func TestCheckBackendLoadDataPartitions(t *testing.T) {
	server.cluster.backendLoadDataPartitions()
}

func TestCheckReleaseDataPartitions(t *testing.T) {
	server.cluster.releaseDataPartitionAfterLoad()
}

func TestCheckHeartbeat(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
}

func TestCheckMetaPartitions(t *testing.T) {
	server.cluster.checkMetaPartitions()
}

func TestCheckAvailSpace(t *testing.T) {
	server.cluster.checkAvailSpace()
}

func TestCheckCreateDataPartitions(t *testing.T) {
	server.cluster.checkCreateDataPartitions()
}

func TestCheckBadDiskRecovery(t *testing.T) {
	var wg sync.WaitGroup
	commonVol.RLock()
	dps := make([]*DataPartition, 0)
	for _, dp := range commonVol.dataPartitions.dataPartitions {
		dps = append(dps, dp)
	}
	dpsMapLen := len(commonVol.dataPartitions.dataPartitionMap)
	commonVol.RUnlock()
	dpsLen := len(dps)
	if dpsLen != dpsMapLen {
		t.Errorf("dpsLen[%v],dpsMapLen[%v]", dpsLen, dpsMapLen)
		return
	}
	//simulate multi data partition decommission
	for _, dp := range dps {
		wg.Add(1)
		go func(dp *DataPartition) {
			defer func() {
				dp.RUnlock()
				wg.Done()
			}()
			dp.RLock()
			if len(dp.Replicas) == 0 {
				dpsLen--
				return
			}
			server.cluster.putBadDataPartitionIDs(dp.Replicas[0], dp.Replicas[0].dataNode.Addr, dp.PartitionID)
		}(dp)
	}
	wg.Wait()
	count := 0
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		badDataPartitionIds := value.([]uint64)
		count = count + len(badDataPartitionIds)
		return true
	})

	if count != dpsLen {
		t.Errorf("expect bad partition num[%v],real num[%v]", dpsLen, count)
		return
	}
	//check recovery
	server.cluster.checkBadDiskRecovery()

	count = 0
	server.cluster.BadDataPartitionIds.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expect bad partition num[0],real num[%v]", count)
		return
	}
}
