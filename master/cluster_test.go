package master

import (
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"sync"
	"testing"
	"time"
)

func buildPanicCluster() *Cluster {
	c := newCluster(server.cluster.Name, server.cluster.leaderInfo, server.cluster.fsm, server.cluster.partition, server.cluster.cfg)
	v := buildPanicVol()
	c.putVol(v)
	return c
}

func buildPanicVol() *Vol {
	vol := NewVol(commonVol.Name, commonVol.Owner, commonVol.VolType, commonVol.dpReplicaNum, commonVol.Capacity, false)
	vol.dataPartitions = nil
	return vol
}

func TestCheckDataPartitions(t *testing.T) {
	server.cluster.checkDataPartitions()
}

func TestPanicCheckDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.checkDataPartitions()
	t.Logf("catched panic")
}

func TestCheckBackendLoadDataPartitions(t *testing.T) {
	server.cluster.backendLoadDataPartitions()
}

func TestPanicBackendLoadDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.backendLoadDataPartitions()
	t.Logf("catched panic")
}

func TestCheckReleaseDataPartitions(t *testing.T) {
	server.cluster.releaseDataPartitionAfterLoad()
}
func TestPanicCheckReleaseDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.releaseDataPartitionAfterLoad()
	t.Logf("catched panic")
}

func TestCheckHeartbeat(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
}

func TestCheckMetaPartitions(t *testing.T) {
	server.cluster.checkMetaPartitions()
}

func TestPanicCheckMetaPartitions(t *testing.T) {
	c := buildPanicCluster()
	vol, err := c.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	partitionID, err := server.cluster.idAlloc.allocateMetaPartitionID()
	if err != nil {
		t.Error(err)
	}
	mp := NewMetaPartition(partitionID, 1, defaultMaxMetaPartitionInodeID, vol.mpReplicaNum, vol.Name)
	vol.AddMetaPartition(mp)
	mp = nil
	c.checkMetaPartitions()
	t.Logf("catched panic")
}

func TestCheckAvailSpace(t *testing.T) {
	server.cluster.checkAvailSpace()
}

func TestPanicCheckAvailSpace(t *testing.T) {
	c := buildPanicCluster()
	c.dataNodeSpace = nil
	c.checkAvailSpace()
}

func TestCheckCreateDataPartitions(t *testing.T) {
	server.cluster.checkCreateDataPartitions()
	//time.Sleep(150 * time.Second)
}

func TestPanicCheckCreateDataPartitions(t *testing.T) {
	c := buildPanicCluster()
	c.checkCreateDataPartitions()
}

func TestPanicCheckBadDiskRecovery(t *testing.T) {
	c := buildPanicCluster()
	vol, err := c.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	partitionID, err := server.cluster.idAlloc.allocateDataPartitionID()
	if err != nil {
		t.Error(err)
	}
	dp := newDataPartition(partitionID, vol.dpReplicaNum, vol.VolType, vol.Name)
	c.BadDataPartitionIds.Store(fmt.Sprintf("%v", dp.PartitionID), dp)
	c.checkBadDiskRecovery()
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
				dp.Unlock()
				wg.Done()
			}()
			dp.Lock()
			if len(dp.Replicas) == 0 {
				dpsLen--
				return
			}
			addr := dp.Replicas[0].dataNode.Addr
			dataNode, _ := server.cluster.getDataNode(addr)
			server.cluster.putBadDataPartitionIDs(dp.Replicas[0], dataNode, dp.PartitionID)
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

func TestUpdateEnd(t *testing.T) {
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	maxPartitionID := vol.getMaxPartitionID()
	vol.RLock()
	mp := vol.MetaPartitions[maxPartitionID]
	mpLen := len(vol.MetaPartitions)
	vol.RUnlock()
	mr := &proto.MetaPartitionReport{
		PartitionID: mp.PartitionID,
		Start:       mp.Start,
		End:         mp.End,
		Status:      int(mp.Status),
		MaxInodeID:  mp.Start + 1,
		IsLeader:    false,
		VolName:     mp.volName,
	}
	metaNode, err := server.cluster.getMetaNode(mp.PersistenceHosts[0])
	if err != nil {
		t.Error(err)
		return
	}
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	if err = server.cluster.updateEnd(mp, mr, true, metaNode); err != nil {
		t.Error(err)
		return
	}
	curMpLen := len(vol.MetaPartitions)
	if curMpLen == mpLen {
		t.Errorf("split failed,oldMpLen[%v],curMpLen[%v]", mpLen, curMpLen)
	}

}
