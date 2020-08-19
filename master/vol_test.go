package master

import (
	"encoding/json"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http"
	"testing"
	"time"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	commonVol.Capacity = 300 * util.TB
	dpCount := len(commonVol.dataPartitions.dataPartitions)
	commonVol.dataPartitions.readWriteDataPartitions = 0
	server.cluster.DisableAutoAlloc = false
	t.Logf("status[%v],disableAutoAlloc[%v],used[%v],cap[%v]\n",
		commonVol.Status, server.cluster.DisableAutoAlloc, commonVol.UsedSpace, commonVol.Capacity)
	commonVol.checkNeedAutoCreateDataPartitions(server.cluster)
	newDpCount := len(commonVol.dataPartitions.dataPartitions)
	if dpCount == newDpCount {
		t.Errorf("autoCreateDataPartitions failed,expand 0 data partitions,oldCount[%v],curCount[%v]", dpCount, newDpCount)
		return
	}
}

func TestCheckVol(t *testing.T) {
	commonVol.checkStatus(server.cluster)
	commonVol.checkMetaPartitions(server.cluster)
	commonVol.checkDataPartitions(server.cluster)
	log.LogFlush()
	fmt.Printf("writable data partitions[%v]\n", commonVol.dataPartitions.readWriteDataPartitions)
}

func TestVol(t *testing.T) {
	cap := 200
	name := "test1"
	createVol(name, t)
	//report mp/dp info to master
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	//check status
	server.cluster.checkMetaPartitions()
	server.cluster.checkDataPartitions()
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	vol.checkStatus(server.cluster)
	getVol(name, t)
	updateVol(name, cap, t)
	statVol(name, t)
	markDeleteVol(name, t)
	getSimpleVol(name, t)
	vol.checkStatus(server.cluster)
	vol.deleteVolFromStore(server.cluster)
}

func createVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfs&mpCount=2", hostAddr, AdminCreateVol, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	checkDataPartitionsWritableTest(vol, t)
	checkMetaPartitionsWritableTest(vol, t)
}
func checkDataPartitionsWritableTest(vol *Vol, t *testing.T) {
	if len(vol.dataPartitions.dataPartitions) == 0 {
		return
	}
	partition := vol.dataPartitions.dataPartitions[0]
	if partition.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
		return
	}

	//after check data partitions ,the status must be writable
	vol.checkDataPartitions(server.cluster)
	partition = vol.dataPartitions.dataPartitions[0]
	if partition.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, partition.Status)
		return
	}
}
func checkMetaPartitionsWritableTest(vol *Vol, t *testing.T) {
	if len(vol.MetaPartitions) == 0 {
		t.Error("no meta partition")
		return
	}

	for _, mp := range vol.MetaPartitions {
		if mp.Status != proto.ReadWrite {
			t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, mp.Status)
			return
		}
	}
	maxPartitionID := vol.getMaxPartitionID()
	maxMp := vol.MetaPartitions[maxPartitionID]
	//after check meta partitions ,the status must be writable
	maxMp.checkStatus(false, int(vol.mpReplicaNum), maxPartitionID)
	if maxMp.Status != proto.ReadWrite {
		t.Errorf("expect partition status[%v],real status[%v]\n", proto.ReadWrite, maxMp.Status)
		return
	}
}

func getVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, ClientVol, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func getSimpleVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, AdminGetVol, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func updateVol(name string, capacity int, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v",
		hostAddr, AdminUpdateVol, name, capacity, buildAuthKey())
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Capacity != uint64(capacity) {
		t.Errorf("update vol failed,expect[%v],real[%v]", capacity, vol.Capacity)
		return
	}
}

func statVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v",
		hostAddr, ClientVolStat, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func markDeleteVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&authKey=%v",
		hostAddr, AdminDeleteVol, name, buildAuthKey())
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err := server.cluster.getVol(name)
	if err != nil {
		t.Error(err)
		return
	}
	if vol.Status != VolMarkDelete {
		t.Errorf("markDeleteVol failed,expect[%v],real[%v]", VolMarkDelete, vol.Status)
		return
	}
}

// if the node change to leader,after reload the metadata,the meta partition's status is readonly,
// and the status is not the actual status,don't put to vol view cache and feedback to client
func TestUpdateVolViewAfterLeaderChange(t *testing.T) {
	name := "afterLeaderChange"
	vol := NewVol(name, name, "extent", 2, 100, 20, 3, false)
	//unavaliable mp
	mp1 := NewMetaPartition(1, 1, defaultMaxMetaPartitionInodeID, 3, name)
	vol.AddMetaPartition(mp1)
	//readonly mp
	mp2 := NewMetaPartition(2, 1, defaultMaxMetaPartitionInodeID, 3, name)
	mp2.Status = proto.ReadOnly
	vol.AddMetaPartition(mp2)
	vol.updateViewCache(server.cluster)
	volView := &VolView{}
	if err := json.Unmarshal(vol.getViewCache(), volView); err != nil {
		t.Error(err)
		return
	}
	if len(volView.MetaPartitions) != 0 {
		t.Error("expect len(mp) is 0")
		return
	}
	server.cluster.vols[name] = vol
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, ClientVol, name)
	fmt.Println(reqUrl)
	processWithStatus(reqUrl, http.StatusInternalServerError, t)
	mp2.Status = proto.ReadWrite
	vol.updateViewCache(server.cluster)
	if vol.isAllMetaPartitionReadonly {
		t.Errorf("isAllMetaPartitionReadonly[%v] should be false,vol.MetaPartitions[%v]\n", vol.isAllMetaPartitionReadonly, vol.MetaPartitions)
		return
	}

	reqUrl = fmt.Sprintf("%v%v?name=%v", hostAddr, ClientVol, name)
	fmt.Println(reqUrl)
	processWithStatus(reqUrl, http.StatusOK, t)
}

func TestConcurrentReadWriteDataPartitionMap(t *testing.T) {
	name := "TestConcurrentReadWriteDataPartitionMap"
	vol := NewVol(name, name, "extent", 2, 100, 20, 3, false)
	//unavaliable mp
	mp1 := NewMetaPartition(1, 1, defaultMaxMetaPartitionInodeID, 3, name)
	vol.AddMetaPartition(mp1)
	//readonly mp
	mp2 := NewMetaPartition(2, 1, defaultMaxMetaPartitionInodeID, 3, name)
	mp2.Status = proto.ReadOnly
	vol.AddMetaPartition(mp2)
	vol.updateViewCache(server.cluster)
	go func() {
		var id uint64
		for {
			id++
			dp := newDataPartition(id, 3, proto.ExtentPartition, name)
			vol.dataPartitions.putDataPartition(dp)
			time.Sleep(time.Second)
		}
	}()
	for i := 0; i < 10; i++ {
		time.Sleep(time.Second)
		vol.updateViewCache(server.cluster)
	}
}
