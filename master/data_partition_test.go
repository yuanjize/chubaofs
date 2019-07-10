package master

import (
	"time"
	"testing"
	"fmt"
	"github.com/chubaofs/chubaofs/util"
)

func TestDataPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkDataPartitions()
	count := 20
	createDataPartition(commonVol, count, t)
	if len(commonVol.dataPartitions.dataPartitions) <= 0 {
		t.Errorf("getDataPartition no dp")
		return
	}
	partition := commonVol.dataPartitions.dataPartitions[0]
	getDataPartition(partition.PartitionID, t)
	updateDataPartition(commonVol, partition.PartitionID, false, t)
	loadDataPartitionTest(partition, t)
	offlineDataPartition(partition, t)
}

func createDataPartition(vol *Vol, count int, t *testing.T) {
	oldCount := len(vol.dataPartitions.dataPartitions)
	reqUrl := fmt.Sprintf("%v%v?count=%v&name=%v&type=extent",
		hostAddr, AdminCreateDataPartition, count, vol.Name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	newCount := len(vol.dataPartitions.dataPartitions)
	total := oldCount + count
	if newCount != total {
		t.Errorf("createDataPartition failed,newCount[%v],total=%v,count[%v],oldCount[%v]",
			newCount, total, count, oldCount)
		return
	}
}

func getDataPartition(id uint64, t *testing.T) {

	reqUrl := fmt.Sprintf("%v%v?id=%v",
		hostAddr, AdminGetDataPartition, id)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func updateDataPartition(vol *Vol, id uint64, isManual bool, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?id=%v&isManual=%v",
		hostAddr, AdminDataPartitionUpdate, id, isManual)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	dp, err := vol.getDataPartitionByID(id)
	if err != nil {
		t.Errorf("updateDataPartition,err [%v]", err)
		return
	}
	if dp.IsManual != isManual {
		t.Errorf("expect isManual[%v],dp.IsManual[%v],not equal", isManual, dp.IsManual)
		return
	}
}

func offlineDataPartition(dp *DataPartition, t *testing.T) {
	offlineAddr := dp.PersistenceHosts[0]
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, AdminDataPartitionOffline, dp.VolName, dp.PartitionID, offlineAddr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	if contains(dp.PersistenceHosts, offlineAddr) {
		t.Errorf("offlineDataPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, dp.PersistenceHosts)
		return
	}
}

func loadDataPartitionTest(dp *DataPartition, t *testing.T) {
	dps := make([]*DataPartition, 0)
	dps = append(dps, dp)
	server.cluster.waitLoadDataPartitionResponse(dps)
	time.Sleep(5 * time.Second)
	dp.RLock()
	for _, replica := range dp.Replicas {
		t.Logf("replica[%v],response[%v]", replica.Addr, replica.LoadPartitionIsResponse)
	}
	tinyFile := NewFileInCore("50000011", 1562507765)
	extentFile := NewFileInCore("10", 1562507765)
	for index, host := range dp.PersistenceHosts {
		fm := NewFileMetaOnNode(uint32(404551221)+uint32(index), host, index, 2*util.MB)
		tinyFile.Metas = append(tinyFile.Metas, fm)
		extentFile.Metas = append(extentFile.Metas, fm)
	}

	dp.FileInCoreMap[tinyFile.Name] = tinyFile
	dp.FileInCoreMap[extentFile.Name] = extentFile
	dp.RUnlock()
	dp.checkFile(server.cluster.Name)
}
