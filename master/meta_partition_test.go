package master

import (
	"fmt"
	"testing"
	"time"
)

func TestMetaPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkMetaPartitions()
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
		return
	}
	createMetaPartition(commonVol, t)
	maxPartitionID := commonVol.getMaxPartitionID()
	getMetaPartition(commonVol.Name, maxPartitionID, t)
	isManual := false
	updateMetaPartition(commonVol, maxPartitionID, isManual, t)
	loadMetaPartitionTest(commonVol, maxPartitionID, t)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	offlineMetaPartition(commonVol, maxPartitionID, t)
	updateMetaPartitionHostsTest(commonVol, maxPartitionID, t)
}

func createMetaPartition(vol *Vol, t *testing.T) {
	maxPartitionID := commonVol.getMaxPartitionID()
	mp, err := commonVol.getMetaPartition(maxPartitionID)
	if err != nil {
		t.Error(err)
		return
	}
	var start uint64
	start = mp.Start + defaultMetaPartitionInodeIDStep
	reqUrl := fmt.Sprintf("%v%v?name=%v&start=%v",
		hostAddr, AdminCreateMP, vol.Name, start)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err = server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}
	maxPartitionID = vol.getMaxPartitionID()
	mp, err = vol.getMetaPartition(maxPartitionID)
	if err != nil {
		t.Errorf("createMetaPartition,err [%v]", err)
		return
	}
	start = start + 1
	if mp.Start != start {
		t.Errorf("expect start[%v],mp.start[%v],not equal", start, mp.Start)
		return
	}
}

func getMetaPartition(volName string, id uint64, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v",
		hostAddr, ClientMetaPartition, volName, id)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func updateMetaPartition(vol *Vol, id uint64, isManual bool, t *testing.T) {

	reqUrl := fmt.Sprintf("%v%v?id=%v&isManual=%v",
		hostAddr, AdminMetaPartitionUpdate, id, isManual)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	mp, err := vol.getMetaPartition(id)
	if err != nil {
		t.Errorf("updateMetaPartition,err [%v]", err)
		return
	}
	if mp.IsManual != isManual {
		t.Errorf("expect isManual[%v],mp.IsManual[%v],not equal", isManual, mp.IsManual)
		return
	}
}

func loadMetaPartitionTest(vol *Vol, id uint64, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v", hostAddr, AdminLoadMetaPartition, vol.Name, id)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func offlineMetaPartition(vol *Vol, id uint64, t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	reqUrl := fmt.Sprintf("%v%v", hostAddr, AdminGetCluster)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	vol, err := server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}
	mp, err := vol.getMetaPartition(id)
	if err != nil {
		t.Errorf("offlineMetaPartition,err [%v]", err)
		return
	}
	offlineAddr := mp.PersistenceHosts[0]
	reqUrl = fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, AdminMetaPartitionOffline, vol.Name, id, offlineAddr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	mp, err = server.cluster.getMetaPartitionByID(id)
	if err != nil {
		t.Errorf("offlineMetaPartition,err [%v]", err)
		return
	}
	if contains(mp.PersistenceHosts, offlineAddr) {
		t.Errorf("offlineMetaPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, mp.PersistenceHosts)
		return
	}
}

func updateMetaPartitionHostsTest(vol *Vol, id uint64, t *testing.T) {
	vol, err := server.cluster.getVol(vol.Name)
	if err != nil {
		t.Error(err)
		return
	}
	hosts := fmt.Sprintf("%v,%v,%v", mms1Addr, mms2Addr, mms3Addr)
	mp, err := vol.getMetaPartition(id)
	if err != nil {
		t.Error(err)
		return
	}
	start := mp.Start + defaultMetaPartitionInodeIDStep
	err = server.cluster.CreateMetaPartitionForManual(vol.Name, start)
	if err != nil {
		t.Error(err)
		return
	}
	maxPartitionID := vol.getMaxPartitionID()
	err = server.cluster.updateMetaPartitionHosts(vol.Name, hosts, maxPartitionID)
	if err != nil {
		t.Error(err)
		return
	}
	mp, err = server.cluster.getMetaPartitionByID(maxPartitionID)
	if err != nil {
		t.Error(err)
		return
	}
	for _, host := range mp.PersistenceHosts {
		if host != mms3Addr && host != mms2Addr && host != mms1Addr {
			t.Errorf("expect hosts[%v],persistentHosts[%v]", hosts, mp.PersistenceHosts)
			return
		}
	}
}
