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
	"net/http"
	"testing"

	"github.com/chubaofs/chubaofs/master/mocktest"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
	"time"
	"io/ioutil"
	"os"
	"crypto/md5"
	"encoding/hex"
)

const (
	hostAddr          = "http://127.0.0.1:8080"
	ConfigKeyLogDir   = "logDir"
	ConfigKeyLogLevel = "logLevel"
	mds1Addr          = "127.0.0.1:9101"
	mds2Addr          = "127.0.0.1:9102"
	mds3Addr          = "127.0.0.1:9103"
	mds4Addr          = "127.0.0.1:9104"
	mds5Addr          = "127.0.0.1:9105"

	mms1Addr      = "127.0.0.1:8101"
	mms2Addr      = "127.0.0.1:8102"
	mms3Addr      = "127.0.0.1:8103"
	commonVolName = "commonVol"
)

var server = createMasterServer()
var commonVol *Vol

func createMasterServer() *Master {
	cfgJson := `{
	"role": "master",
		"ip": "127.0.0.1",
		"port": "8080",
		"prof":"10088",
		"id":"1",
		"peers": "1:127.0.0.1:8080",
		"retainLogs":"20000",
		"logDir": "/export/chubaofs/Logs",
		"logLevel":"DEBUG",
		"walDir":"/export/chubaofs/raft",
		"storeDir":"/export/chubaofs/rocksdbstore",
		"clusterName":"chubaofs"
	}`
	cfg := config.LoadConfigString(cfgJson)
	server := NewServer()
	logDir := cfg.GetString(ConfigKeyLogDir)
	walDir := cfg.GetString(WalDir)
	storeDir := cfg.GetString(StoreDir)
	os.RemoveAll(logDir)
	os.RemoveAll(walDir)
	os.RemoveAll(storeDir)
	os.Mkdir(walDir, 0755)
	os.Mkdir(storeDir, 0755)
	logLevel := cfg.GetString(ConfigKeyLogLevel)
	var level log.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}
	if _, err := log.InitLog(logDir, "master", level); err != nil {
		fmt.Println("Fatal: failed to start the chubaofs daemon - ", err)
	}
	server.Start(cfg)
	time.Sleep(5 * time.Second)
	fmt.Println(server.config.peerAddrs, server.leaderInfo.addr)
	//add data node
	addDataServer(mds1Addr)
	addDataServer(mds2Addr)
	addDataServer(mds3Addr)
	addDataServer(mds4Addr)
	addDataServer(mds5Addr)
	// add meta node
	addMetaServer(mms1Addr)
	addMetaServer(mms2Addr)
	addMetaServer(mms3Addr)
	time.Sleep(5 * time.Second)
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.startCheckAvailSpace()
	server.cluster.createVol(commonVolName, "cfs", "extent", 3, 100)
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		panic(err)
	}
	commonVol = vol
	fmt.Printf("vol[%v] has created\n", commonVol.Name)
	return server
}

func addDataServer(addr string) {
	mds := mocktest.NewMockDataServer(addr)
	mds.Start()
}

func addMetaServer(addr string) {
	mms := mocktest.NewMockMetaServer(addr)
	mms.Start()
}

func TestSetMetaNodeThreshold(t *testing.T) {
	threshold := 0.5
	reqUrl := fmt.Sprintf("%v%v?threshold=%v", hostAddr, AdminSetMetaNodeThreshold, threshold)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	if server.cluster.cfg.MetaNodeThreshold != float32(threshold) {
		t.Errorf("set metanode threshold to %v failed", threshold)
		return
	}
}

func TestSetCompactStatus(t *testing.T) {
	enable := true
	reqUrl := fmt.Sprintf("%v%v?enable=%v", hostAddr, AdminSetCompactStatus, enable)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	// compact status has deprecated
	//if server.cluster.compactStatus != enable {
	//	t.Errorf("set compact status to %v failed",enable)
	//}
}

func TestSetDisableAutoAlloc(t *testing.T) {
	enable := true
	reqUrl := fmt.Sprintf("%v%v?enable=%v", hostAddr, AdminClusterFreeze, enable)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	if server.cluster.DisableAutoAlloc != enable {
		t.Errorf("set disableAutoAlloc to %v failed", enable)
		return
	}
}

func TestGetCompactStatus(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, AdminGetCompactStatus)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func TestGetCluster(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, AdminGetCluster)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func TestGetIpAndClusterName(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, AdminGetIp)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func TestDataNode(t *testing.T) {
	// /dataNode/add and /dataNode/response processed by mock data server
	addr := "127.0.0.1:9096"
	addDataServer(addr)
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getDataNodeInfo(addr, t)
	offlineDataNode(addr, t)
	_, err := server.cluster.getDataNode(addr)
	if err == nil {
		t.Errorf("offline datanode [%v] failed", addr)
	}
	server.cluster.dataNodes.Delete(addr)
}

func getDataNodeInfo(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, GetDataNode, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func offlineDataNode(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, DataNodeOffline, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func TestMetaNode(t *testing.T) {
	// /metaNode/add and /metaNode/response processed by mock meta server
	addr := "127.0.0.1:8106"
	addMetaServer(addr)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getMetaNodeInfo(addr, t)
	offlineMetaNode(addr, t)
}

func getMetaNodeInfo(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, GetMetaNode, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func offlineMetaNode(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, MetaNodeOffline, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
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
}

func createVol(name string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfs", hostAddr, AdminCreateVol, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func process(reqUrl string, t *testing.T) {
	resp, err := http.Get(reqUrl)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	fmt.Println(resp.StatusCode)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Errorf("err is %v", err)
		return
	}
	fmt.Println(string(body))
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	return
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

func TestMetaPartition(t *testing.T) {
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.checkMetaPartitions()
	createMetaPartition(commonVol, 10000, t)
	maxPartitionID := commonVol.getMaxPartitionID()
	getMetaPartition(commonVol.Name, maxPartitionID, t)
	isManual := false
	updateMetaPartition(commonVol, maxPartitionID, isManual, t)
	offlineMetaPartition(commonVol, maxPartitionID, t)
}

func offlineMetaPartition(vol *Vol, id uint64, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, AdminGetCluster)
	fmt.Println(reqUrl)
	process(reqUrl, t)
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
	if contains(mp.PersistenceHosts, offlineAddr) {
		t.Errorf("offlineMetaPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, mp.PersistenceHosts)
		return
	}
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

func getMetaPartition(volName string, id uint64, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v",
		hostAddr, ClientMetaPartition, volName, id)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func createMetaPartition(vol *Vol, start int, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v&start=%v",
		hostAddr, AdminCreateMP, vol.Name, start)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	maxPartitionID := vol.getMaxPartitionID()
	mp, err := vol.getMetaPartition(maxPartitionID)
	if err != nil {
		t.Errorf("createMetaPartition,err [%v]", err)
		return
	}
	start = start + 1
	if mp.Start != uint64(start) {
		t.Errorf("expect start[%v],mp.start[%v],not equal", start, mp.Start)
		return
	}

}

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
	isManual := false
	updateDataPartition(commonVol, partition.PartitionID, isManual, t)
	offlineDataPartition(partition, t)
}

func offlineDataPartition(p *DataPartition, t *testing.T) {
	offlineAddr := p.PersistenceHosts[0]
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, AdminDataPartitionOffline, p.VolName, p.PartitionID, offlineAddr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
	if contains(p.PersistenceHosts, offlineAddr) {
		t.Errorf("offlineDataPartition failed,offlineAddr[%v],hosts[%v]", offlineAddr, p.PersistenceHosts)
		return
	}
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

func getDataPartition(id uint64, t *testing.T) {

	reqUrl := fmt.Sprintf("%v%v?id=%v",
		hostAddr, AdminGetDataPartition, id)
	fmt.Println(reqUrl)
	process(reqUrl, t)
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

func TestDisk(t *testing.T) {
	addr := mds5Addr
	disk := "/cfs"
	diskOffline(addr, disk, t)
}

func diskOffline(addr, path string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v&disk=%v",
		hostAddr, DiskOffLine, addr, path)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func TestMarkDeleteVol(t *testing.T) {
	name := "delVol"
	createVol(name, t)
	reqUrl := fmt.Sprintf("%v%v?name=%v&authKey=%v", hostAddr, AdminDeleteVol, name, buildAuthKey())
	process(reqUrl, t)
}

func TestUpdateVol(t *testing.T) {
	cap := 2000
	reqUrl := fmt.Sprintf("%v%v?name=%v&capacity=%v&authKey=%v",
		hostAddr, AdminUpdateVol, commonVol.Name, cap, buildAuthKey())
	process(reqUrl, t)
}
func buildAuthKey() string {
	h := md5.New()
	h.Write([]byte("cfs"))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func TestGetVolSimpleInfo(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, AdminGetVol, commonVol.Name)
	process(reqUrl, t)
}

func TestCreateVol(t *testing.T) {
	name := "test_create_vol"
	reqUrl := fmt.Sprintf("%v%v?name=%v&replicas=3&type=extent&capacity=100&owner=cfs", hostAddr, AdminCreateVol, name)
	fmt.Println(reqUrl)
	process(reqUrl, t)

}

func TestCreateMetaPartition(t *testing.T) {
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	commonVol.checkMetaPartitions(server.cluster)
	createMetaPartition(commonVol, 10000, t)
}

func TestUpdateMetaPartition(t *testing.T) {
	maxPartitionID := commonVol.getMaxPartitionID()
	updateMetaPartition(commonVol, maxPartitionID, false, t)
}

func TestUpdateDataPartition(t *testing.T) {
	if len(commonVol.dataPartitions.dataPartitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.dataPartitions[0]
	updateDataPartition(commonVol, partition.PartitionID, false, t)
}

func TestDeleteDataPartition(t *testing.T) {
	if len(commonVol.dataPartitions.dataPartitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.dataPartitions[0]
	reqUrl := fmt.Sprintf("%v%v?id=%v", hostAddr, AdminDataPartitionDelete, partition.PartitionID)
	process(reqUrl, t)
}

func TestCreateDataPartition(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?count=2&name=%v&type=extent",
		hostAddr, AdminCreateDataPartition, commonVol.Name)
	process(reqUrl, t)
}

func TestGetDataPartition(t *testing.T) {
	if len(commonVol.dataPartitions.dataPartitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.dataPartitions[0]
	reqUrl := fmt.Sprintf("%v%v?id=%v", hostAddr, AdminGetDataPartition, partition.PartitionID)
	process(reqUrl, t)
}

func TestLoadDataPartition(t *testing.T) {
	if len(commonVol.dataPartitions.dataPartitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.dataPartitions[0]
	reqUrl := fmt.Sprintf("%v%v?id=%v&name=%v",
		hostAddr, AdminLoadDataPartition, partition.PartitionID, commonVol.Name)
	process(reqUrl, t)
}

func TestDataPartitionOffline(t *testing.T) {
	if len(commonVol.dataPartitions.dataPartitions) == 0 {
		t.Errorf("no data partitions")
		return
	}
	partition := commonVol.dataPartitions.dataPartitions[0]
	offlineAddr := partition.PersistenceHosts[0]
	reqUrl := fmt.Sprintf("%v%v?name=%v&id=%v&addr=%v",
		hostAddr, AdminDataPartitionOffline, commonVol.Name, partition.PartitionID, offlineAddr)
	process(reqUrl, t)
	if contains(partition.PersistenceHosts, offlineAddr) {
		t.Errorf("offlineAddr[%v],hosts[%v]", offlineAddr, partition.PersistenceHosts)
		return
	}
}
