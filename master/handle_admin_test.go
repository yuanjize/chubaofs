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
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/chubaofs/chubaofs/master/mocktest"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"testing"
	"time"
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
	mms4Addr      = "127.0.0.1:8104"
	mms5Addr      = "127.0.0.1:8105"
	mms6Addr      = "127.0.0.1:8106"
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
	profPort := cfg.GetString("prof")
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
	if profPort != "" {
		go func() {
			err := http.ListenAndServe(fmt.Sprintf(":%v", profPort), nil)
			if err != nil {
				panic(fmt.Sprintf("cannot listen pprof %v err %v", profPort, err.Error()))
			}
		}()
	}
	server.Start(cfg)
	time.Sleep(5 * time.Second)
	fmt.Println(server.config.peerAddrs, server.leaderInfo.addr)
	//add data node
	addDataServer(mds1Addr, "rack1")
	addDataServer(mds2Addr, "rack1")
	addDataServer(mds3Addr, "rack2")
	addDataServer(mds4Addr, "rack2")
	addDataServer(mds5Addr, "rack2")
	// add meta node
	addMetaServer(mms1Addr)
	addMetaServer(mms2Addr)
	addMetaServer(mms3Addr)
	addMetaServer(mms4Addr)
	addMetaServer(mms5Addr)
	time.Sleep(5 * time.Second)
	server.cluster.checkDataNodeHeartbeat()
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	server.cluster.startCheckAvailSpace()
	server.cluster.createVol(commonVolName, "cfs", "extent", 3, 100, 2)
	vol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		panic(err)
	}
	commonVol = vol
	fmt.Printf("vol[%v] has created\n", commonVol.Name)
	return server
}

func addDataServer(addr, rackName string) {
	mds := mocktest.NewMockDataServer(addr, rackName)
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

func processWithStatus(reqUrl string,statusCode int, t *testing.T) {
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
	if resp.StatusCode != statusCode {
		t.Errorf("status code[%v]", resp.StatusCode)
		return
	}
	return
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
	createMetaPartition(commonVol, t)
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
