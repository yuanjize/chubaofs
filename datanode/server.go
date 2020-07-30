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

package datanode

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/master"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/third_party/pool"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
	"runtime/debug"
	"syscall"
)

var (
	ErrStoreTypeMismatch        = errors.New("store type error")
	ErrPartitionNotExist        = errors.New("DataPartition not exists")
	ErrChunkOffsetMismatch      = errors.New("chunk offset not mismatch")
	ErrNoDiskForCreatePartition = errors.New("no disk for create DataPartition")
	ErrBadConfFile              = errors.New("bad config file")

	LocalIP      string
	gConnPool    = pool.NewConnectPool()
	ClusterID    string
	MasterHelper = util.NewMasterHelper()
)

const (
	GetIpFromMaster = master.AdminGetIp
	DefaultRackName = "huitian_rack1"
)

const (
	UmpModuleName = "dataNode"
)

const (
	ConfigKeyPort       = "port"       // int
	ConfigKeyClusterID  = "clusterID"  // string
	ConfigKeyMasterAddr = "masterAddr" // array
	ConfigKeyRack       = "rack"       // string
	ConfigKeyDisks      = "disks"      // array
)

type DataNode struct {
	space          *SpaceManager
	port           string
	rackName       string
	clusterId      string
	localIp        string
	localServeAddr string
	tcpListener    net.Listener
	stopC          chan bool
	state          uint32
	wg             sync.WaitGroup
}

func NewServer() *DataNode {
	return &DataNode{}
}

func (s *DataNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	if atomic.CompareAndSwapUint32(&s.state, Standby, Start) {
		defer func() {
			if err != nil {
				atomic.StoreUint32(&s.state, Standby)
			} else {
				atomic.StoreUint32(&s.state, Running)
			}
		}()
		if err = s.onStart(cfg); err != nil {
			return
		}
		s.wg.Add(1)
	}
	return
}

func (s *DataNode) Shutdown() {
	if atomic.CompareAndSwapUint32(&s.state, Running, Shutdown) {
		s.onShutdown()
		s.wg.Done()
		atomic.StoreUint32(&s.state, Stopped)
	}
}

func (s *DataNode) Sync() {
	if atomic.LoadUint32(&s.state) == Running {
		s.wg.Wait()
	}
}

func (s *DataNode) onStart(cfg *config.Config) (err error) {
	s.stopC = make(chan bool, 0)
	if err = s.parseConfig(cfg); err != nil {
		return
	}

	s.registerToMaster()
	debug.SetMaxThreads(20000)
	if err = s.startSpaceManager(cfg); err != nil {
		return
	}

	// check local partition compare with master ,if lack,then not start
	if err = s.checkLocalPartitionMatchWithMaster(); err != nil {
		fmt.Println(err)
		umpKey := fmt.Sprintf("%s_datanode", ClusterID)
		ump.Alarm(umpKey, err.Error())
		return
	}

	if err = s.startTcpService(); err != nil {
		return
	}
	go s.registerProfHandler()
	return
}

type DataNodeInfo struct {
	Addr                      string
	PersistenceDataPartitions []uint64
}

const (
	GetDataNode = "/dataNode/get"
)

func (s *DataNode) checkLocalPartitionMatchWithMaster() (err error) {
	persistenceDataPartitions,err:=s.getDataPartitionFromMaster()
	if err!=nil {
		return  err
	}
	if len(persistenceDataPartitions) == 0 {
		return
	}
	lackPartitions := make([]uint64, 0)
	for _, partitionID := range persistenceDataPartitions {
		dp := s.space.GetPartition(uint32(partitionID))
		if dp == nil {
			lackPartitions = append(lackPartitions, partitionID)
		}
	}
	if len(lackPartitions) == 0 {
		return
	}
	err = fmt.Errorf("LackPartitions %v on datanode %v,datanode cannot start", lackPartitions, s.localServeAddr)
	log.LogErrorf(err.Error())
	return
}

func (s *DataNode) onShutdown() {
	close(s.stopC)
	s.stopTcpService()
	return
}

func (s *DataNode) parseConfig(cfg *config.Config) (err error) {
	var (
		port       string
		regexpPort *regexp.Regexp
	)
	port = cfg.GetString(ConfigKeyPort)
	if regexpPort, err = regexp.Compile("^(\\d)+$"); err != nil {
		return
	}
	if !regexpPort.MatchString(port) {
		err = ErrBadConfFile
		return
	}
	s.port = port
	s.clusterId = cfg.GetString(ConfigKeyClusterID)
	if len(cfg.GetArray(ConfigKeyMasterAddr)) == 0 {
		return ErrBadConfFile
	}
	for _, ip := range cfg.GetArray(ConfigKeyMasterAddr) {
		MasterHelper.AddNode(ip.(string))
	}
	s.rackName = cfg.GetString(ConfigKeyRack)
	if s.rackName == "" {
		s.rackName = DefaultRackName
	}
	log.LogDebugf("action[parseConfig] load masterAddrs[%v].", MasterHelper.Nodes())
	log.LogDebugf("action[parseConfig] load port[%v].", s.port)
	log.LogDebugf("action[parseConfig] load clusterId[%v].", s.clusterId)
	log.LogDebugf("action[parseConfig] load rackName[%v].", s.rackName)
	return
}

func (s *DataNode) getDataPartitionFromMaster() (persistenceDataPartitions []uint64, err error) {
	params := make(map[string]string)
	params["addr"] = s.localServeAddr
	var data interface{}
	for i := 0; i < 3; i++ {
		data, err = MasterHelper.Request(http.MethodGet, GetDataNode, params, nil)
		if err != nil {
			log.LogErrorf("getDataPartitionFromMaster error %v", err)
			continue
		}
		break
	}
	dinfo := new(DataNodeInfo)
	if err = json.Unmarshal(data.([]byte), dinfo); err != nil {
		err = fmt.Errorf("getDataPartitionFromMaster jsonUnmarsh failed %v", err)
		log.LogErrorf(err.Error())
		return
	}
	persistenceDataPartitions = make([]uint64, len(dinfo.PersistenceDataPartitions))
	copy(persistenceDataPartitions, dinfo.PersistenceDataPartitions)
	return
}

func (s *DataNode) startSpaceManager(cfg *config.Config) (err error) {
	s.space = NewSpaceManager(s.rackName)
	if err != nil || len(strings.TrimSpace(s.port)) == 0 {
		err = ErrBadConfFile
		return
	}

	persistenceDataPartitions, err := s.getDataPartitionFromMaster()
	if err != nil {
		err = fmt.Errorf("cannot get dataPartition from master %v", err)
		return
	}

	var wg sync.WaitGroup
	for _, d := range cfg.GetArray(ConfigKeyDisks) {
		log.LogDebugf("action[startSpaceManager] load disk raw config[%v].", d)
		arr := strings.Split(d.(string), ":")
		if len(arr) != 3 {
			return ErrBadConfFile
		}
		path := arr[0]
		restSize, err := strconv.ParseUint(arr[1], 10, 64)
		if err != nil {
			return ErrBadConfFile
		}
		maxErr, err := strconv.Atoi(arr[2])
		if err != nil {
			return ErrBadConfFile
		}
		dataPartitionOnMaster := make([]uint64, len(persistenceDataPartitions))
		copy(dataPartitionOnMaster, persistenceDataPartitions)
		wg.Add(1)
		go func(wg *sync.WaitGroup, path string, restSize uint64, maxErrs int, persistenceDataPartitionsOnMaster []uint64) {
			defer wg.Done()
			s.space.LoadDisk(path, restSize, maxErrs, persistenceDataPartitionsOnMaster)
		}(&wg, path, restSize, maxErr, dataPartitionOnMaster)
	}
	wg.Wait()
	return nil
}

func (s *DataNode) registerToMaster() {
	var (
		err  error
		data []byte
	)
	// Get IP address and cluster ID from master.
	for {
		timer := time.NewTimer(0)
		select {
		case <-timer.C:
			data, err = MasterHelper.Request(http.MethodGet, GetIpFromMaster, nil, nil)
			masterAddr := MasterHelper.Leader()
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot get ip from master[%v] err[%v].",
					masterAddr, err)
				timer = time.NewTimer(5 * time.Second)
				continue
			}
			cInfo := new(proto.ClusterInfo)
			json.Unmarshal(data, cInfo)
			LocalIP = string(cInfo.Ip)
			s.clusterId = cInfo.Cluster
			ClusterID = s.clusterId
			s.localServeAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)
			if !util.IP(LocalIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip[%v] from master[%v].",
					LocalIP, masterAddr)
				timer = time.NewTimer(5 * time.Second)
				continue
			}
			// Register this data node to master.
			params := make(map[string]string)
			params["addr"] = fmt.Sprintf("%s:%v", LocalIP, s.port)
			data, err = MasterHelper.Request(http.MethodPost, master.AddDataNode, params, nil)
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot register this node to master[%] err[%v].",
					masterAddr, err)
				continue
			}
			return
		case <-s.stopC:
			timer.Stop()
			return
		}

	}

}

func (s *DataNode) registerProfHandler() {
	http.HandleFunc("/disks", s.apiGetDisk)
	http.HandleFunc("/partitions", s.apiGetPartitions)
	http.HandleFunc("/partition", s.apiGetPartition)
	http.HandleFunc("/extent", s.apiGetExtent)
	http.HandleFunc("/stats", s.apiGetStat)
}

func (s *DataNode) startTcpService() (err error) {
	log.LogInfo("Start: startTcpService")
	addr := fmt.Sprintf(":%v", s.port)
	l, err := net.Listen(NetType, addr)
	log.LogDebugf("action[startTcpService] listen %v address[%v].", NetType, addr)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	s.tcpListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[startTcpService] failed to accept, err:%s", err.Error())
				break
			}
			go s.serveConn(conn)
		}
	}(l)
	return
}

func (s *DataNode) stopTcpService() (err error) {
	if s.tcpListener != nil {
		s.tcpListener.Close()
		log.LogDebugf("action[stopTcpService] stop tcp service.")
	}
	return
}

func (s *DataNode) serveConn(conn net.Conn) {
	space := s.space
	space.Stats().AddConnection()
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	msgH := NewMsgHandler(c)
	go s.handleRequest(msgH)
	go s.writeToCli(msgH)
	s.dealClientPkg(msgH)

}

func (s *DataNode) dealClientPkg(msgH *MessageHandler) {
	var (
		err error
	)
	defer func() {
		msgH.Stop()
		msgH.exitedMu.Lock()
		if atomic.AddInt32(&msgH.exited, -1) == ReplHasExited {
			msgH.inConn.Close()
			msgH.cleanResource(s)
		}
		msgH.exitedMu.Unlock()
	}()
	for {
		if err = s.readFromCliAndDeal(msgH); err != nil {
			return
		}
	}

}

func (s *DataNode) addDiskErrs(partitionId uint32, err error, flag uint8) {
	if err == nil {
		return
	}
	dp := s.space.GetPartition(partitionId)
	if dp == nil {
		return
	}
	d := dp.Disk()
	if d == nil {
		return
	}
	if !IsDiskErr(err.Error()) {
		return
	}
	if flag == WriteFlag {
		d.addWriteErr()
	} else if flag == ReadFlag {
		d.addReadErr()
	}
	d.Status = proto.Unavaliable
	d.Lock()
	defer d.Unlock()
	for _, dp := range d.partitionMap {
		dp.partitionStatus = proto.Unavaliable
	}

}

func IsDiskErr(errMsg string) bool {
	if strings.Contains(errMsg, syscall.EIO.Error()) || strings.Contains(errMsg, syscall.EROFS.Error()) {
		return true
	}
	return false
}
