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
	"github.com/chubaofs/chubaofs/raftstore"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"net/http/httputil"
	"regexp"
	"strconv"
	"sync"
)

//config keys
const (
	ClusterName       = "clusterName"
	ID                = "id"
	IP                = "ip"
	Port              = "port"
	LogLevel          = "logLevel"
	WalDir            = "walDir"
	StoreDir          = "storeDir"
	GroupId           = 1
	UmpModuleName     = "master"
	CfgRetainLogs     = "retainLogs"
	cfgTickInterval   = "tickInterval"
	cfgElectionTick   = "electionTick"
	DefaultRetainLogs = 20000
)

var volNameRegexp *regexp.Regexp

type Master struct {
	id           uint64
	clusterName  string
	ip           string
	port         string
	walDir       string
	storeDir     string
	retainLogs   uint64
	tickInterval int
	electionTick int
	leaderInfo   *LeaderInfo
	config       *ClusterConfig
	cluster      *Cluster
	raftStore    raftstore.RaftStore
	fsm          *MetadataFsm
	partition    raftstore.Partition
	wg           sync.WaitGroup
	reverseProxy *httputil.ReverseProxy
	metaReady    bool
}

func NewServer() *Master {
	return &Master{}
}

func (m *Master) Start(cfg *config.Config) (err error) {
	m.config = NewClusterConfig()
	m.leaderInfo = &LeaderInfo{}
	m.reverseProxy = m.newReverseProxy()
	if err = m.checkConfig(cfg); err != nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	pattern := "^[a-zA-Z0-9_-]{3,256}$"
	volNameRegexp, err = regexp.Compile(pattern)
	if err != nil {
		log.LogError(err)
		return
	}
	if err = m.createRaftServer(); err != nil {
		log.LogError(errors.ErrorStack(err))
		return
	}
	m.cluster = newCluster(m.clusterName, m.leaderInfo, m.fsm, m.partition, m.config)
	m.cluster.retainLogs = m.retainLogs
	//m.loadMetadata()
	m.startHttpService()
	m.wg.Add(1)
	return nil
}

func (m *Master) Shutdown() {
	m.wg.Done()
}

func (m *Master) Sync() {
	m.wg.Wait()
}

func (m *Master) checkConfig(cfg *config.Config) (err error) {
	m.clusterName = cfg.GetString(ClusterName)
	m.ip = cfg.GetString(IP)
	m.port = cfg.GetString(Port)
	vfDelayCheckCrcSec := cfg.GetString(FileDelayCheckCrc)
	dataPartitionMissSec := cfg.GetString(DataPartitionMissSec)
	dataPartitionTimeOutSec := cfg.GetString(DataPartitionTimeOutSec)
	everyLoadDataPartitionCount := cfg.GetString(EveryLoadDataPartitionCount)
	replicaNum := cfg.GetString(ReplicaNum)
	m.walDir = cfg.GetString(WalDir)
	m.storeDir = cfg.GetString(StoreDir)
	m.tickInterval = int(cfg.GetFloat(cfgTickInterval))
	m.electionTick = int(cfg.GetFloat(cfgElectionTick))
	peerAddrs := cfg.GetString(CfgPeers)
	if m.retainLogs, err = strconv.ParseUint(cfg.GetString(CfgRetainLogs), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
	}
	if m.retainLogs <= 0 {
		m.retainLogs = DefaultRetainLogs
	}
	if m.tickInterval <= 300 {
		m.tickInterval = 500
	}
	if m.electionTick <= 3 {
		m.electionTick = 5
	}
	fmt.Println("retainLogs=", m.retainLogs)
	if err = m.config.parsePeers(peerAddrs); err != nil {
		return
	}

	if m.id, err = strconv.ParseUint(cfg.GetString(ID), 10, 64); err != nil {
		return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
	}

	if m.ip == "" || m.port == "" || m.walDir == "" || m.storeDir == "" || m.clusterName == "" {
		return fmt.Errorf("%v,err:%v", ErrBadConfFile, "one of (ip,port,walDir,storeDir,clusterName) is null")
	}

	if replicaNum != "" {
		if m.config.replicaNum, err = strconv.Atoi(replicaNum); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}

		if m.config.replicaNum > 10 {
			return fmt.Errorf("%v,replicaNum(%v) can't too large", ErrBadConfFile, m.config.replicaNum)
		}
	}

	if vfDelayCheckCrcSec != "" {
		if m.config.FileDelayCheckCrcSec, err = strconv.ParseInt(vfDelayCheckCrcSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}

	if dataPartitionMissSec != "" {
		if m.config.DataPartitionMissSec, err = strconv.ParseInt(dataPartitionMissSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if dataPartitionTimeOutSec != "" {
		if m.config.DataPartitionTimeOutSec, err = strconv.ParseInt(dataPartitionTimeOutSec, 10, 0); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if everyLoadDataPartitionCount != "" {
		if m.config.everyLoadDataPartitionCount, err = strconv.Atoi(everyLoadDataPartitionCount); err != nil {
			return fmt.Errorf("%v,err:%v", ErrBadConfFile, err.Error())
		}
	}
	if m.config.everyLoadDataPartitionCount <= 40 {
		m.config.everyLoadDataPartitionCount = 40
	}

	return
}

func (m *Master) createRaftServer() (err error) {
	raftCfg := &raftstore.Config{
		ClusterID:    m.clusterName + "_master",
		NodeID:       m.id,
		WalPath:      m.walDir,
		RetainLogs:   m.retainLogs,
		TickInterval: m.tickInterval,
		ElectionTick: m.electionTick,
	}
	if m.raftStore, err = raftstore.NewRaftStore(raftCfg); err != nil {
		return errors.Annotatef(err, "NewRaftStore failed! id[%v] walPath[%v]", m.id, m.walDir)
	}
	fsm := newMetadataFsm(m.storeDir, m.retainLogs, m.raftStore.RaftServer())
	fsm.RegisterLeaderChangeHandler(m.handleLeaderChange)
	fsm.RegisterPeerChangeHandler(m.handlePeerChange)
	fsm.RegisterApplySnapshotHandler(m.handleApplySnapshot)
	fsm.restore()
	m.fsm = fsm
	fmt.Println(m.config.peers, m.tickInterval, m.electionTick)
	partitionCfg := &raftstore.PartitionConfig{
		ID:      GroupId,
		Peers:   m.config.peers,
		Applied: fsm.applied,
		SM:      fsm,
	}
	if m.partition, err = m.raftStore.CreatePartition(partitionCfg); err != nil {
		return errors.Annotate(err, "CreatePartition failed")
	}
	return
}
