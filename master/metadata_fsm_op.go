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
	"encoding/json"
	"fmt"
	bsProto "github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/tiglabs/raft/proto"
	"strconv"
	"strings"
)

const (
	OpSyncAddMetaNode          uint32 = 0x01
	OpSyncAddDataNode          uint32 = 0x02
	OpSyncAddDataPartition     uint32 = 0x03
	OpSyncAddVol               uint32 = 0x04
	OpSyncAddMetaPartition     uint32 = 0x05
	OpSyncUpdateDataPartition  uint32 = 0x06
	OpSyncUpdateMetaPartition  uint32 = 0x07
	OpSyncDeleteDataNode       uint32 = 0x08
	OpSyncDeleteMetaNode       uint32 = 0x09
	OpSyncAllocDataPartitionID uint32 = 0x0A
	OpSyncAllocMetaPartitionID uint32 = 0x0B
	OpSyncAllocMetaNodeID      uint32 = 0x0C
	OpSyncPutCluster           uint32 = 0x0D
	OpSyncUpdateVol            uint32 = 0x0E
	OpSyncDeleteVol            uint32 = 0x0F
	OpSyncDeleteDataPartition  uint32 = 0x10
	OpSyncDeleteMetaPartition  uint32 = 0x11
	OpSyncAddToken             uint32 = 0x20
	OpSyncDelToken             uint32 = 0x21
	OpSyncUpdateToken          uint32 = 0x22
)

const (
	KeySeparator         = "#"
	MetaNodeAcronym      = "mn"
	DataNodeAcronym      = "dn"
	DataPartitionAcronym = "dp"
	MetaPartitionAcronym = "mp"
	VolAcronym           = "vol"
	ClusterAcronym       = "c"
	TokenAcronym         = "t"
	MetaNodePrefix       = KeySeparator + MetaNodeAcronym + KeySeparator
	DataNodePrefix       = KeySeparator + DataNodeAcronym + KeySeparator
	DataPartitionPrefix  = KeySeparator + DataPartitionAcronym + KeySeparator
	VolPrefix            = KeySeparator + VolAcronym + KeySeparator
	MetaPartitionPrefix  = KeySeparator + MetaPartitionAcronym + KeySeparator
	ClusterPrefix        = KeySeparator + ClusterAcronym + KeySeparator
	TokenPrefix          = KeySeparator + TokenAcronym + KeySeparator
)

type MetaPartitionValue struct {
	PartitionID uint64
	ReplicaNum  uint8
	Start       uint64
	End         uint64
	Hosts       string
	Peers       []bsProto.Peer
	IsManual    bool
}

func newMetaPartitionValue(mp *MetaPartition) (mpv *MetaPartitionValue) {
	mpv = &MetaPartitionValue{
		PartitionID: mp.PartitionID,
		ReplicaNum:  mp.ReplicaNum,
		Start:       mp.Start,
		End:         mp.End,
		Hosts:       mp.hostsToString(),
		Peers:       mp.Peers,
		IsManual:    mp.IsManual,
	}
	return
}

type replicaValue struct {
	Addr     string
	DiskPath string
}

type DataPartitionValue struct {
	PartitionID   uint64
	ReplicaNum    uint8
	Hosts         string
	PartitionType string
	IsManual      bool
	Replicas      []*replicaValue
}

func newDataPartitionValue(dp *DataPartition) (dpv *DataPartitionValue) {
	dpv = &DataPartitionValue{
		PartitionID:   dp.PartitionID,
		ReplicaNum:    dp.ReplicaNum,
		Hosts:         dp.HostsToString(),
		PartitionType: dp.PartitionType,
		IsManual:      dp.IsManual,
		Replicas:      make([]*replicaValue, 0),
	}
	for _, replica := range dp.Replicas {
		rv := &replicaValue{Addr: replica.Addr, DiskPath: replica.DiskPath}
		dpv.Replicas = append(dpv.Replicas, rv)
	}
	return
}

type VolValue struct {
	VolType    string
	ReplicaNum uint8
	Status     uint8
	Capacity   uint64
	Owner      string
}

func newVolValue(vol *Vol) (vv *VolValue) {
	vv = &VolValue{
		VolType:    vol.VolType,
		ReplicaNum: vol.dpReplicaNum,
		Status:     vol.Status,
		Capacity:   vol.Capacity,
		Owner:      vol.Owner,
	}
	return
}

type TokenValue struct {
	VolName   string
	Value     string
	TokenType int8
}

func newTokenValue(token *bsProto.Token) (tv *TokenValue) {
	tv = &TokenValue{
		TokenType: token.TokenType,
		Value:     token.Value,
		VolName:   token.VolName,
	}
	return
}

type Metadata struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

func (m *Metadata) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *Metadata) Unmarshal(data []byte) (err error) {
	return json.Unmarshal(data, m)
}

func (m *Metadata) setOpType() {
	keyArr := strings.Split(m.K, KeySeparator)

	switch keyArr[0] {
	case MaxDataPartitionIDKey:
		m.Op = OpSyncAllocDataPartitionID
		return
	case MaxMetaPartitionIDKey:
		m.Op = OpSyncAllocMetaPartitionID
		return
	case MaxMetaNodeIDKey:
		m.Op = OpSyncAllocMetaNodeID
		return
	}
	if len(keyArr) < 2 {
		log.LogWarnf("action[setOpType] invalid length[%v]", keyArr)
		return
	}
	switch keyArr[1] {
	case MetaNodeAcronym:
		m.Op = OpSyncAddMetaNode
	case DataNodeAcronym:
		m.Op = OpSyncAddDataNode
	case DataPartitionAcronym:
		m.Op = OpSyncAddDataPartition
	case MetaPartitionAcronym:
		m.Op = OpSyncAddMetaPartition
	case VolAcronym:
		m.Op = OpSyncAddVol
	case ClusterAcronym:
		m.Op = OpSyncPutCluster
	case TokenAcronym:
		m.Op = OpSyncAddToken
	default:
		log.LogWarnf("action[setOpType] unknown opCode[%v]", keyArr[1])
	}
}

func (c *Cluster) syncDeleteToken(token *bsProto.Token) (err error) {
	return c.syncPutTokenInfo(OpSyncAddToken, token)
}

func (c *Cluster) syncAddToken(token *bsProto.Token) (err error) {
	return c.syncPutTokenInfo(OpSyncDelToken, token)
}

func (c *Cluster) syncUpdateToken(token *bsProto.Token) (err error) {
	return c.syncPutTokenInfo(OpSyncUpdateToken, token)
}

func (c *Cluster) syncPutTokenInfo(opType uint32, token *bsProto.Token) (err error) {
	metadata := new(Metadata)
	metadata.Op = opType
	metadata.K = TokenPrefix + token.VolName + KeySeparator + token.Value
	tv := newTokenValue(token)
	metadata.V, err = json.Marshal(tv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) syncPutCluster() (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncPutCluster
	metadata.K = ClusterPrefix + c.Name + KeySeparator + strconv.FormatBool(c.compactStatus)
	return c.submit(metadata)
}

//key=#vg#volName#partitionID,value=json.Marshal(DataPartitionValue)
func (c *Cluster) syncAddDataPartition(volName string, dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncAddDataPartition, volName, dp)
}

func (c *Cluster) syncUpdateDataPartition(volName string, dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncUpdateDataPartition, volName, dp)
}

func (c *Cluster) syncDeleteDataPartition(volName string, dp *DataPartition) (err error) {
	return c.putDataPartitionInfo(OpSyncDeleteDataPartition, volName, dp)
}

func (c *Cluster) putDataPartitionInfo(opType uint32, volName string, dp *DataPartition) (err error) {
	metadata := new(Metadata)
	metadata.Op = opType
	metadata.K = DataPartitionPrefix + volName + KeySeparator + strconv.FormatUint(dp.PartitionID, 10)
	dpv := newDataPartitionValue(dp)
	metadata.V, err = json.Marshal(dpv)
	if err != nil {
		return
	}
	return c.submit(metadata)
}

func (c *Cluster) submit(metadata *Metadata) (err error) {
	cmd, err := metadata.Marshal()
	if err != nil {
		return errors.New(err.Error())
	}
	if _, err = c.partition.Submit(cmd); err != nil {
		msg := fmt.Sprintf("action[metadata_submit] err:%v", err.Error())
		return errors.New(msg)
	}
	return
}

//key=#vol#volName,value=json.Marshal(vv)
func (c *Cluster) syncAddVol(vol *Vol) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddVol
	metadata.K = VolPrefix + vol.Name
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) syncUpdateVol(vol *Vol) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncUpdateVol
	metadata.K = VolPrefix + vol.Name
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

func (c *Cluster) syncDeleteVol(vol *Vol) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncDeleteVol
	metadata.K = VolPrefix + vol.Name
	vv := newVolValue(vol)
	if metadata.V, err = json.Marshal(vv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

////key=#mp#volName#metaPartitionID,value=json.Marshal(MetaPartitionValue)
func (c *Cluster) syncAddMetaPartition(volName string, mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncAddMetaPartition, volName, mp)
}

func (c *Cluster) syncUpdateMetaPartition(volName string, mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncUpdateMetaPartition, volName, mp)
}

func (c *Cluster) syncDeleteMetaPartition(volName string, mp *MetaPartition) (err error) {
	return c.putMetaPartitionInfo(OpSyncDeleteMetaPartition, volName, mp)
}

func (c *Cluster) putMetaPartitionInfo(opType uint32, volName string, mp *MetaPartition) (err error) {
	metadata := new(Metadata)
	metadata.Op = opType
	partitionID := strconv.FormatUint(mp.PartitionID, 10)
	metadata.K = MetaPartitionPrefix + volName + KeySeparator + partitionID
	mpv := newMetaPartitionValue(mp)
	if metadata.V, err = json.Marshal(mpv); err != nil {
		return errors.New(err.Error())
	}
	return c.submit(metadata)
}

//key=#mn#id#addr,value = nil
func (c *Cluster) syncAddMetaNode(metaNode *MetaNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddMetaNode
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + KeySeparator + metaNode.Addr
	return c.submit(metadata)
}

func (c *Cluster) syncDeleteMetaNode(metaNode *MetaNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncDeleteMetaNode
	metadata.K = MetaNodePrefix + strconv.FormatUint(metaNode.ID, 10) + KeySeparator + metaNode.Addr
	return c.submit(metadata)
}

//key=#dn#httpAddr,value = nil
func (c *Cluster) syncAddDataNode(dataNode *DataNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncAddDataNode
	metadata.K = DataNodePrefix + dataNode.Addr
	return c.submit(metadata)
}

func (c *Cluster) syncDeleteDataNode(dataNode *DataNode) (err error) {
	metadata := new(Metadata)
	metadata.Op = OpSyncDeleteDataNode
	metadata.K = DataNodePrefix + dataNode.Addr
	return c.submit(metadata)
}

func (c *Cluster) addRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfAddNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[addRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) removeRaftNode(nodeID uint64, addr string) (err error) {
	peer := proto.Peer{ID: nodeID}
	_, err = c.partition.ChangeMember(proto.ConfRemoveNode, peer, []byte(addr))
	if err != nil {
		return errors.New("action[removeRaftNode] error: " + err.Error())
	}
	return nil
}

func (c *Cluster) decodeDataPartitionKey(key string) (acronym, volName string) {
	return c.decodeAcronymAndNsName(key)
}

func (c *Cluster) decodeMetaPartitionKey(key string) (acronym, volName string) {
	return c.decodeAcronymAndNsName(key)
}

func (c *Cluster) decodeVolKey(key string) (acronym, volName string, err error) {
	arr := strings.Split(key, KeySeparator)
	acronym = arr[1]
	volName = arr[2]
	return
}

func (c *Cluster) decodeAcronymAndNsName(key string) (acronym, volName string) {
	arr := strings.Split(key, KeySeparator)
	acronym = arr[1]
	volName = arr[2]
	return
}

func (c *Cluster) loadCompactStatus() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(ClusterPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		keys := strings.Split(string(encodedKey.Data()), KeySeparator)
		var status bool
		status, err = strconv.ParseBool(keys[3])
		if err != nil {
			return errors.Annotatef(err, "action[loadCompactStatus] failed,err:%v", err)
		}
		c.compactStatus = status
		encodedKey.Free()
		log.LogInfof("action[loadCompactStatus] cluster[%v] status[%v]", c.Name, c.compactStatus)
	}
	return
}

func (c *Cluster) loadDataNodes() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(DataNodePrefix)
	it.Seek(prefixKey)

	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		keys := strings.Split(string(encodedKey.Data()), KeySeparator)
		dataNode := NewDataNode(keys[2], c.Name)
		c.dataNodes.Store(dataNode.Addr, dataNode)
		encodedKey.Free()
		log.LogInfof("action[loadDataNodes],dataNode[%v]", dataNode.Addr)
	}
	return
}

func (c *Cluster) decodeMetaNodeKey(key string) (nodeID uint64, addr string, err error) {
	keys := strings.Split(key, KeySeparator)
	addr = keys[3]
	nodeID, err = strconv.ParseUint(keys[2], 10, 64)
	return
}

func (c *Cluster) loadMetaNodes() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(MetaNodePrefix)
	it.Seek(prefixKey)

	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		nodeID, addr, err1 := c.decodeMetaNodeKey(string(encodedKey.Data()))
		if err1 != nil {
			err = fmt.Errorf("action[loadMetaNodes] err:%v", err1.Error())
			return err
		}
		metaNode := NewMetaNode(addr, c.Name)
		metaNode.ID = nodeID
		c.metaNodes.Store(addr, metaNode)
		encodedKey.Free()
		log.LogInfof("action[loadMetaNodes],metaNode[%v]", metaNode.Addr)
	}
	return
}

func (c *Cluster) loadVols() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(VolPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		_, volName, err1 := c.decodeVolKey(string(encodedKey.Data()))
		if err1 != nil {
			err = fmt.Errorf("action[loadVols] err:%v", err1.Error())
			return err
		}
		vv := &VolValue{}
		if err = json.Unmarshal(encodedValue.Data(), vv); err != nil {
			err = fmt.Errorf("action[loadVols],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		vol := NewVol(volName, vv.Owner, vv.VolType, vv.ReplicaNum, vv.Capacity)
		vol.Status = vv.Status
		c.putVol(vol)
		encodedKey.Free()
		log.LogInfof("action[loadVols],vol[%v]", vol)
	}
	return
}

func (c *Cluster) loadTokens() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(TokenPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		tv := &TokenValue{}
		if err = json.Unmarshal(encodedValue.Data(), tv); err != nil {
			err = fmt.Errorf("action[loadTokens],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		vol, err1 := c.getVol(tv.VolName)
		if err1 != nil {
			// if vol not found,record log and continue
			log.LogErrorf("action[loadTokens] err:%v", err1.Error())
			continue
		}
		token := &bsProto.Token{VolName: tv.VolName, TokenType: tv.TokenType, Value: tv.Value}
		vol.putToken(token)
		encodedKey.Free()
		encodedValue.Free()
		log.LogInfof("action[loadTokens],vol[%v],token[%v]", vol.Name, token.Value)
	}
	return
}

func (c *Cluster) loadMetaPartitions() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(MetaPartitionPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		_, volName := c.decodeMetaPartitionKey(string(encodedKey.Data()))
		vol, err1 := c.getVol(volName)
		if err1 != nil {
			// if vol not found,record log and continue
			//err = fmt.Errorf("action[loadMetaPartitions] err:%v", err1.Error())
			log.LogErrorf("action[loadMetaPartitions] err:%v", err1.Error())
			continue
		}
		mpv := &MetaPartitionValue{}
		if err = json.Unmarshal(encodedValue.Data(), mpv); err != nil {
			err = fmt.Errorf("action[decodeMetaPartitionValue],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		mp := NewMetaPartition(mpv.PartitionID, mpv.Start, mpv.End, vol.mpReplicaNum, volName)
		mp.Lock()
		mp.setPersistenceHosts(strings.Split(mpv.Hosts, UnderlineSeparator))
		mp.setPeers(mpv.Peers)
		mp.IsManual = mpv.IsManual
		mp.Unlock()
		vol.AddMetaPartition(mp)
		encodedKey.Free()
		encodedValue.Free()
		log.LogInfof("action[loadMetaPartitions],vol[%v],mp[%v]", vol.Name, mp.PartitionID)
	}
	return
}

func (c *Cluster) loadDataPartitions() (err error) {
	snapshot := c.fsm.store.RocksDBSnapshot()
	it := c.fsm.store.Iterator(snapshot)
	defer func() {
		it.Close()
		c.fsm.store.ReleaseSnapshot(snapshot)
	}()
	prefixKey := []byte(DataPartitionPrefix)
	it.Seek(prefixKey)
	for ; it.ValidForPrefix(prefixKey); it.Next() {
		encodedKey := it.Key()
		encodedValue := it.Value()
		_, volName := c.decodeDataPartitionKey(string(encodedKey.Data()))
		vol, err1 := c.getVol(volName)
		if err1 != nil {
			// if vol not found,record log and continue
			//err = fmt.Errorf("action[loadDataPartitions] err:%v", err1.Error())
			log.LogErrorf("action[loadDataPartitions] err:%v", err1.Error())
			continue
		}
		dpv := &DataPartitionValue{}
		if err = json.Unmarshal(encodedValue.Data(), dpv); err != nil {
			err = fmt.Errorf("action[decodeDataPartitionValue],value:%v,err:%v", encodedValue.Data(), err)
			return err
		}
		dp := newDataPartition(dpv.PartitionID, dpv.ReplicaNum, dpv.PartitionType, volName)
		dp.Lock()
		dp.PersistenceHosts = strings.Split(dpv.Hosts, UnderlineSeparator)
		dp.IsManual = dpv.IsManual
		for _, rv := range dpv.Replicas {
			dp.afterCreation(rv.Addr, rv.DiskPath, c)
		}
		dp.Unlock()
		vol.dataPartitions.putDataPartition(dp)
		encodedKey.Free()
		encodedValue.Free()
		log.LogInfof("action[loadDataPartitions],vol[%v],dp[%v]", vol.Name, dp.PartitionID)
	}
	return
}
