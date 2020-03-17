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

package meta

import (
	"encoding/json"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/chubaofs/chubaofs/util/log"
)

const (
	MaxSendToMaster = 3
)

var (
	NotLeader    = errors.New("NotLeader")
	InvalidToken = errors.New("Invalid Token")
)

type VolumeView struct {
	VolName        string
	MetaPartitions []*MetaPartition
}

type ClusterInfo struct {
	Cluster string
	Ip      string
}

type VolStatInfo struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

type SimpleVolView struct {
	Name        string
	EnableToken bool
	Tokens      map[string]*proto.Token
}

// VolName view managements
//
func (mw *MetaWrapper) PullVolumeView() (*VolumeView, error) {
	params := make(map[string]string)
	params["name"] = mw.volname
	body, err := mw.master.Request(http.MethodPost, MetaPartitionViewURL, params, nil)
	if err != nil {
		log.LogWarnf("PullVolumeView request: err(%v)", err)
		return nil, err
	}

	view := new(VolumeView)
	if err = json.Unmarshal(body, view); err != nil {
		log.LogWarnf("PullVolumeView unmarshal: err(%v) body(%v)", err, string(body))
		return nil, err
	}
	return view, nil
}

func (mw *MetaWrapper) PullMetaPartitions() ([]*MetaPartition, error) {
	params := make(map[string]string)
	params["name"] = mw.volname
	body, err := mw.master.Request(http.MethodPost, ForceUpdateMetaPartitionsURL, params, nil)
	if err != nil {
		log.LogWarnf("PullMetaPartitions request: err(%v)", err)
		return nil, err
	}

	var mpview []*MetaPartition
	if err = json.Unmarshal(body, &mpview); err != nil {
		log.LogWarnf("PullMetaPartitions unmarshal: err(%v) body(%v)", err, string(body))
		return nil, err
	}
	return mpview, nil
}

func (mw *MetaWrapper) PullMetaPartition(id uint64) (*MetaPartition, error) {
	params := make(map[string]string)
	params["name"] = mw.volname
	params["id"] = strconv.FormatUint(id, 10)
	body, err := mw.master.Request(http.MethodPost, UpdateMetaPartitionURL, params, nil)
	if err != nil {
		log.LogWarnf("PullMetaPartition request: err(%v)", err)
		return nil, err
	}

	var mp *MetaPartition
	if err = json.Unmarshal(body, &mp); err != nil {
		log.LogWarnf("PullMetaPartition unmarshal: err(%v) body(%v)", err, string(body))
		return nil, err
	}
	return mp, nil
}

func (mw *MetaWrapper) UpdateClusterInfo() error {
	body, err := mw.master.Request(http.MethodPost, GetClusterInfoURL, nil, nil)
	if err != nil {
		log.LogWarnf("UpdateClusterInfo request: err(%v)", err)
		return err
	}

	info := new(ClusterInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogWarnf("UpdateClusterInfo unmarshal: err(%v)", err)
		return err
	}
	log.LogInfof("ClusterInfo: %v", *info)
	mw.cluster = info.Cluster
	mw.localIP = info.Ip
	return nil
}

func (mw *MetaWrapper) QueryTokenType(volume, token string) (int8, error) {
	params := make(map[string]string)
	params["name"] = mw.volname
	body, err := mw.master.Request(http.MethodGet, AdminGetVolURL, params, nil)
	if err != nil {
		log.LogWarnf("admin/getVol request: err(%v)", err)
		return proto.ReadOnlyToken, err
	}

	volView := new(SimpleVolView)
	if err = json.Unmarshal(body, volView); err != nil {
		log.LogWarnf("SimpleVolView unmarshal: err(%v) body(%v)", err, string(body))
		return proto.ReadOnlyToken, err
	}

	log.LogInfof("SimpleVolView: %v", volView)
	if !volView.EnableToken {
		return proto.ReadWriteToken, nil
	}

	tokenObj, ok := volView.Tokens[token]
	if !ok {
		return proto.ReadOnlyToken, InvalidToken
	}

	return tokenObj.TokenType, nil
}

func (mw *MetaWrapper) UpdateVolStatInfo() error {
	params := make(map[string]string)
	params["name"] = mw.volname
	body, err := mw.master.Request(http.MethodPost, GetVolStatURL, params, nil)
	if err != nil {
		log.LogWarnf("UpdateVolStatInfo request: err(%v)", err)
		return err
	}

	info := new(VolStatInfo)
	if err = json.Unmarshal(body, info); err != nil {
		log.LogWarnf("UpdateVolStatInfo unmarshal: err(%v)", err)
		return err
	}
	log.LogInfof("UpdateVolStatInfo: info(%v)", *info)
	atomic.StoreUint64(&mw.totalSize, info.TotalSize)
	atomic.StoreUint64(&mw.usedSize, info.UsedSize)
	return nil
}

func (mw *MetaWrapper) UpdateMetaPartitions() error {
	nv, err := mw.PullVolumeView()
	if err != nil {
		return err
	}

	for _, mp := range nv.MetaPartitions {
		mw.replaceOrInsertPartition(mp)
		log.LogInfof("UpdateMetaPartition: mp(%v)", mp)
	}
	return nil
}

func (mw *MetaWrapper) forceUpdateMetaPartitions() error {
	// Only one forceUpdateMetaPartition is allowed in a specific period of time.
	if ok := mw.forceUpdateLimit.AllowN(time.Now(), MinForceUpdateMetaPartitionsInterval); !ok {
		return errors.New("Force update meta partitions throttled!")
	}

	mps, err := mw.PullMetaPartitions()
	if err != nil {
		return err
	}

	for _, mp := range mps {
		mw.replaceOrInsertPartition(mp)
		log.LogInfof("forceUpdateMetaPartitions: mp(%v)", mp)
	}
	return nil
}

// Should be protected by partMutex, otherwise the caller might not be signaled.
func (mw *MetaWrapper) triggerAndWaitForceUpdate() {
	mw.partMutex.Lock()
	select {
	case mw.forceUpdate <- struct{}{}:
	default:
	}
	mw.partCond.Wait()
	mw.partMutex.Unlock()
}

func (mw *MetaWrapper) refresh() {
	t := time.NewTimer(RefreshMetaPartitionsInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			mw.UpdateMetaPartitions()
			mw.UpdateVolStatInfo()
			t.Reset(RefreshMetaPartitionsInterval)
		case <-mw.forceUpdate:
			mw.partMutex.Lock()
			if err := mw.forceUpdateMetaPartitions(); err == nil {
				t.Reset(RefreshMetaPartitionsInterval)
			}
			mw.partMutex.Unlock()
			mw.partCond.Broadcast()
		}
	}
}
