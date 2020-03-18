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
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

type VolStatInfo struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

type DataPartitionResponse struct {
	PartitionID   uint64
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
}

type DataPartitionsView struct {
	DataPartitions []*DataPartitionResponse
}

func NewDataPartitionsView() (dataPartitionsView *DataPartitionsView) {
	dataPartitionsView = new(DataPartitionsView)
	dataPartitionsView.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
	MaxInodeID  uint64
	IsRecover   bool
}

type VolView struct {
	Name           string
	VolType        string
	Status         uint8
	MetaPartitions []*MetaPartitionView
	DataPartitions []*DataPartitionResponse
}

func NewVolView(name, volType string, status uint8) (view *VolView) {
	view = new(VolView)
	view.Name = name
	view.VolType = volType
	view.Status = status
	view.MetaPartitions = make([]*MetaPartitionView, 0)
	view.DataPartitions = make([]*DataPartitionResponse, 0)
	return
}

func NewMetaPartitionView(partitionID, start, end uint64, status int8) (mpView *MetaPartitionView) {
	mpView = new(MetaPartitionView)
	mpView.PartitionID = partitionID
	mpView.Start = start
	mpView.End = end
	mpView.Status = status
	mpView.Members = make([]string, 0)
	return
}

func (m *Master) getAllVols(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		err  error
	)

	vols := m.cluster.getAllVols()
	if body, err = json.Marshal(vols); err != nil {
		goto errDeal
	}
	io.WriteString(w, string(body))
	return

errDeal:
	logMsg := getReturnMessage("getAllVols", r.RemoteAddr, err.Error(), http.StatusBadRequest)
	HandleError(logMsg, err, http.StatusBadRequest, w)
	return
}

func (m *Master) getMetaPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		code = http.StatusBadRequest
		name string
		vol  *Vol
		err  error
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}
	w.Write(vol.getMpsCache())
	return
errDeal:
	logMsg := getReturnMessage("getMetaPartitions", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) getDataPartitions(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code = http.StatusBadRequest
		name string
		vol  *Vol
		err  error
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}

	if body, err = vol.getDataPartitionsView(m.cluster.getLiveDataNodesRate()); err != nil {
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getDataPartitions", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) getVol(w http.ResponseWriter, r *http.Request) {
	var (
		code = http.StatusBadRequest
		err  error
		name string
		vol  *Vol
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}

	if vol, err = m.cluster.getVol(name); err != nil {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}
	if vol.isAllMetaPartitionReadonly {
		err = fmt.Errorf("vol[%v] meta parititions are readonly", vol.Name)
		code = http.StatusInternalServerError
		goto errDeal
	}
	w.Write(vol.getViewCache())
	return
errDeal:
	logMsg := getReturnMessage("getVol", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) getVolStatInfo(w http.ResponseWriter, r *http.Request) {
	var (
		body []byte
		code = http.StatusBadRequest
		err  error
		name string
		vol  *Vol
	)
	if name, err = parseGetVolPara(r); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}
	if body, err = json.Marshal(volStat(vol)); err != nil {
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getVolStatInfo", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func volStat(vol *Vol) (stat *VolStatInfo) {
	stat = new(VolStatInfo)
	stat.Name = vol.Name
	stat.TotalSize = vol.Capacity * util.GB
	stat.UsedSize = vol.getTotalUsedSpace()
	if stat.UsedSize > stat.TotalSize {
		stat.UsedSize = stat.TotalSize
	}
	log.LogDebugf("total[%v],usedSize[%v]", stat.TotalSize, stat.UsedSize)
	return
}

func getMetaPartitionView(mp *MetaPartition) (mpView *MetaPartitionView) {
	mpView = NewMetaPartitionView(mp.PartitionID, mp.Start, mp.End, mp.Status)
	mp.Lock()
	defer mp.Unlock()
	for _, host := range mp.PersistenceHosts {
		mpView.Members = append(mpView.Members, host)
	}
	mr, err := mp.getLeaderMetaReplica()
	if err != nil {
		return
	}
	mpView.LeaderAddr = mr.Addr
	mpView.MaxInodeID = mp.MaxInodeID
	mpView.IsRecover = mp.IsRecover
	return
}

func (m *Master) getMetaPartition(w http.ResponseWriter, r *http.Request) {
	var (
		body        []byte
		code        = http.StatusBadRequest
		err         error
		partitionID uint64
		mp          *MetaPartition
	)
	if partitionID, err = parseGetMetaPartitionPara(r); err != nil {
		goto errDeal
	}
	if mp, err = m.cluster.getMetaPartitionByID(partitionID); err != nil {
		err = errors.Annotatef(MetaPartitionNotFound, "%v not found", partitionID)
		code = http.StatusNotFound
		goto errDeal
	}
	if body, err = mp.toJson(); err != nil {
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getMetaPartition", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func (m *Master) getToken(w http.ResponseWriter, r *http.Request) {
	var (
		body     []byte
		code     = http.StatusBadRequest
		err      error
		name     string
		token    string
		tokenObj *proto.Token
		vol      *Vol
	)
	if name, token, err = parseGetTokenPara(r); err != nil {
		goto errDeal
	}
	if vol, err = m.cluster.getVol(name); err != nil {
		err = errors.Annotatef(VolNotFound, "%v not found", name)
		code = http.StatusNotFound
		goto errDeal
	}

	if tokenObj, err = vol.getToken(token); err != nil {
		goto errDeal
	}

	if body, err = json.Marshal(tokenObj); err != nil {
		goto errDeal
	}
	w.Write(body)
	return
errDeal:
	logMsg := getReturnMessage("getToken", r.RemoteAddr, err.Error(), code)
	HandleError(logMsg, err, code, w)
	return
}

func parseGetTokenPara(r *http.Request) (name string, token string, err error) {
	r.ParseForm()
	if name, err = checkVolPara(r); err != nil {
		return
	}
	if token, err = checkTokenValue(r); err != nil {
		return
	}
	return
}

func checkTokenValue(r *http.Request) (token string, err error) {
	if token = r.FormValue(ParaToken); token == "" {
		err = paraNotFound(ParaToken)
		return
	}
	token, err = url.QueryUnescape(token)
	return
}

func parseGetMetaPartitionPara(r *http.Request) (partitionID uint64, err error) {
	r.ParseForm()
	if partitionID, err = checkMetaPartitionID(r); err != nil {
		return
	}
	return
}

func checkMetaPartitionID(r *http.Request) (partitionID uint64, err error) {
	var value string
	if value = r.FormValue(ParaId); value == "" {
		err = paraNotFound(ParaId)
		return
	}
	return strconv.ParseUint(value, 10, 64)
}

func parseGetVolPara(r *http.Request) (name string, err error) {
	r.ParseForm()
	return checkVolPara(r)
}

func checkAuthKeyPara(r *http.Request) (authKey string, err error) {
	if authKey = r.FormValue(ParaAuthKey); authKey == "" {
		err = paraNotFound(ParaAuthKey)
		return
	}
	return
}

func checkVolPara(r *http.Request) (name string, err error) {
	if name = r.FormValue(ParaName); name == "" {
		err = paraNotFound(ParaName)
		return
	}
	if !volNameRegexp.MatchString(name) {
		return "", errors.New("name can only be number and letters")
	}

	return
}
