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

package metanode

import (
	"encoding/json"
	"fmt"
	"net"
	"os"

	"bytes"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	raftProto "github.com/tiglabs/raft/proto"
)

const (
	MaxUsedMemFactor = 1.1
)

func (m *metaManager) opMasterHeartbeat(conn net.Conn, p *Packet) (err error) {
	// For ack to master
	m.responseAckOKToMaster(conn, p)
	var (
		req       = &proto.HeartBeatRequest{}
		resp      = &proto.MetaNodeHeartbeatResponse{}
		adminTask = &proto.AdminTask{}
		reqData   []byte
	)
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		resp.Status = proto.TaskFail
		resp.Result = err.Error()
		goto end
	}
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		resp.Status = proto.TaskFail
		resp.Result = err.Error()
		goto end
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		resp.Status = proto.TaskFail
		resp.Result = err.Error()
		goto end
	}
	if curMasterAddr != req.MasterAddr {
		curMasterAddr = req.MasterAddr
	}
	// collect used info
	// machine mem total and used
	resp.Total, _, err = util.GetMemInfo()
	if err != nil {
		adminTask.Status = proto.TaskFail
		goto end
	}
	if configTotalMem != 0 {
		resp.Total = uint64(configTotalMem)
	}
	resp.Used, err = util.GetProcessMemory(os.Getpid())
	if err != nil {
		adminTask.Status = proto.TaskFail
		goto end
	}
	log.LogInfof("Action(GetMemInfo) Total(%v) Used(%v)", resp.Total, resp.Used)
	// every partition used
	m.Range(func(id uint64, partition MetaPartition) bool {
		mConf := partition.GetBaseConfig()
		mpr := &proto.MetaPartitionReport{
			PartitionID: mConf.PartitionId,
			Start:       mConf.Start,
			End:         mConf.End,
			Status:      proto.ReadWrite,
			MaxInodeID:  mConf.Cursor,
			VolName:     mConf.VolName,
		}
		addr, isLeader := partition.IsLeader()
		if addr == "" {
			mpr.Status = proto.NoLeader
		}
		mpr.IsLeader = isLeader
		if mConf.Cursor >= mConf.End {
			mpr.Status = proto.ReadOnly
		}
		mpr.InodeCount=partition.GetInodeCount()
		mpr.DentryCount=partition.GetDentryCount()
		resp.MetaPartitionInfo = append(resp.MetaPartitionInfo, mpr)
		return true
	})
	resp.Status = proto.TaskSuccess
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.respondToMaster(adminTask)
	data, _ := json.Marshal(resp)
	log.LogInfof("[opMasterHeartbeat] req:%v; respAdminTask: %v, resp: %v",
		req, adminTask, string(data))
	if resp.Status != proto.TaskSuccess {
		log.LogErrorf(fmt.Sprintf("opMasterHeartbeat failed errResp(%v)", string(data)))
	}
	return
}

// Handle OpCreateMetaRange
func (m *metaManager) opCreateMetaPartition(conn net.Conn, p *Packet) (err error) {
	defer func() {
		var buf []byte
		status := proto.OpOk
		if err != nil {
			status = proto.OpErr
			buf = []byte(err.Error())
		}
		p.PackErrorWithBody(status, buf)
		m.respondToClient(conn, p)
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opCreateMetaPartition failed (%v)", err))
		}
	}()
	// GetConnect task from packet.
	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		err = errors.Errorf("[opCreateMetaPartition]: Unmarshal AdminTask"+
			" struct: %s", err.Error())
		return
	}
	log.LogDebugf("[opCreateMetaPartition] [remoteAddr=%s]accept a from"+
		" master message: %v", conn.RemoteAddr(), adminTask)
	// Marshal request body.
	requestJson, err := json.Marshal(adminTask.Request)
	if err != nil {
		err = errors.Errorf("[opCreateMetaPartition]: Marshal AdminTask."+
			"Request: %s", err.Error())
		return
	}
	// Unmarshal request to entity
	req := &proto.CreateMetaPartitionRequest{}
	if err = json.Unmarshal(requestJson, req); err != nil {
		err = errors.Errorf("[opCreateMetaPartition]: Unmarshal AdminTask."+
			"Request to CreateMetaPartitionRequest: %s", err.Error())
		return
	}
	// Create new  metaPartition.
	if err = m.createPartition(req.PartitionID, req.VolName, req.Start, req.End,
		req.Members); err != nil {
		err = errors.Errorf("[opCreateMetaPartition]->%s; request message: %v",
			err.Error(), adminTask.Request)
		return
	}
	log.LogInfof("[opCreateMetaPartition] req:%v; resp: %v", req, adminTask)
	return
}

// Handle OpCreate inode
func (m *metaManager) opCreateInode(conn net.Conn, p *Packet) (err error) {
	req := &CreateInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opCreateInode request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateInode(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opCreateInode] req:%v; resp: %v, body: %s remote:%v", req, p.GetResultMesg(), p.Data, conn.RemoteAddr().String())
	return
}

func (m *metaManager) opMetaLinkInode(conn net.Conn, p *Packet) (err error) {
	req := &LinkInodeReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opMetaLinkInode request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateLinkInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaLinkInode] req: %v, resp: %v, body: %s", req, p.GetResultMesg(), p.Data)
	return
}

// Handle OpCreate
func (m *metaManager) opCreateDentry(conn net.Conn, p *Packet) (err error) {
	req := &CreateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opCreateDentry request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.CreateDentry(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opCreateDentry] req:%v; resp: %v, body: %s", req, p.GetResultMesg(), p.Data)
	return
}

// Handle OpDelete Dentry
func (m *metaManager) opDeleteDentry(conn net.Conn, p *Packet) (err error) {
	req := &DeleteDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opDeleteDentry request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteDentry(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opDeleteDentry] req:%v; resp: %v, body: %s,remote :%v", req,
		p.GetResultMesg(), p.Data, p.Data, conn.RemoteAddr().String())
	return
}

func (m *metaManager) opUpdateDentry(conn net.Conn, p *Packet) (err error) {
	req := &UpdateDentryReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opUpdateDentry request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.UpdateDentry(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opUpdateDentry] req: %v; resp: %v, body: %s,remote :%v",
		req, p.GetResultMesg(), p.Data, conn.RemoteAddr().String())
	return
}

func (m *metaManager) opDeleteInode(conn net.Conn, p *Packet) (err error) {
	req := &DeleteInoReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opDeleteInode request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.DeleteInode(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opDeleteInode] req:%v; resp: %v, body: %s,remote :%v", req,
		p.GetResultMesg(), p.Data, conn.RemoteAddr().String())
	return
}

// Handle OpReadDir
func (m *metaManager) opReadDir(conn net.Conn, p *Packet) (err error) {
	req := &proto.ReadDirRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opReadDir request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ReadDir(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opReadDir] req:%v; resp: %v, body: %s", req,
		p.GetResultMesg(), p.Data)
	return
}

// Handle OpOpen
func (m *metaManager) opOpen(conn net.Conn, p *Packet) (err error) {
	req := &proto.OpenRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opOpen request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if ok := m.serveProxy(conn, mp, p); !ok {
		return
	}
	err = mp.Open(req, p)
	// Reply operation result to client though TCP connection.
	m.respondToClient(conn, p)
	log.LogDebugf("[opOpen] req:%v; resp: %v, body: %s", req,
		p.GetResultMesg(), p.Data)
	return
}

func (m *metaManager) opMetaInodeGet(conn net.Conn, p *Packet) (err error) {
	req := &InodeGetReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaInodeGet]: %s", err.Error())
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opMetaInodeGet request(%v) failed (%v)", req, err))
		}
	}()
	log.LogDebugf("[opMetaInodeGet] receive request: %v", req)
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaInodeGet] %s, req: %s", err.Error(),
			string(p.Data))
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	if err = mp.InodeGet(req, p); err != nil {
		err = errors.Errorf("[opMetaInodeGet] %s, req: %s", err.Error(),
			string(p.Data))
	}
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaInodeGet] req:%v; resp: %v, body: %s", req,
		p.GetResultMesg(), p.Data)
	return
}

func (m *metaManager) opMetaEvictInode(conn net.Conn, p *Packet) (err error) {
	req := &proto.EvictInodeRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, []byte(err.Error()))
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaEvictInode] request unmarshal: %v", err.Error())
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opMetaEvictInode request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("[opMetaEvictInode] req: %s, resp: %v", req, err.Error())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	if err = mp.EvictInode(req, p); err != nil {
		err = errors.Errorf("[opMetaEvictInode] req: %s, resp: %v", req, err.Error())
	}
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaEvictInode] req: %v, resp: %v, body: %s", req,
		p.GetResultMesg(), p.Data)
	return
}

func (m *metaManager) opMetaLookup(conn net.Conn, p *Packet) (err error) {
	req := &proto.LookupRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opMetaLookup request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.Lookup(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaLookup] req:%v; resp: %v, body: %s", req,
		p.GetResultMesg(), p.Data)
	return
}

func (m *metaManager) opMetaExtentsAdd(conn net.Conn, p *Packet) (err error) {
	req := &proto.AppendExtentKeyRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opMetaExtentsAdd request(%v) failed (%v)", req, err))
		}
	}()
	log.LogDebugf("[opMetaExtentsAdd] receive request: %v", req)
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		err = errors.Errorf("%s, response to client: %s", err.Error(),
			p.GetResultMesg())
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.ExtentAppend(req, p)
	m.respondToClient(conn, p)
	if err != nil {
		log.LogErrorf("[opMetaExtentsAdd] ExtentAppend: %s, "+
			"response to client: %s", err.Error(), p.GetResultMesg())
	}
	log.LogDebugf("[opMetaExtentsAdd] req: %v, resp: %v, body: %s", req,
		p.GetResultMesg(), p.Data)
	return
}

func (m *metaManager) opMetaExtentsList(conn net.Conn, p *Packet) (err error) {
	req := &proto.GetExtentsRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opMetaExtentsList request(%v) failed (%v)", req, err))
		}
	}()
	log.LogDebugf("[opMetaExtentsList] receive request: %v", req)
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}

	err = mp.ExtentsList(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaExtentsList] req:%v; resp: %v, body: %s", req,
		p.GetResultMesg(), p.Data)
	return
}

func (m *metaManager) opMetaExtentsDel(conn net.Conn, p *Packet) (err error) {
	// TODO: not implement yet
	panic("not implement yet")
}

func (m *metaManager) opMetaExtentsTruncate(conn net.Conn, p *Packet) (err error) {
	req := &ExtentsTruncateReq{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opMetaExtentsTruncate request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	mp.ExtentsTruncate(req, p)
	m.respondToClient(conn, p)
	return
}

//func (m *metaManager) opDeleteMetaPartition(conn net.Conn, p *Packet) (err error) {
//	adminTask := &proto.AdminTask{}
//	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
//	decode.UseNumber()
//	if err = decode.Decode(adminTask); err != nil {
//		p.PackErrorWithBody(proto.OpErr, nil)
//		m.respondToClient(conn, p)
//		return
//	}
//
//	req := &proto.DeleteMetaPartitionRequest{}
//	reqData, err := json.Marshal(adminTask.Request)
//	if err != nil {
//		p.PackErrorWithBody(proto.OpErr, nil)
//		m.respondToClient(conn, p)
//		return
//	}
//	if err = json.Unmarshal(reqData, req); err != nil {
//		p.PackErrorWithBody(proto.OpErr, nil)
//		m.respondToClient(conn, p)
//		return
//	}
//	defer func() {
//		if err != nil {
//			log.LogErrorf(fmt.Sprintf("opDeleteMetaPartition request(%v) failed (%v)", req, err))
//		}
//	}()
//	mp, err := m.getPartition(req.PartitionID)
//	if err != nil {
//		p.PackErrorWithBody(proto.OpNotExistErr, nil)
//		m.respondToClient(conn, p)
//		return
//	}
//	resp := &proto.DeleteMetaPartitionResponse{
//		PartitionID: req.PartitionID,
//		Status:      proto.TaskSuccess,
//	}
//	// Ack Master Request
//	m.responseAckOKToMaster(conn, p)
//	conf := mp.GetBaseConfig()
//	mp.Stop()
//	err = mp.DeleteRaft()
//	os.RemoveAll(conf.RootDir)
//	if err != nil {
//		resp.Status = proto.TaskFail
//	}
//	adminTask.Response = resp
//	adminTask.Request = nil
//	err = m.respondToMaster(adminTask)
//	log.LogWarnf("[opDeleteMetaPartition] req: %v, resp: %v", req, adminTask)
//	return
//}

func (m *metaManager) opExpiredMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}

	req := &proto.DeleteMetaPartitionRequest{}
	reqData, err := json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opDeleteMetaPartition request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		m.respondToClient(conn, p)
		return
	}
	resp := &proto.DeleteMetaPartitionResponse{
		PartitionID: req.PartitionID,
		Status:      proto.TaskSuccess,
	}
	// Ack Master Request
	mp.Expired()
	err = mp.ExpiredRaft()
	if err != nil {
		resp.Status = proto.TaskFail
	}
	adminTask.Response = resp
	adminTask.Request = nil
	m.responseAckOKToMaster(conn, p)
	err = m.respondToMaster(adminTask)
	log.LogWarnf("[opDeleteMetaPartition] req: %v, resp: %v", req, adminTask)
	return
}

func (m *metaManager) opUpdateMetaPartition(conn net.Conn, p *Packet) (err error) {
	log.LogDebugf("[opUpdateMetaPartition] request.")
	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	var (
		reqData []byte
		req     = new(UpdatePartitionReq)
	)
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	log.LogDebugf("[opUpdateMetaPartition] req: %v", adminTask)
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opUpdateMetaPartition request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	resp := &UpdatePartitionResp{
		VolName:     req.VolName,
		PartitionID: req.PartitionID,
		End:         req.End,
	}
	err = mp.UpdatePartition(req, resp)
	adminTask.Response = resp
	adminTask.Request = nil
	m.responseAckOKToMaster(conn, p)
	m.respondToMaster(adminTask)
	log.LogDebugf("[opUpdateMetaPartition] req[%v], response[%v].", req, adminTask)
	return
}

func (m *metaManager) opLoadMetaPartition(conn net.Conn, p *Packet) (err error) {
	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		p.WriteToConn(conn)
		return
	}
	var (
		req     = &proto.LoadMetaPartitionMetricRequest{}
		resp    = &proto.LoadMetaPartitionMetricResponse{}
		reqData []byte
	)
	if reqData, err = json.Marshal(adminTask.Request); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		p.WriteToConn(conn)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		p.WriteToConn(conn)
		return
	}

	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		log.LogErrorf("%s [opLoadMetaPartition] req[%v], "+
			"response marshal[%v]", conn.RemoteAddr(), req, err.Error())
		m.respondToClient(conn, p)
		return
	}
	resp = mp.ResponseLoadMetaPartition(p)
	mConf := mp.GetBaseConfig()
	resp.Start = mConf.Start
	resp.End = mConf.End
	resp.Status = proto.OpOk
	adminTask.Response = resp
	adminTask.Request = nil
	data, err := json.Marshal(resp)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, ([]byte)(err.Error()))
		log.LogErrorf("[opLoadMetaPartition] req[%v], "+
			"response marshal[%v]", req, err.Error())
		m.respondToClient(conn, p)
		return
	}
	p.PackOkWithBody(data)
	m.respondToClient(conn, p)
	log.LogDebugf("[opLoadMetaPartition] req[%v], response[%v].", req, adminTask)
	return
}

func (m *metaManager) opOfflineMetaPartition(conn net.Conn, p *Packet) (err error) {
	var (
		reqData []byte
		req     = &proto.MetaPartitionOfflineRequest{}
	)
	adminTask := &proto.AdminTask{}
	decode := json.NewDecoder(bytes.NewBuffer(p.Data))
	decode.UseNumber()
	if err = decode.Decode(adminTask); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	log.LogDebugf("[opOfflineMetaPartition] received task: %v", adminTask)
	reqData, err = json.Marshal(adminTask.Request)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if err = json.Unmarshal(reqData, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opOfflineMetaPartition request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		m.respondToClient(conn, p)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	resp := proto.MetaPartitionOfflineResponse{
		PartitionID: req.PartitionID,
		VolName:     req.VolName,
		Status:      proto.TaskFail,
	}

	if req.AddPeer.ID == req.RemovePeer.ID {
		err = errors.Errorf("[opOfflineMetaPartition]: AddPeer[%v] same withRemovePeer[%v]", req.AddPeer, req.RemovePeer)
		resp.Result = err.Error()
		goto end
	}
	_, err = mp.ChangeMember(raftProto.ConfAddNode,
		raftProto.Peer{ID: req.AddPeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	_, err = mp.ChangeMember(raftProto.ConfRemoveNode,
		raftProto.Peer{ID: req.RemovePeer.ID}, reqData)
	if err != nil {
		resp.Result = err.Error()
		goto end
	}
	resp.Status = proto.TaskSuccess
end:
	adminTask.Request = nil
	adminTask.Response = resp
	m.responseAckOKToMaster(conn, p)
	m.respondToMaster(adminTask)
	log.LogWarnf("[opOfflineMetaPartition]: the end %v", adminTask)
	return
}

func (m *metaManager) opMetaBatchInodeGet(conn net.Conn, p *Packet) (err error) {
	req := &proto.BatchInodeGetRequest{}
	if err = json.Unmarshal(p.Data, req); err != nil {
		p.PackErrorWithBody(proto.OpErr, nil)
		return
	}
	defer func() {
		if err != nil {
			log.LogErrorf(fmt.Sprintf("opMetaBatchInodeGet request(%v) failed (%v)", req, err))
		}
	}()
	mp, err := m.getPartition(req.PartitionID)
	if err != nil {
		p.PackErrorWithBody(proto.OpNotExistErr, nil)
		return
	}
	if !m.serveProxy(conn, mp, p) {
		return
	}
	err = mp.InodeGetBatch(req, p)
	m.respondToClient(conn, p)
	log.LogDebugf("[opMetaBatchInodeGet] req[%v], resp[%v], body: %s", req,
		p.GetResultMesg(), p.Data)
	return
}
