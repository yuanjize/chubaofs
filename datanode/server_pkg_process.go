// Copyright 2018 The Containerfs Authors.
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
	"container/list"
	"fmt"
	"time"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/third_party/juju/errors"
	"github.com/tiglabs/containerfs/util/log"
	"sync/atomic"
)

func (s *DataNode) readFromCliAndDeal(msgH *MessageHandler) (err error) {
	defer func() {
		if err != nil {
			msgH.Stop()
		}
	}()
	pkg := NewPacket()
	s.statsFlow(pkg, InFlow)
	if err = pkg.ReadFromConnFromCli(msgH.inConn, proto.NoReadDeadlineTime); err != nil {
		return
	}
	log.LogDebugf("action[readFromCliAndDeal] read packet[%v] from remote[%v].",
		pkg.GetUniqueLogId(), msgH.inConn.RemoteAddr().String())
	if pkg.IsMasterCommand() {
		msgH.requestCh <- pkg
		return
	}
	pkg.beforeTp(s.clusterId)

	if err = s.checkPacket(pkg); err != nil {
		pkg.PackErrorBody("checkPacket", err.Error())
		msgH.replyCh <- pkg
		return
	}
	if err = s.checkAction(pkg); err != nil {
		pkg.PackErrorBody("checkAction", err.Error())
		msgH.replyCh <- pkg
		return
	}
	if err = s.checkAndAddInfo(pkg); err != nil {
		pkg.PackErrorBody("checkAndAddInfo", err.Error())
		msgH.replyCh <- pkg
		return
	}
	msgH.requestCh <- pkg

	return
}

func (s *DataNode) checkAndAddInfo(pkg *Packet) error {
	if pkg.isHeadNode() && pkg.StoreMode == proto.TinyExtentMode && pkg.IsWriteOperation() {
		store := pkg.DataPartition.GetExtentStore()
		extentId, err := store.GetAvaliTinyExtent()
		if err != nil {
			return err
		}
		pkg.FileID = extentId
		pkg.Offset, err = store.GetWatermarkForWrite(extentId)
		if err != nil {
			return err
		}
	} else if pkg.isHeadNode() && pkg.Opcode == proto.OpCreateFile {
		pkg.FileID = pkg.DataPartition.GetExtentStore().NextExtentId()
	}

	return nil
}

func (s *DataNode) handleRequest(msgH *MessageHandler) {
	for {
		select {
		case <-msgH.handleCh:
			s.reciveFromAllReplicates(msgH)
		case <-msgH.exitC:
			return
		}
	}
}

func (s *DataNode) doRequestCh(req *Packet, msgH *MessageHandler) {
	var (
		err error
	)
	if !req.IsTransitPkg() {
		s.operatePacket(req, msgH.inConn)
		if !(req.Opcode == proto.OpStreamRead || req.Opcode == proto.OpExtentRepairRead) {
			msgH.replyCh <- req
		}

		return
	}
	if _, err = s.sendToAllReplicates(req, msgH); err == nil {
		s.operatePacket(req, msgH.inConn)
	}
	msgH.handleCh <- single

	return
}

func (s *DataNode) doReplyCh(reply *Packet, msgH *MessageHandler) {
	var err error
	if reply.IsErrPack() {
		err = fmt.Errorf(reply.ActionMsg(ActionWriteToCli, msgH.inConn.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		log.LogErrorf("action[doReplyCh] %v", err)
		reply.forceDestoryCheckUsedClosedConnect(err)
	}
	s.cleanup(reply)
	if err = reply.WriteToConn(msgH.inConn); err != nil {
		err = fmt.Errorf(reply.ActionMsg(ActionWriteToCli, msgH.inConn.RemoteAddr().String(),
			reply.StartT, err))
		log.LogErrorf("action[doReplyCh] %v", err)
		reply.forceDestoryAllConnect()
		msgH.Stop()
		return
	}
	s.addMetrics(reply)
	log.LogDebugf("action[doReplyCh] %v", reply.ActionMsg(ActionWriteToCli,
		msgH.inConn.RemoteAddr().String(), reply.StartT, err))
	s.statsFlow(reply, OutFlow)

}

func (s *DataNode) cleanup(pkg *Packet) {
	if !pkg.isHeadNode() {
		return
	}
	s.leaderPutTinyExtentToStore(pkg)
	if !pkg.useConnectMap {
		pkg.PutConnectsToPool()
	}
}

func (s *DataNode) addMetrics(reply *Packet) {
	if reply.IsMasterCommand() {
		return
	}
	reply.afterTp()
	latency := time.Since(reply.tpObject.StartTime)
	if reply.DataPartition == nil {
		return
	}
	if reply.IsWriteOperation() {
		reply.DataPartition.AddWriteMetrics(uint64(latency))
	} else if reply.IsReadOperation() {
		reply.DataPartition.AddReadMetrics(uint64(latency))
	}
}

func (s *DataNode) writeToCli(msgH *MessageHandler) {
	for {
		select {
		case req := <-msgH.requestCh:
			s.doRequestCh(req, msgH)
		case reply := <-msgH.replyCh:
			s.doReplyCh(reply, msgH)
		case <-msgH.exitC:
			msgH.ClearReqs(s)
			return
		}
	}
}

func (s *DataNode) reciveFromAllReplicates(msgH *MessageHandler) (request *Packet) {
	var (
		e *list.Element
	)

	if e = msgH.GetListElement(); e == nil {
		return
	}
	request = e.Value.(*Packet)
	defer func() {
		msgH.DelListElement(request)
	}()
	for index := 0; index < len(request.NextAddrs); index++ {
		_, err := s.receiveFromNext(request, index)
		if err != nil {
			request.PackErrorBody(ActionReceiveFromNext, err.Error())
			return
		}
	}
	request.PackOkReply()

	return
}

func (s *DataNode) receiveFromNext(request *Packet, index int) (reply *Packet, err error) {
	if request.NextConns[index] == nil {
		err = errors.Annotatef(fmt.Errorf(ConnIsNullErr), "Request(%v) receiveFromNext Error", request.GetUniqueLogId())
		return
	}

	// Check local execution result.
	if request.IsErrPack() {
		err = fmt.Errorf(ActionReceiveFromNext+" local operator failed (%v)", request.getErr())
		err = errors.Annotatef(fmt.Errorf(request.getErr()), "Request(%v) receiveFromNext Error", request.GetUniqueLogId())
		log.LogErrorf(err.Error())
		return
	}

	reply = NewPacket()

	if err = reply.ReadFromConn(request.NextConns[index], proto.ReadDeadlineTime); err != nil {
		err = fmt.Errorf(ActionReceiveFromNext+"recive From remote (%v) network error (%v) ",
			request.NextAddrs[index], err)
		err = errors.Annotatef(err, "Request(%v) receiveFromNext %v Error", request.GetUniqueLogId(), request.NextConns[index])
		log.LogErrorf(err.Error())
		return
	}

	if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
		reply.Offset != request.Offset || reply.Crc != request.Crc || reply.FileID != request.FileID {
		err = fmt.Errorf(ActionReceiveFromNext+" request (%v) reply(%v) %v from localAddr(%v)"+
			" remoteAddr(%v) requestCrc(%v) replyCrc(%v) not match ", request.GetUniqueLogId(), reply.GetUniqueLogId(), request.NextAddrs[index],
			request.NextConns[index].LocalAddr().String(), request.NextConns[index].RemoteAddr().String(), request.Crc, reply.Crc)
		log.LogErrorf(err.Error())
		return
	}

	if reply.IsErrPack() {
		err = fmt.Errorf(ActionReceiveFromNext+"remote (%v) do failed(%v)",
			request.NextAddrs[index], string(reply.Data[:reply.Size]))
		err = errors.Annotatef(err, "Request(%v) receiveFromNext Error", request.GetUniqueLogId())
		log.LogErrorf(err.Error())
		return
	}

	log.LogDebugf("action[receiveFromNext] %v.", reply.ActionMsg(ActionReceiveFromNext, request.NextAddrs[index], request.StartT, err))
	return
}

func (s *DataNode) sendToAllReplicates(pkg *Packet, msgH *MessageHandler) (index int, err error) {
	msgH.PushListElement(pkg)
	for index = 0; index < len(pkg.NextConns); index++ {
		err = msgH.AllocateNextConn(pkg, index)
		if err != nil {
			msg := fmt.Sprintf("pkg inconnect(%v) to(%v) err(%v)", msgH.inConn.RemoteAddr().String(),
				pkg.NextAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendToAllReplicates Error", pkg.GetUniqueLogId())
			pkg.PackErrorBody(ActionSendToNext, err.Error())
			return
		}
		nodes := pkg.Nodes
		pkg.Nodes = 0
		if err == nil {
			err = pkg.WriteToConn(pkg.NextConns[index])
		}
		pkg.Nodes = nodes
		if err != nil {
			msg := fmt.Sprintf("pkg inconnect(%v) to(%v) err(%v)", msgH.inConn.RemoteAddr().String(),
				pkg.NextAddrs[index], err.Error())
			err = errors.Annotatef(fmt.Errorf(msg), "Request(%v) sendToAllReplicates Error", pkg.GetUniqueLogId())
			pkg.PackErrorBody(ActionSendToNext, err.Error())
			return
		}
	}

	return
}

func (s *DataNode) checkStoreMode(p *Packet) (err error) {
	if p.StoreMode == proto.TinyExtentMode || p.StoreMode == proto.NormalExtentMode {
		return nil
	}
	return ErrStoreTypeMismatch
}

func (s *DataNode) checkPacket(pkg *Packet) error {
	var err error
	pkg.StartT = time.Now().UnixNano()
	if err = s.checkStoreMode(pkg); err != nil {
		return err
	}

	if err = pkg.CheckCrc(); err != nil {
		return err
	}
	var addrs []string
	if addrs, err = pkg.UnmarshalAddrs(); err == nil {
		err = pkg.GetNextAddr(addrs)
	}
	if err != nil {
		return err
	}
	return nil
}

func (s *DataNode) checkAction(pkg *Packet) (err error) {
	dp := s.space.GetPartition(pkg.PartitionID)
	if dp == nil {
		err = errors.Errorf("partition %v is not exist", pkg.PartitionID)
		return
	}
	pkg.DataPartition = dp
	if pkg.Opcode == proto.OpWrite || pkg.Opcode == proto.OpCreateFile {
		if pkg.DataPartition.Available() <= 0 {
			err = storage.ErrSyscallNoSpace
			return
		}
	}
	return
}

func (s *DataNode) statsFlow(pkg *Packet, flag int) {
	stat := s.space.Stats()
	if pkg == nil {
		return
	}
	if flag == OutFlow {
		stat.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
		return
	}

	if pkg.IsReadOperation() {
		stat.AddInDataSize(uint64(pkg.Arglen))
	} else {
		stat.AddInDataSize(uint64(pkg.Size + pkg.Arglen))
	}

}

func (s *DataNode) leaderPutTinyExtentToStore(pkg *Packet) {
	if pkg == nil || pkg.FileID <= 0 || atomic.LoadInt32(&pkg.IsReturn) == HasReturnToStore || !storage.IsTinyExtent(pkg.FileID) {
		return
	}
	if pkg.StoreMode != proto.TinyExtentMode || !pkg.isHeadNode() || !pkg.IsWriteOperation() || !pkg.IsTransitPkg() {
		return
	}
	store := pkg.DataPartition.GetExtentStore()
	if pkg.IsErrPack() {
		store.PutTinyExtentToUnavaliCh(pkg.FileID)
	} else {
		store.PutTinyExtentToAvaliCh(pkg.FileID)
	}
	atomic.StoreInt32(&pkg.IsReturn, HasReturnToStore)
}
