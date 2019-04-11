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
	"container/list"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

var single = struct{}{}

type MessageHandler struct {
	listMux     sync.RWMutex
	sentList    *list.List
	handleCh    chan struct{}
	requestCh   chan *Packet
	replyCh     chan *Packet
	inConn      *net.TCPConn
	isClean     bool
	exitC       chan bool
	exited      int32
	exitedMu    sync.RWMutex
	connectMap  map[string]*net.TCPConn
	connectLock sync.RWMutex
}

func NewMsgHandler(inConn *net.TCPConn) *MessageHandler {
	m := new(MessageHandler)
	m.sentList = list.New()
	m.handleCh = make(chan struct{}, RequestChanSize)
	m.requestCh = make(chan *Packet, RequestChanSize)
	m.replyCh = make(chan *Packet, RequestChanSize)
	m.exitC = make(chan bool, 100)
	m.inConn = inConn
	m.exited = ReplRuning
	m.connectMap = make(map[string]*net.TCPConn)

	return m
}

func (msgH *MessageHandler) RenewList(isHeadNode bool) {
	if !isHeadNode {
		msgH.sentList = list.New()
	}
}

func (msgH *MessageHandler) Stop() {
	msgH.exitedMu.Lock()
	defer msgH.exitedMu.Unlock()
	if atomic.LoadInt32(&msgH.exited) == ReplRuning {
		if msgH.exitC != nil {
			close(msgH.exitC)
		}
		atomic.StoreInt32(&msgH.exited, ReplExiting)
	}

}

func (msgH *MessageHandler) AllocateNextConn(pkg *Packet, index int) (err error) {
	key := fmt.Sprintf("%v_%v_%v", pkg.PartitionID, pkg.FileID, pkg.NextAddrs[index])
	msgH.connectLock.RLock()
	conn := msgH.connectMap[key]
	msgH.connectLock.RUnlock()
	if conn == nil {
		conn, err = gConnPool.Get(pkg.NextAddrs[index])
		if err != nil {
			return
		}
		msgH.connectLock.Lock()
		msgH.connectMap[key] = conn
		msgH.connectLock.Unlock()
	}
	pkg.useConnectMap = true
	pkg.NextConns[index] = conn

	return nil
}

func (msgH *MessageHandler) GetListElement() (e *list.Element) {
	msgH.listMux.RLock()
	e = msgH.sentList.Front()
	msgH.listMux.RUnlock()

	return
}

func (msgH *MessageHandler) PushListElement(e *Packet) {
	msgH.listMux.Lock()
	msgH.sentList.PushBack(e)
	msgH.listMux.Unlock()
}

func (msgH *MessageHandler) cleanRequest(s *DataNode) {
	defer func() {
		close(msgH.requestCh)
	}()
	for {
		select {
		case pkg := <-msgH.requestCh:
			pkg.forceDestoryAllConnect()
			s.leaderPutTinyExtentToStore(pkg)
		default:
			return

		}
	}
}

func (msgH *MessageHandler) cleanReply(s *DataNode) {
	defer func() {
		close(msgH.replyCh)
	}()
	for {
		select {
		case pkg := <-msgH.replyCh:
			pkg.forceDestoryAllConnect()
			s.leaderPutTinyExtentToStore(pkg)
		default:
			return

		}
	}
}

func (msgH *MessageHandler) cleanResource(s *DataNode) {
	msgH.listMux.Lock()
	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		request.forceDestoryAllConnect()
		s.leaderPutTinyExtentToStore(request)
	}
	close(msgH.handleCh)
	msgH.cleanReply(s)
	msgH.cleanRequest(s)
	msgH.sentList = list.New()
	msgH.connectLock.Lock()
	for _, conn := range msgH.connectMap {
		conn.Close()
	}
	msgH.connectMap = make(map[string]*net.TCPConn, 0)
	msgH.connectLock.Unlock()
	msgH.listMux.Unlock()
}

func (msgH *MessageHandler) DelListElement(reply *Packet) (success bool) {
	msgH.listMux.Lock()
	defer msgH.listMux.Unlock()
	for e := msgH.sentList.Front(); e != nil; e = e.Next() {
		request := e.Value.(*Packet)
		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.Offset != request.Offset || reply.Crc != request.Crc || reply.FileID != request.FileID {
			request.forceDestoryAllConnect()
			request.PackErrorBody(ActionReceiveFromNext, fmt.Sprintf("unknow expect reply"))
			break
		}
		msgH.sentList.Remove(e)
		success = true
		msgH.replyCh <- reply
	}

	return
}
