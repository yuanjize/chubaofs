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

package stream

import (
	"fmt"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/wrapper"
	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"net"
	"strings"
	"sync/atomic"
)

const (
	MaxSelectDataPartionForWrite = 32
	MaxStreamInitRetry           = 3
	HasClosed                    = -1
)

const (
	StreamerNormal int32 = iota
	StreamerError
)

type OpenRequest struct {
	flag uint32
	err  error
	done chan struct{}
}
type WriteRequest struct {
	data         []byte
	size         int
	canWrite     int
	err          error
	kernelOffset int
	actualOffset int
	done         chan struct{}
}

type ExitRequest struct {
	err  error
	done chan struct{}
}

type FlushRequest struct {
	err  error
	done chan struct{}
}

type ReleaseRequest struct {
	flag uint32
	err  error
	done chan struct{}
}

type EvictRequest struct {
	err  error
	done chan struct{}
}

func (s *StreamWriter) IssueOpenRequest(request *OpenRequest, flag uint32) error {
	request.flag = flag
	s.requestCh <- request
	s.client.writerLock.Unlock()
	<-request.done
	err := request.err

	return err
}

func (s *StreamWriter) IssueWriteRequest(offset int, data []byte) (write int, actualOffset int, err error) {
	if atomic.LoadInt32(&s.status) >= StreamerError {
		return 0, 0, errors.New(fmt.Sprintf("IssueWriteRequest: stream writer in error status, ino(%v)", s.inode))
	}

	request := writeRequestPool.Get().(*WriteRequest)
	request.data = data
	request.size = len(data)
	request.done = make(chan struct{}, 1)
	s.requestCh <- request
	<-request.done
	err = request.err
	write = request.canWrite
	actualOffset = request.actualOffset
	close(request.done)
	writeRequestPool.Put(request)
	return
}

func (s *StreamWriter) IssueFlushRequest() error {
	request := flushRequestPool.Get().(*FlushRequest)
	request.done = make(chan struct{}, 1)
	s.requestCh <- request
	<-request.done
	err := request.err
	close(request.done)
	flushRequestPool.Put(request)
	return err
}

func (s *StreamWriter) IssueReleaseRequest(request *ReleaseRequest, flag uint32) error {
	request.flag = flag
	s.requestCh <- request
	s.client.writerLock.Unlock()
	<-request.done
	err := request.err

	return err
}

func (s *StreamWriter) IssueEvictRequest(request *EvictRequest) error {
	s.requestCh <- request
	s.client.writerLock.Unlock()
	<-request.done
	err := request.err

	return err
}

func (s *StreamWriter) evict() error {
	s.client.writerLock.Lock()
	if s.refcnt > 0 || len(s.requestCh) != 0 {
		s.client.writerLock.Unlock()
		return errors.New(fmt.Sprintf("evict: streamer(%v) refcnt(%v) openWriteCnt(%v)", s, s.refcnt, s.openWriteCnt))
	}
	delete(s.client.writers, s.inode)
	s.client.writerLock.Unlock()
	return nil
}

func (s *StreamWriter) open(flag uint32) error {
	s.refcnt++
	if !proto.IsWriteFlag(flag) {
		return nil
	}
	s.openWriteCnt++
	log.LogDebugf("open: streamer(%v) openWriteCnt(%v) ", s, s.openWriteCnt)
	return nil
}

func (s *StreamWriter) release(flag uint32) error {
	s.refcnt--
	if !proto.IsWriteFlag(flag) {
		return nil
	}
	err := s.flushCurrExtentWriter()
	s.openWriteCnt--
	if s.openWriteCnt <= 0 {
		s.close()
	}
	log.LogDebugf("release: streamer(%v) openWriteCnt(%v)", s.toString(), s.openWriteCnt)
	return err
}

type StreamWriter struct {
	currentWriter           *ExtentWriter //current ExtentWriter
	currentPartitionId      uint32        //current PartitionId
	currentExtentId         uint64        //current FileId
	inode                   uint64        //inode
	excludePartition        []uint32
	appendExtentKey         AppendExtentKeyFunc
	requestCh               chan interface{}
	exitCh                  chan struct{}
	hasUpdateKey            map[string]int
	hasWriteSize            uint64
	fileSize                uint64
	hasClosed               int32
	metaNodeStreamKey       *proto.StreamKey
	hasUpdateToMetaNodeSize uint64
	recoverPackages         []*Packet
	inodeHasDelete          bool
	kernelOffset            uint64

	refcnt        int
	autoForgetCnt int
	openWriteCnt  int
	status        int32
	client        *ExtentClient
}

func NewStreamWriter(inode uint64, client *ExtentClient, appendExtentKey AppendExtentKeyFunc) (s *StreamWriter) {
	s = new(StreamWriter)
	s.appendExtentKey = appendExtentKey
	s.inode = inode
	s.requestCh = make(chan interface{}, 1000)
	s.exitCh = make(chan struct{}, 10)
	s.excludePartition = make([]uint32, 0)
	s.hasUpdateKey = make(map[string]int, 0)
	s.metaNodeStreamKey = new(proto.StreamKey)
	s.recoverPackages = make([]*Packet, 0)
	s.client = client
	go s.server()

	return
}

func (s *StreamWriter) toString() (m string) {
	currentWriterMsg := ""
	if s.currentWriter != nil {
		currentWriterMsg = s.currentWriter.toString()
	}
	return fmt.Sprintf("ino(%v) currentDataPartion(%v) currentExtentId(%v) exitCh(%v) openCnt(%v)",
		s.inode, s.currentPartitionId, currentWriterMsg, len(s.exitCh), s.openWriteCnt)
}

func (s *StreamWriter) toStringWithWriter(writer *ExtentWriter) (m string) {
	currentWriterMsg := writer.toString()
	return fmt.Sprintf("ino(%v) currentDataPartion(%v) currentExtentId(%v) exitCh(%v) openCnt(%v)",
		s.inode, s.currentPartitionId, currentWriterMsg, len(s.exitCh), s.openWriteCnt)
}

//stream init,alloc a extent ,select dp and extent
func (s *StreamWriter) init(useNormalExtent bool, prepareWriteSize int) (err error) {
	if s.currentWriter != nil && s.currentWriter.isFullExtent(prepareWriteSize) {
		err = s.flushCurrExtentWriter()
		if err == syscall.ENOENT {
			return
		}
		if err != nil {
			return errors.Annotatef(err, "Flush error WriteInit")
		}
		useNormalExtent = true
		s.setCurrentWriter(nil)
	}
	if s.currentWriter != nil {
		return
	}
	var writer *ExtentWriter
	writer, err = s.allocateNewExtentWriter(useNormalExtent)
	if err != nil {
		err = errors.Annotatef(err, "WriteInit AllocNewExtentFailed")
		return err
	}
	s.setCurrentWriter(writer)
	return
}

const (
	MaxAutoForgetCnt = 20
)

func (s *StreamWriter) server() {
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()
	for {
		select {
		case request := <-s.requestCh:
			s.handleRequest(request)
		case <-s.exitCh:
			s.flushCurrExtentWriter()
			log.LogDebugf(fmt.Sprintf("ino(%v) recive sigle exit serve", s.inode))
			return
		case <-t.C:
			autoExit := s.autoTask()
			if !autoExit {
				continue
			}
			for {
				select {
				case req := <-s.requestCh:
					s.handleExit(req)
				default:
					return
				}
			}

			return
		}
	}
}

func (s *StreamWriter) autoTask() (autoExit bool) {
	log.LogDebugf("ino(%v) update to metanode filesize To(%v) user has Write to (%v)",
		s.inode, s.metaNodeStreamKey.Size(), s.getHasWriteSize())

	if s.refcnt <= 0 {
		s.autoForgetCnt++
	} else {
		s.autoForgetCnt = 0
	}

	err := s.flushCurrExtentWriter()
	if err == syscall.ENOENT {
		autoExit = true
		log.LogWarnf("ino(%v) has beeen delete meta", s.inode)
	}

	if s.autoForgetCnt >= MaxAutoForgetCnt {
		autoExit = true
	}
	if autoExit {
		s.client.release(s.inode)
		log.LogWarnf("ino(%v) auto send forget command", s.inode)
	}
	return
}

func (s *StreamWriter) handleRequest(request interface{}) {
	switch request := request.(type) {
	case *OpenRequest:
		log.LogDebugf("received an open request: ino(%v) flag(%v)", s.inode, request.flag)
		request.err = s.open(request.flag)
		log.LogDebugf("open returned: ino(%v) flag(%v)", s.inode, request.flag)
		request.done <- struct{}{}
	case *WriteRequest:
		if s.inodeHasDelete {
			request.err = syscall.ENOENT
			request.done <- struct{}{}
			return
		}
		request.actualOffset = int(s.getHasWriteSize())
		request.canWrite, request.err = s.write(request.data, request.actualOffset, request.size)
		s.addHasWriteSize(request.canWrite)
		request.done <- struct{}{}
	case *FlushRequest:
		request.err = s.flushCurrExtentWriter()
		request.done <- struct{}{}
	case *ReleaseRequest:
		request.err = s.release(request.flag)
		request.done <- struct{}{}
	case *EvictRequest:
		request.err = s.evict()
		request.done <- struct{}{}
	default:
	}
}

func (s *StreamWriter) handleExit(req interface{}) {
	switch request := req.(type) {
	case *OpenRequest:
		request.err = syscall.EAGAIN
		request.done <- struct{}{}
		return
	case *WriteRequest:
		if s.inodeHasDelete {
			request.err = syscall.ENOENT
			request.done <- struct{}{}
			return
		}
		request.err = syscall.EINVAL
		request.done <- struct{}{}
		return
	case *FlushRequest:
		request.done <- struct{}{}
		return
	case *ReleaseRequest:
		request.done <- struct{}{}
		return
	case *EvictRequest:
		request.done <- struct{}{}
		return
	default:
		return
	}
}

func (s *StreamWriter) write(data []byte, offset, size int) (total int, err error) {
	var (
		write int
	)
	defer func() {
		if err == nil {
			total = size
			return
		}
		if err == syscall.ENOENT {
			total = 0
			return
		}
		err = errors.Annotatef(err, "UserRequest{ino(%v) write "+
			"KernelOffset(%v) KernelSize(%v) hasWrite(%v)}  stream{ (%v) occous error}",
			s.inode, offset, size, total, s.toString())
		log.LogError(err.Error())
		log.LogError(errors.ErrorStack(err))
	}()

	var (
		initRetry int = 0
	)
	for total < size {
		var useExtent = true
		if offset+size <= util.TinyBlockSize {
			useExtent = false
		}
		err = s.init(useExtent, size)
		if err == syscall.ENOENT {
			return
		}
		if err != nil {
			if initRetry > MaxStreamInitRetry {
				return
			}
			initRetry++
			continue
		}
		write, err = s.currentWriter.write(data[total:size], offset, size-total)
		if err == nil {
			write = size - total
			total += write
			continue
		}
		if strings.Contains(err.Error(), FullExtentErr.Error()) {
			continue
		}
		err = s.recoverExtent()
		if err == syscall.ENOENT {
			return
		}
		if err != nil {
			return
		}
		total += write
	}

	return total, err
}

func (s *StreamWriter) close() (err error) {
	if s.currentWriter != nil {
		err = s.currentWriter.close()
	}
	return
}

func (s *StreamWriter) flushData() (err error) {
	writer := s.getCurrentWriter()
	if writer == nil {
		err = nil
		return nil
	}
	if err = writer.flush(); err != nil {
		err = errors.Annotatef(err, "writer(%v) Flush Failed", writer.toString())
		log.LogWarnf(err.Error())
		return err
	}
	err = s.updateToMetaNode()
	if err == syscall.ENOENT {
		return
	}
	if err != nil {
		err = errors.Annotatef(err, "update to MetaNode failed(%v)", err.Error())
		log.LogWarnf(err.Error())
		return err
	}
	if writer.isTinyExtent() || writer.isFullExtent(0) {
		writer.close()
		writer.getConnect().Close()
		err = s.updateToMetaNode()
		if err == syscall.ENOENT {
			return
		}
		if err != nil {
			err = errors.Annotatef(err, "update to MetaNode failed(%v)", err.Error())
			log.LogWarnf(err.Error())
			return err
		}
		s.currentWriter.notifyRecvThreadExit()
		s.setCurrentWriter(nil)
	}
	return
}

func (s *StreamWriter) flushCurrExtentWriter() (err error) {
	defer func() {
		if err == syscall.ENOENT {
			return
		}
		if len(s.recoverPackages) != 0 {
			err = fmt.Errorf("MaxFlushCurrentExtent failed,packet(%v) not flush %v err(%v)",
				s.toString(), len(s.recoverPackages), err)
			log.LogErrorf(err.Error())
		}
	}()
	var errCount = 0
	for {
		err = s.flushData()
		if err == nil {
			return
		}
		if err == syscall.ENOENT {
			return
		}
		log.LogWarnf("FlushCurrentExtent %v failed,err %v errCnt %v",
			s.toString(), err.Error(), errCount)
		err = s.recoverExtent()
		if err == syscall.ENOENT {
			return
		}
		if err == nil {
			err = s.flushData()
			if err == syscall.ENOENT {
				return
			}
			if err == nil {
				return
			}
		}

		log.LogWarnf("FlushCurrentExtent %v failed,err %v errCnt %v",
			s.toString(), err.Error(), errCount)
		errCount++
		if errCount > MaxSelectDataPartionForWrite {
			break
		}
	}

	return err
}

func (s *StreamWriter) setCurrentWriter(writer *ExtentWriter) {
	oldWriter := s.currentWriter
	if oldWriter != nil {
		oldWriter.notifyRecvThreadExit()
	}
	s.currentWriter = writer
}

func (s *StreamWriter) getCurrentWriter() *ExtentWriter {
	return s.currentWriter
}

func (s *StreamWriter) updateToMetaNode() (err error) {
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		writer := s.currentWriter
		if writer == nil {
			log.LogDebugf("updateToMetaNode: ino(%v) nil writer", s.inode)
			return
		}

		if writer.isDirty() == false {
			log.LogDebugf("updateToMetaNode: ino(%v) writeer(%v) current extent"+
				" writer not dirty", s.inode, writer.toString())
			return
		}

		ek := writer.toKey()                 //first get currentExtent Key
		err = s.appendExtentKey(s.inode, ek) //put it to metanode
		if err == syscall.ENOENT {
			s.inodeHasDelete = true
			return
		}
		if err != nil {
			err = errors.Annotatef(err, "update extent(%v) to MetaNode Failed", ek.Size)
			log.LogWarnf("stream(%v) err(%v)", s.toString(), err.Error())
			continue
		}
		writer.clearDirty(ek.Size)
		s.metaNodeStreamKey.Put(ek)
		return
	}

	return err
}

func (s *StreamWriter) writeRecoverPackets(writer *ExtentWriter, retryPackets []*Packet) (err error) {
	for _, p := range retryPackets {
		log.LogWarnf("stream(%v) recover packet (%v) kernelOffset(%v) to extent(%v)",
			s.toString(), p.GetUniqueLogId(), p.kernelOffset, writer.toString())
		_, err = writer.write(p.Data[:p.Size], p.kernelOffset, int(p.Size))
		if err != nil {
			err = errors.Annotatef(err, "pkg(%v) RecoverExtent write failed", p.GetUniqueLogId())
			log.LogWarnf("stream(%v) err(%v)", s.toStringWithWriter(writer), err.Error())
			s.excludePartitionId(writer.dp.PartitionID)
			return err
		}
	}
	return
}

func (s *StreamWriter) recoverExtent() (err error) {
	s.excludePartitionId(s.currentWriter.dp.PartitionID)
	s.currentWriter.notifyRecvThreadExit()
	s.recoverPackages = s.currentWriter.getNeedRetrySendPackets() //get need retry
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if err = s.updateToMetaNode(); err == nil {
			break
		}
		if err == syscall.ENOENT {
			return err
		}
	}
	if err != nil {
		return err
	}
	if len(s.recoverPackages) == 0 {
		return nil
	}
	var writer *ExtentWriter
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		err = nil
		s.excludePartition = make([]uint32, 0)
		s.excludePartitionId(s.currentPartitionId)
		if writer, err = s.allocateNewExtentWriter(true); err != nil { //allocate new extent
			err = errors.Annotatef(err, "RecoverExtent Failed")
			log.LogWarnf("stream(%v) err(%v)", s.toString(), err.Error())
			continue
		}
		if err = s.writeRecoverPackets(writer, s.recoverPackages); err == nil {
			s.excludePartition = make([]uint32, 0)
			s.recoverPackages = make([]*Packet, 0)
			s.setCurrentWriter(writer)
			s.updateToMetaNode()
			return nil
		} else {
			writer.forbirdUpdateToMetanode()
			writer.notifyRecvThreadExit()
		}
	}

	return err

}

func (s *StreamWriter) excludePartitionId(partitionId uint32) {
	if s.excludePartition == nil {
		s.excludePartition = make([]uint32, 0)
	}
	hasExclude := false
	for _, pId := range s.excludePartition {
		if pId == partitionId {
			hasExclude = true
			break
		}
	}
	if hasExclude == false {
		s.excludePartition = append(s.excludePartition, partitionId)
	}

}

func (s *StreamWriter) allocateNewExtentWriter(useNormalExtent bool) (writer *ExtentWriter, err error) {
	var (
		dp       *wrapper.DataPartition
		extentId uint64
	)
	err = fmt.Errorf("cannot alloct new extent after maxrery")
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		extentId = 0
		if dp, err = gDataWrapper.GetWriteDataPartition(s.excludePartition); err != nil {
			log.LogWarnf(fmt.Sprintf("stream (%v) ActionAllocNewExtentWriter "+
				"failed on getWriteDataPartion,error(%v) execludeDataPartion(%v)", s.toString(), err.Error(), s.excludePartition))
			continue
		}
		if useNormalExtent == true {
			if extentId, err = s.createExtent(dp); err != nil {
				log.LogWarnf(fmt.Sprintf("stream (%v)ActionAllocNewExtentWriter "+
					"create Extent,error(%v) execludeDataPartion(%v)", s.toString(), err.Error(), s.excludePartition))
				if !strings.Contains(err.Error(), "use of closed network connection") {
					s.excludePartitionId(dp.PartitionID)
				}
				continue
			}
		}
		if writer, err = NewExtentWriter(s.inode, dp, extentId); err != nil {
			log.LogWarnf(fmt.Sprintf("stream (%v) ActionAllocNewExtentWriter "+
				"NewExtentWriter(%v),error(%v) execludeDataPartion(%v)", s.toString(), extentId, err.Error(), s.excludePartition))
			continue
		}
		err = nil
		break
	}
	if useNormalExtent == true && extentId <= 0 || err != nil {
		log.LogWarnf("allocateNewExtentWriter: err(%v) extentId(%v)", err, extentId)
		return nil, err
	}
	s.currentPartitionId = dp.PartitionID
	err = nil
	if useNormalExtent {
		writer.storeMode = proto.NormalExtentMode
	} else {
		writer.storeMode = proto.TinyExtentMode
	}
	return writer, nil
}

func (s *StreamWriter) createExtent(dp *wrapper.DataPartition) (extentId uint64, err error) {
	var (
		connect *net.TCPConn
	)
	conn, err := net.DialTimeout("tcp", dp.Hosts[0], time.Second)
	if err != nil {
		err = errors.Annotatef(err, " get connect from datapartionHosts(%v)", dp.Hosts[0])
		return 0, err
	}
	connect, _ = conn.(*net.TCPConn)
	connect.SetKeepAlive(true)
	connect.SetNoDelay(true)
	defer connect.Close()
	p := NewCreateExtentPacket(dp, s.inode)
	if err = p.WriteToConn(connect); err != nil {
		err = errors.Annotatef(err, "send CreateExtent(%v) to datapartionHosts(%v)", p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	if err = p.ReadFromConn(connect, proto.ReadDeadlineTime*2); err != nil {
		err = errors.Annotatef(err, "receive CreateExtent(%v) failed datapartionHosts(%v)", p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.Annotatef(fmt.Errorf("unavali resultcode %v errmsg %v",
			p.ResultCode, string(p.Data[:p.Size])), "receive CreateExtent(%v) failed datapartionHosts(%v) ",
			p.GetUniqueLogId(), dp.Hosts[0])
		return
	}
	extentId = p.FileID
	if p.FileID <= 0 {
		err = errors.Annotatef(err, "illegal extentId(%v) from (%v) response",
			extentId, dp.Hosts[0])
		return

	}

	return extentId, nil
}

func (s *StreamWriter) exit() {
	select {
	case s.exitCh <- struct{}{}:
	default:
	}
}

func (s *StreamWriter) getHasWriteSize() uint64 {
	return atomic.LoadUint64(&s.hasWriteSize)
}

func (s *StreamWriter) addHasWriteSize(writed int) {
	atomic.AddUint64(&s.hasWriteSize, uint64(writed))
}

func (s *StreamWriter) setHasWriteSize(writeSize uint64) {
	atomic.StoreUint64(&s.hasWriteSize, writeSize)

}
