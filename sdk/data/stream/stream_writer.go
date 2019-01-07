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

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/third_party/juju/errors"
	"github.com/tiglabs/containerfs/util/log"
	"net"
	"strings"
	"sync/atomic"
)

const (
	MaxSelectDataPartionForWrite = 32
	MaxStreamInitRetry           = 3
	HasClosed                    = -1
)

type WriteRequest struct {
	data         []byte
	size         int
	canWrite     int
	err          error
	kernelOffset int
	actualOffset int
	done         chan struct{}
}

type FlushRequest struct {
	err  error
	done chan struct{}
}

type CloseRequest struct {
	err  error
	done chan struct{}
}

type StreamWriter struct {
	currentWriter           *ExtentWriter //current ExtentWriter
	currentPartitionId      uint32        //current PartitionId
	currentExtentId         uint64        //current FileId
	Inode                   uint64        //inode
	excludePartition        []uint32
	appendExtentKey         AppendExtentKeyFunc
	requestCh               chan interface{}
	exitCh                  chan bool
	hasUpdateKey            map[string]int
	hasWriteSize            uint64
	hasClosed               int32
	metaNodeStreamKey       *proto.StreamKey
	hasUpdateToMetaNodeSize uint64
	recoverPackages         []*Packet
	inodeHasDelete          bool
	kernelOffset            uint64
}

func NewStreamWriter(inode, start uint64, appendExtentKey AppendExtentKeyFunc) (stream *StreamWriter) {
	stream = new(StreamWriter)
	stream.appendExtentKey = appendExtentKey
	stream.Inode = inode
	stream.requestCh = make(chan interface{}, 1000)
	stream.exitCh = make(chan bool, 10)
	stream.excludePartition = make([]uint32, 0)
	stream.hasUpdateKey = make(map[string]int, 0)
	stream.metaNodeStreamKey = new(proto.StreamKey)
	stream.recoverPackages = make([]*Packet, 0)
	go stream.server()

	return
}

func (stream *StreamWriter) toString() (m string) {
	currentWriterMsg := ""
	if stream.currentWriter != nil {
		currentWriterMsg = stream.currentWriter.toString()
	}
	return fmt.Sprintf("ino(%v) currentDataPartion(%v) currentExtentId(%v)",
		stream.Inode, stream.currentPartitionId, currentWriterMsg)
}

func (stream *StreamWriter) toStringWithWriter(writer *ExtentWriter) (m string) {
	currentWriterMsg := writer.toString()
	return fmt.Sprintf("ino(%v) currentDataPartion(%v) currentExtentId(%v)",
		stream.Inode, stream.currentPartitionId, currentWriterMsg)
}

//stream init,alloc a extent ,select dp and extent
func (stream *StreamWriter) init(useNormalExtent bool, prepareWriteSize int) (err error) {
	if stream.currentWriter != nil && (stream.currentWriter.isFullExtent(prepareWriteSize)) {
		err = stream.flushCurrExtentWriter()
		if err == syscall.ENOENT {
			return
		}
		if err != nil {
			return errors.Annotatef(err, "Flush error WriteInit")
		}
		useNormalExtent = true
		stream.setCurrentWriter(nil)
	}
	if stream.currentWriter != nil {
		return
	}
	var writer *ExtentWriter
	writer, err = stream.allocateNewExtentWriter(useNormalExtent)
	if err != nil {
		err = errors.Annotatef(err, "WriteInit AllocNewExtentFailed")
		return err
	}
	stream.setCurrentWriter(writer)
	return
}

func (stream *StreamWriter) server() {
	t := time.NewTicker(time.Second * 5)
	defer t.Stop()
	for {
		select {
		case request := <-stream.requestCh:
			stream.handleRequest(request)
		case <-stream.exitCh:
			stream.flushCurrExtentWriter()
			return
		case <-t.C:
			log.LogDebugf("ino(%v) update to metanode filesize To(%v) user has Write to (%v)",
				stream.Inode, stream.metaNodeStreamKey.Size(), stream.getHasWriteSize())
			if stream.getCurrentWriter() == nil {
				continue
			}
			stream.flushCurrExtentWriter()
		}
	}
}

func (stream *StreamWriter) handleRequest(request interface{}) {
	switch request := request.(type) {
	case *WriteRequest:
		if stream.inodeHasDelete {
			request.err = syscall.ENOENT
			request.done <- struct{}{}
			return
		}
		request.actualOffset = int(stream.getHasWriteSize())
		request.canWrite, request.err = stream.write(request.data, request.actualOffset, request.size)
		stream.addHasWriteSize(request.canWrite)
		request.done <- struct{}{}
	case *FlushRequest:
		request.err = stream.flushCurrExtentWriter()
		request.done <- struct{}{}
	case *CloseRequest:
		request.err = stream.flushCurrExtentWriter()
		if request.err == nil {
			request.err = stream.close()
			request.err = stream.flushCurrExtentWriter()
		}
		request.done <- struct{}{}
		stream.exit()
	default:
	}
}

func (stream *StreamWriter) write(data []byte, offset, size int) (total int, err error) {
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
			stream.Inode, offset, size, total, stream.toString())
		log.LogError(err.Error())
		log.LogError(errors.ErrorStack(err))
	}()

	var (
		initRetry int = 0
	)
	for total < size {
		var useExtent = true
		if offset+size <= MaxTinyExtentSize {
			useExtent = false
		}
		err = stream.init(useExtent, size)
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
		write, err = stream.currentWriter.write(data[total:size], offset, size-total)
		if err == nil {
			write = size - total
			total += write
			continue
		}
		if strings.Contains(err.Error(), FullExtentErr.Error()) {
			continue
		}
		err = stream.recoverExtent()
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

func (stream *StreamWriter) close() (err error) {
	if stream.currentWriter != nil {
		err = stream.currentWriter.close()
	}
	return
}

func (stream *StreamWriter) flushData() (err error) {
	writer := stream.getCurrentWriter()
	if writer == nil {
		err = nil
		return nil
	}
	if err = writer.flush(); err != nil {
		err = errors.Annotatef(err, "writer(%v) Flush Failed", writer.toString())
		log.LogWarnf(err.Error())
		return err
	}
	err = stream.updateToMetaNode()
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
		err = stream.updateToMetaNode()
		if err == syscall.ENOENT {
			return
		}
		if err != nil {
			err = errors.Annotatef(err, "update to MetaNode failed(%v)", err.Error())
			log.LogWarnf(err.Error())
			return err
		}
		stream.currentWriter.notifyRecvThreadExit()
		stream.setCurrentWriter(nil)
	}
	return
}

func (stream *StreamWriter) flushCurrExtentWriter() (err error) {
	defer func() {
		if err == syscall.ENOENT {
			return
		}
		if len(stream.recoverPackages) != 0 {
			err = fmt.Errorf("MaxFlushCurrentExtent failed,packet(%v) not flush %v err(%v)",
				stream.toString(), len(stream.recoverPackages), err)
			log.LogErrorf(err.Error())
		}
	}()
	var errCount = 0
	for {
		err = stream.flushData()
		if err == nil {
			return
		}
		if err == syscall.ENOENT {
			return
		}
		log.LogWarnf("FlushCurrentExtent %v failed,err %v errCnt %v",
			stream.toString(), err.Error(), errCount)
		err = stream.recoverExtent()
		if err == syscall.ENOENT {
			return
		}
		if err == nil {
			err = stream.flushData()
			if err == syscall.ENOENT {
				return
			}
			if err == nil {
				return
			}
		}

		log.LogWarnf("FlushCurrentExtent %v failed,err %v errCnt %v",
			stream.toString(), err.Error(), errCount)
		errCount++
		if errCount > MaxSelectDataPartionForWrite {
			break
		}
	}

	return err
}

func (stream *StreamWriter) setCurrentWriter(writer *ExtentWriter) {
	oldWriter := stream.currentWriter
	if oldWriter != nil {
		oldWriter.notifyRecvThreadExit()
	}
	stream.currentWriter = writer
}

func (stream *StreamWriter) getCurrentWriter() *ExtentWriter {
	return stream.currentWriter
}

func (stream *StreamWriter) updateToMetaNode() (err error) {
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		writer := stream.currentWriter
		if writer == nil {
			log.LogDebugf("updateToMetaNode: ino(%v) nil writer", stream.Inode)
			return
		}

		if writer.isDirty() == false {
			log.LogDebugf("updateToMetaNode: ino(%v) writeer(%v) current extent"+
				" writer not dirty", stream.Inode, writer.toString())
			return
		}

		ek := writer.toKey()                           //first get currentExtent Key
		err = stream.appendExtentKey(stream.Inode, ek) //put it to metanode
		if err == syscall.ENOENT {
			stream.inodeHasDelete = true
			return
		}
		if err != nil {
			err = errors.Annotatef(err, "update extent(%v) to MetaNode Failed", ek.Size)
			log.LogWarnf("stream(%v) err(%v)", stream.toString(), err.Error())
			continue
		}
		writer.clearDirty(ek.Size)
		stream.metaNodeStreamKey.Put(ek)
		return
	}

	return err
}

func (stream *StreamWriter) writeRecoverPackets(writer *ExtentWriter, retryPackets []*Packet) (err error) {
	for _, p := range retryPackets {
		log.LogWarnf("stream(%v) recover packet (%v) kernelOffset(%v) to extent(%v)",
			stream.toString(), p.GetUniqueLogId(), p.kernelOffset, writer.toString())
		_, err = writer.write(p.Data[:p.Size], p.kernelOffset, int(p.Size))
		if err != nil {
			err = errors.Annotatef(err, "pkg(%v) RecoverExtent write failed", p.GetUniqueLogId())
			log.LogWarnf("stream(%v) err(%v)", stream.toStringWithWriter(writer), err.Error())
			stream.excludePartitionId(writer.dp.PartitionID)
			return err
		}
	}
	return
}

func (stream *StreamWriter) recoverExtent() (err error) {
	stream.excludePartitionId(stream.currentWriter.dp.PartitionID)
	stream.currentWriter.notifyRecvThreadExit()
	stream.recoverPackages = stream.currentWriter.getNeedRetrySendPackets() //get need retry
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		if err = stream.updateToMetaNode(); err == nil {
			break
		}
		if err == syscall.ENOENT {
			return err
		}
	}
	if err != nil {
		return err
	}
	if len(stream.recoverPackages) == 0 {
		return nil
	}
	var writer *ExtentWriter
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		err = nil
		stream.excludePartition = make([]uint32, 0)
		stream.excludePartitionId(stream.currentPartitionId)
		if writer, err = stream.allocateNewExtentWriter(true); err != nil { //allocate new extent
			err = errors.Annotatef(err, "RecoverExtent Failed")
			log.LogWarnf("stream(%v) err(%v)", stream.toString(), err.Error())
			continue
		}
		if err = stream.writeRecoverPackets(writer, stream.recoverPackages); err == nil {
			stream.excludePartition = make([]uint32, 0)
			stream.recoverPackages = make([]*Packet, 0)
			stream.setCurrentWriter(writer)
			stream.updateToMetaNode()
			return nil
		} else {
			writer.forbirdUpdateToMetanode()
			writer.notifyRecvThreadExit()
		}
	}

	return err

}

func (stream *StreamWriter) excludePartitionId(partitionId uint32) {
	if stream.excludePartition == nil {
		stream.excludePartition = make([]uint32, 0)
	}
	hasExclude := false
	for _, pId := range stream.excludePartition {
		if pId == partitionId {
			hasExclude = true
			break
		}
	}
	if hasExclude == false {
		stream.excludePartition = append(stream.excludePartition, partitionId)
	}

}

func (stream *StreamWriter) allocateNewExtentWriter(useNormalExtent bool) (writer *ExtentWriter, err error) {
	var (
		dp       *wrapper.DataPartition
		extentId uint64
	)
	err = fmt.Errorf("cannot alloct new extent after maxrery")
	for i := 0; i < MaxSelectDataPartionForWrite; i++ {
		extentId = 0
		if dp, err = gDataWrapper.GetWriteDataPartition(stream.excludePartition); err != nil {
			log.LogWarnf(fmt.Sprintf("stream (%v) ActionAllocNewExtentWriter "+
				"failed on getWriteDataPartion,error(%v) execludeDataPartion(%v)", stream.toString(), err.Error(), stream.excludePartition))
			continue
		}
		if useNormalExtent == true {
			if extentId, err = stream.createExtent(dp); err != nil {
				log.LogWarnf(fmt.Sprintf("stream (%v)ActionAllocNewExtentWriter "+
					"create Extent,error(%v) execludeDataPartion(%v)", stream.toString(), err.Error(), stream.excludePartition))
				if !strings.Contains(err.Error(), "use of closed network connection") {
					stream.excludePartitionId(dp.PartitionID)
				}
				continue
			}
		}
		if writer, err = NewExtentWriter(stream.Inode, dp, extentId); err != nil {
			log.LogWarnf(fmt.Sprintf("stream (%v) ActionAllocNewExtentWriter "+
				"NewExtentWriter(%v),error(%v) execludeDataPartion(%v)", stream.toString(), extentId, err.Error(), stream.excludePartition))
			continue
		}
		err = nil
		break
	}
	if useNormalExtent == true && extentId <= 0 || err != nil {
		log.LogWarnf("allocateNewExtentWriter: err(%v) extentId(%v)", err, extentId)
		return nil, err
	}
	stream.currentPartitionId = dp.PartitionID
	err = nil
	if useNormalExtent {
		writer.storeMode = proto.NormalExtentMode
	} else {
		writer.storeMode = proto.TinyExtentMode
	}
	return writer, nil
}

func (stream *StreamWriter) createExtent(dp *wrapper.DataPartition) (extentId uint64, err error) {
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
	p := NewCreateExtentPacket(dp, stream.Inode)
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

func (stream *StreamWriter) exit() {
	select {
	case stream.exitCh <- true:
	default:
	}
}

func (stream *StreamWriter) getHasWriteSize() uint64 {
	return atomic.LoadUint64(&stream.hasWriteSize)
}

func (stream *StreamWriter) addHasWriteSize(writed int) {
	atomic.AddUint64(&stream.hasWriteSize, uint64(writed))
}

func (stream *StreamWriter) setHasWriteSize(writeSize uint64) {
	atomic.StoreUint64(&stream.hasWriteSize, writeSize)

}
