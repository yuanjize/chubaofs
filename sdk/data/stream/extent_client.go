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
	"sync"

	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/sdk/data/wrapper"
	"github.com/tiglabs/containerfs/third_party/juju/errors"
	"github.com/tiglabs/containerfs/util/log"
	"github.com/tiglabs/containerfs/util/ump"
	"io"
	"runtime"
	"strings"
)

type AppendExtentKeyFunc func(inode uint64, key proto.ExtentKey) error
type GetExtentsFunc func(inode uint64) ([]proto.ExtentKey, error)

var (
	gDataWrapper       *wrapper.Wrapper
	openRequestPool    *sync.Pool
	writeRequestPool   *sync.Pool
	flushRequestPool   *sync.Pool
	releaseRequestPool *sync.Pool
	evictRequestPool   *sync.Pool
)

type ExtentClient struct {
	writers         map[uint64]*StreamWriter
	writerLock      sync.RWMutex
	appendExtentKey AppendExtentKeyFunc
	getExtents      GetExtentsFunc
}

func NewExtentClient(volname, master string, appendExtentKey AppendExtentKeyFunc, getExtents GetExtentsFunc) (client *ExtentClient, err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	client = new(ExtentClient)
	gDataWrapper, err = wrapper.NewDataPartitionWrapper(volname, master)
	if err != nil {
		return nil, fmt.Errorf("init dp Wrapper failed (%v)", err.Error())
	}
	client.writers = make(map[uint64]*StreamWriter)
	client.appendExtentKey = appendExtentKey
	client.getExtents = getExtents
	writeRequestPool = &sync.Pool{New: func() interface{} {
		return &WriteRequest{}
	}}
	openRequestPool = &sync.Pool{New: func() interface{} {
		return &OpenRequest{}
	}}
	flushRequestPool = &sync.Pool{New: func() interface{} {
		return &FlushRequest{}
	}}
	releaseRequestPool = &sync.Pool{New: func() interface{} {
		return &ReleaseRequest{}
	}}
	evictRequestPool = &sync.Pool{New: func() interface{} {
		return &EvictRequest{}
	}}

	return
}

func (client *ExtentClient) getStreamWriter(inode uint64) (stream *StreamWriter) {
	client.writerLock.RLock()
	stream = client.writers[inode]
	client.writerLock.RUnlock()

	return
}

func (client *ExtentClient) OpenStream(inode uint64, flag uint32) (err error) {
	client.writerLock.Lock()
	s, ok := client.writers[inode]
	if !ok {
		s = NewStreamWriter(inode, client, client.appendExtentKey)
		client.writers[inode] = s
	}
	client.writerLock.Unlock()
	return s.IssueOpenRequest(flag)
}

func (client *ExtentClient) CloseStream(inode uint64, flag uint32) (err error) {
	client.writerLock.Lock()
	s, ok := client.writers[inode]
	if !ok {
		client.writerLock.Unlock()
		return
	}
	client.writerLock.Unlock()
	return s.IssueReleaseRequest(flag)
}

func (client *ExtentClient) EvictStream(inode uint64) error {
	client.writerLock.Lock()
	s, ok := client.writers[inode]
	if !ok {
		client.writerLock.Unlock()
		return nil
	}
	err := s.IssueEvictRequest()
	if err != nil {
		client.writerLock.Unlock()
		return err
	}
	delete(client.writers, inode)
	client.writerLock.Unlock()

	s.exit()
	return nil
}

func (client *ExtentClient) getStreamWriterForRead(inode uint64) (stream *StreamWriter) {
	client.writerLock.RLock()
	defer client.writerLock.RUnlock()
	stream = client.writers[inode]

	return
}

func (client *ExtentClient) Write(inode uint64, offset int, data []byte) (write int, actualOffset int, err error) {
	stream := client.getStreamWriter(inode)
	if stream == nil {
		prefix := fmt.Sprintf("inodewrite %v_%v_%v", inode, offset, len(data))
		return 0, 0, fmt.Errorf("Prefix(%v) cannot init write stream", prefix)
	}

	write,actualOffset,err=stream.IssueWriteRequest(offset,data)
	if err != nil {
		prefix := fmt.Sprintf("inodewrite %v_%v_%v", inode, offset, len(data))
		err = errors.Annotatef(err, prefix)
		log.LogError(errors.ErrorStack(err))
		if !strings.Contains(err.Error(), io.EOF.Error()) {
			mesg := fmt.Sprintf("volname %v Write error %v", wrapper.GVolname, err.Error())
			log.LogErrorf(mesg)
			ump.Alarm(gDataWrapper.UmpWarningKey(), fmt.Sprintf("volname(%v) write error", wrapper.GVolname, err.Error()))
		}
	}
	return
}

func (client *ExtentClient) OpenForRead(inode uint64) (stream *StreamReader, err error) {
	return NewStreamReader(inode, client.getExtents)
}

func (client *ExtentClient) GetWriteSize(inode uint64) uint64 {
	client.writerLock.RLock()
	defer client.writerLock.RUnlock()
	writer, ok := client.writers[inode]
	if !ok {
		return 0
	}
	return writer.getHasWriteSize()
}

func (client *ExtentClient) SetWriteSize(inode, size uint64) {
	client.writerLock.Lock()
	defer client.writerLock.Unlock()
	writer, ok := client.writers[inode]
	if ok {
		writer.setHasWriteSize(size)
	}
}

func (client *ExtentClient) release(inode uint64) {
	client.writerLock.Lock()
	defer client.writerLock.Unlock()
	delete(client.writers, inode)

}

func (client *ExtentClient) Flush(inode uint64) (err error) {
	stream := client.getStreamWriterForRead(inode)
	if stream == nil {
		return nil
	}
	err=stream.IssueFlushRequest()
	if err != nil {
		mesg := fmt.Sprintf("volname %v Flush %v", wrapper.GVolname, err.Error())
		log.LogErrorf(mesg)
	}
	return err
}

func (client *ExtentClient) Read(stream *StreamReader, inode uint64, data []byte, offset int, size int) (read int, err error) {
	if size == 0 {
		return
	}

	defer func() {
		if err != nil && err != io.EOF {
			mesg := fmt.Sprintf("volname %v readError %v", wrapper.GVolname, err.Error())
			log.LogErrorf(mesg)
			ump.Alarm(gDataWrapper.UmpWarningKey(), fmt.Sprintf("volname %v readError %v", wrapper.GVolname, err.Error()))
		}
	}()

	wstream := client.getStreamWriterForRead(inode)
	if wstream != nil {
		err=wstream.IssueFlushRequest()
		if err != nil {
			return 0, err
		}
	}
	read, err = stream.read(data, offset, size)

	return
}
