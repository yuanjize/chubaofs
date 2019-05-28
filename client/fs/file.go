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

package fs

import (
	"io"
	"time"

	"github.com/chubaofs/chubaofs/third_party/fuse"
	"github.com/chubaofs/chubaofs/third_party/fuse/fs"
	"golang.org/x/net/context"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/util"
	"github.com/chubaofs/chubaofs/util/log"
	"sync"
)

type File struct {
	super  *Super
	inode  *Inode
	stream *stream.StreamReader
	sync.RWMutex
}

//functions that File needs to implement
var (
	_ fs.Node              = (*File)(nil)
	_ fs.Handle            = (*File)(nil)
	_ fs.NodeForgetter     = (*File)(nil)
	_ fs.NodeOpener        = (*File)(nil)
	_ fs.HandleReleaser    = (*File)(nil)
	_ fs.HandleReader      = (*File)(nil)
	_ fs.HandleWriter      = (*File)(nil)
	_ fs.HandleFlusher     = (*File)(nil)
	_ fs.NodeFsyncer       = (*File)(nil)
	_ fs.NodeSetattrer     = (*File)(nil)
	_ fs.NodeReadlinker    = (*File)(nil)
	_ fs.NodeGetxattrer    = (*File)(nil)
	_ fs.NodeListxattrer   = (*File)(nil)
	_ fs.NodeSetxattrer    = (*File)(nil)
	_ fs.NodeRemovexattrer = (*File)(nil)
)

func (f *File) getReadStream() (r *stream.StreamReader) {
	f.RLock()
	defer f.RUnlock()
	return f.stream
}

func (f *File) setReadStream(stream *stream.StreamReader) {
	f.Lock()
	defer f.Unlock()
	f.stream = stream
}

func NewFile(s *Super, i *Inode) *File {
	return &File{super: s, inode: i}
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	ino := f.inode.ino
	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Attr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	inode.fillAttr(a)
	fileSize := f.super.ec.GetFileSize(ino)
	writeSize := f.super.ec.GetWriteSize(ino)
	if fileSize > a.Size || writeSize > a.Size {
		a.Size = uint64(util.Max(int(fileSize), int(writeSize)))
	}

	log.LogDebugf("TRACE Attr: inode(%v) attr(%v) fileSize(%v) writeSize(%v) resp(%v)", inode, a, fileSize, writeSize, a)
	return nil
}

func (f *File) Forget() {
	ino := f.inode.ino
	defer func() {
		log.LogDebugf("TRACE Forget: ino(%v)", ino)
	}()

	if f.super.orphan.Evict(ino) {
		if err := f.super.mw.Evict(ino); err != nil {
			log.LogErrorf("Forget: ino(%v) err(%v)", ino, err)
			//TODO: push back to evicted list, and deal with it later
		}
	}

	f.super.ic.Delete(ino)
	if err := f.super.ec.EvictStream(ino); err != nil {
		log.LogWarnf("Forget: stream not ready to evict, ino(%v) err(%v)", ino, err)
	}
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (handle fs.Handle, err error) {
	var flag uint32

	ino := f.inode.ino
	log.LogDebugf("Open enter: ino(%v) req(%v)", ino, req)
	start := time.Now()
	if req.Flags.IsWriteOnly() || req.Flags.IsReadWrite() {
		flag = proto.FlagWrite
	}
	err = f.super.ec.OpenStream(ino, flag)
	if err != nil {
		log.LogErrorf("Open: failed to get write authorization, ino(%v) req(%v) err(%v)", ino, req, err)
		return nil, fuse.EPERM
	}
	if req.Flags.IsReadWrite() || req.Flags.IsReadOnly(){
		stream, err := f.super.ec.OpenForRead(f.inode.ino)
		if err != nil {
			log.LogErrorf("Open for Read: ino(%v) err(%v)", f.inode.ino, err)
			return nil,fuse.EPERM
		}
		f.setReadStream(stream)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Open: ino(%v) flags(%v) (%v)ns", ino, req.Flags, elapsed.Nanoseconds())
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) (err error) {
	var flag uint32

	ino := f.inode.ino
	start := time.Now()

	if req.Flags.IsWriteOnly() || req.Flags.IsReadWrite() {
		flag = proto.FlagWrite
	}

	f.super.ic.Delete(ino)
	err = f.super.ec.CloseStream(ino, flag)
	if err != nil {
		log.LogErrorf("Release: close writer failed, ino(%v) req(%v) err(%v)", ino, req, err)
		return fuse.EIO
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Release: ino(%v) req(%v) (%v)ns", ino, req, elapsed.Nanoseconds())
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) (err error) {
	if f.getReadStream() == nil {
		stream, err := f.super.ec.OpenForRead(f.inode.ino)
		if err != nil {
			log.LogErrorf("Open for Read: ino(%v) err(%v)", f.inode.ino, err)
			return fuse.EPERM
		}
		f.setReadStream(stream)
	}
	start := time.Now()
	size, err := f.super.ec.Read(f.getReadStream(), f.inode.ino, resp.Data[fuse.OutHeaderSize:], int(req.Offset), req.Size)
	if err != nil && err != io.EOF {
		log.LogErrorf("Read: ino(%v) req(%v) err(%v) size(%v)", f.inode.ino, req, err, size)
		return fuse.EIO
	}
	if size > req.Size {
		log.LogErrorf("Read: ino(%v) req(%v) size(%v)", f.inode.ino, req, size)
		return fuse.ERANGE
	}
	if size > 0 {
		resp.Data = resp.Data[:size+fuse.OutHeaderSize]
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Read: ino(%v) req(%v) size(%v) (%v)ns", f.inode.ino, req, size, elapsed.Nanoseconds())
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) (err error) {
	ino := f.inode.ino
	reqlen := len(req.Data)
	writeSize := f.super.ec.GetWriteSize(ino)

	log.LogDebugf("TRACE Write enter: ino(%v) offset(%v) len(%v) req(%v)",
		ino, req.Offset, reqlen, req)

	if uint64(req.Offset) > writeSize && reqlen == 1 && req.Data[0] == 0 {
		// The user is doing posix_fallocate.
		log.LogWarnf("Write: fallocate, ino(%v) req(%v) writeSize(%v)", ino, req, writeSize)
		f.super.ec.SetFileSize(ino, uint64(req.Offset)+uint64(reqlen))
		return nil
	}

	defer func() {
		f.super.ic.Delete(f.inode.ino)
	}()

	start := time.Now()
	size, actualOffset, err := f.super.ec.Write(ino, int(req.Offset), req.Data)
	if err != nil {
		log.LogErrorf("Write: ino(%v) offset(%v) actualOffset(%v) len(%v) err(%v)", ino, req.Offset, actualOffset, reqlen, err)
		return fuse.EIO
	}
	resp.Size = size
	if size != reqlen {
		log.LogErrorf("Write: ino(%v) offset(%v) actualOffset(%v) len(%v) size(%v)", ino, req.Offset, actualOffset, reqlen, size)
	}

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Write exit: ino(%v) offset(%v) actualOffset(%v) len(%v) req(%v) (%v)ns ",
		f.inode.ino, req.Offset, actualOffset, reqlen, req, elapsed.Nanoseconds())
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) (err error) {
	return fuse.ENOSYS
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) (err error) {
	start := time.Now()
	err = f.super.ec.Flush(f.inode.ino)
	if err != nil {
		log.LogErrorf("Fsync: ino(%v) err(%v)", f.inode.ino, err)
		return fuse.EIO
	}
	f.super.ic.Delete(f.inode.ino)
	elapsed := time.Since(start)
	log.LogDebugf("TRACE Fsync: ino(%v) (%v)ns", f.inode.ino, elapsed.Nanoseconds())
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	ino := f.inode.ino
	start := time.Now()
	if req.Valid.Size() && req.Size == 0 {
		err := f.super.mw.Truncate(ino)
		if err != nil {
			log.LogErrorf("Setattr: truncate ino(%v) err(%v)", ino, err)
			return ParseError(err)
		}
		f.super.ic.Delete(ino)
		f.super.ec.SetWriteSize(ino, 0)
		f.super.ec.SetFileSize(ino, 0)
	}

	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Setattr: ino(%v) err(%v)", ino, err)
		return ParseError(err)
	}

	if req.Valid.Size() {
		if req.Size != inode.size {
			log.LogWarnf("Setattr: truncate ino(%v) reqSize(%v) inodeSize(%v)", ino, req.Size, inode.size)
		}
	}

	if req.Valid.Mode() {
		inode.osMode = req.Mode
	}

	inode.fillAttr(&resp.Attr)

	elapsed := time.Since(start)
	log.LogDebugf("TRACE Setattr: ino(%v) req(%v) (%v)ns", ino, req, elapsed.Nanoseconds())
	return nil
}

func (f *File) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	ino := f.inode.ino
	inode, err := f.super.InodeGet(ino)
	if err != nil {
		log.LogErrorf("Readlink: ino(%v) err(%v)", ino, err)
		return "", ParseError(err)
	}
	log.LogDebugf("TRACE Readlink: ino(%v) target(%v)", ino, string(inode.target))
	return string(inode.target), nil
}

func (f *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ENOSYS
}

func (f *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ENOSYS
}

func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ENOSYS
}

func (f *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ENOSYS
}
