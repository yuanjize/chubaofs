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
	"os"
	"time"

	"github.com/chubaofs/chubaofs/third_party/fuse"

	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/util/log"
)

const (
	LogTimeFormat = "20060102150405000"
)

func (s *Super) InodeGet(ino uint64) (*proto.InodeInfo, error) {
	inode := s.ic.Get(ino)
	if inode != nil {
		//log.LogDebugf("InodeGet: InodeCache hit, inode(%v)", inode)
		return inode, nil
	}

	info, err := s.mw.InodeGet_ll(ino)
	if err != nil || info == nil {
		log.LogErrorf("InodeGet: ino(%v) err(%v) info(%v)", ino, err, info)
		if err != nil {
			return nil, ParseError(err)
		} else {
			return nil, fuse.ENOENT
		}
	}
	inode = info
	s.ic.Put(inode)
	//log.LogDebugf("InodeGet: inode(%v)", inode)
	return inode, nil
}

func fillAttr(info *proto.InodeInfo, attr *fuse.Attr) {
	attr.Valid = AttrValidDuration
	attr.Nlink = info.Nlink
	attr.Inode = info.Inode
	attr.Size = info.Size
	attr.Blocks = attr.Size >> 9 // In 512 bytes
	attr.Atime = info.AccessTime
	attr.Ctime = info.CreateTime
	attr.Mtime = info.ModifyTime
	attr.BlockSize = DefaultBlksize

	switch info.Mode {
	case proto.ModeDir:
		attr.Mode = os.ModeDir | os.ModePerm
	case proto.ModeSymlink:
		attr.Mode = os.ModeSymlink | os.ModePerm
	default:
		attr.Mode = os.ModePerm
	}
}

func inodeExpired(info *proto.InodeInfo) bool {
	if time.Now().UnixNano() > info.Expiration() {
		return true
	}
	return false
}

func inodeSetExpiration(info *proto.InodeInfo, t time.Duration) {
	info.SetExpiration(time.Now().Add(t).UnixNano())
}
