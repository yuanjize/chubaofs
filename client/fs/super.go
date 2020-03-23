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
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/chubaofs/chubaofs/third_party/fuse"
	"github.com/chubaofs/chubaofs/third_party/fuse/fs"
	"golang.org/x/net/context"

	"github.com/chubaofs/chubaofs/sdk/data/stream"
	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
)

type MountOption struct {
	MountPoint    string
	Volname       string
	Owner         string
	Master        string
	Logpath       string
	Loglvl        string
	Profport      string
	Rdonly        bool
	IcacheTimeout int64
	LookupValid   int64
	AttrValid     int64
	ReadRate      int64
	WriteRate     int64
	EnSyncWrite   int64
	AutoInvalData int64
	UmpDatadir    string
	Token         string
	ExtentSize    int64
	DisableDcache bool
}

func (mo *MountOption) String() string {
	return fmt.Sprint(
		"\nMountPoint: ", mo.MountPoint,
		"\nVolname: ", mo.Volname,
		"\nOwner: ", mo.Owner,
		"\nMaster: ", mo.Master,
		"\nLogpath: ", mo.Logpath,
		"\nLoglvl: ", mo.Loglvl,
		"\nProfport: ", mo.Profport,
		"\nRdonly: ", mo.Rdonly,
		"\nIcacheTimeout: ", mo.IcacheTimeout,
		"\nLookupValid: ", mo.LookupValid,
		"\nAttrValid: ", mo.AttrValid,
		"\nUmpDatadir: ", mo.UmpDatadir,
		"\nToken: ", mo.Token,
		"\nExtentSize: ", mo.ExtentSize,
		"\nDisableDcache: ", mo.DisableDcache,
		"\n",
	)
}

type Super struct {
	cluster string
	volname string
	localIP string
	ic      *InodeCache
	mw      *meta.MetaWrapper
	ec      *stream.ExtentClient
	orphan  *OrphanInodeList

	nodeCache map[uint64]fs.Node
	fslock    sync.Mutex

	disableDcache bool
}

//functions that Super needs to implement
var (
	_ fs.FS         = (*Super)(nil)
	_ fs.FSStatfser = (*Super)(nil)
)

func NewSuper(opt *MountOption) (s *Super, err error) {
	s = new(Super)
	s.mw, err = meta.NewMetaWrapper(opt.Volname, opt.Master)
	if err != nil {
		log.LogErrorf("NewMetaWrapper failed! %v", err.Error())
		return nil, err
	}

	s.ec, err = stream.NewExtentClient(opt.Volname, opt.Master, opt.ReadRate, opt.WriteRate, opt.ExtentSize, s.mw.AppendExtentKey, s.mw.GetExtents)
	if err != nil {
		log.LogErrorf("NewExtentClient failed! %v", err.Error())
		return nil, err
	}

	s.volname = opt.Volname
	s.cluster = s.mw.Cluster()
	s.localIP = s.mw.LocalIP()
	inodeExpiration := DefaultInodeExpiration
	if opt.IcacheTimeout >= 0 {
		inodeExpiration = time.Duration(opt.IcacheTimeout) * time.Second
	}
	if opt.LookupValid >= 0 {
		LookupValidDuration = time.Duration(opt.LookupValid) * time.Second
	}
	if opt.AttrValid >= 0 {
		AttrValidDuration = time.Duration(opt.AttrValid) * time.Second
	}
	s.ic = NewInodeCache(inodeExpiration, MaxInodeCache)
	s.orphan = NewOrphanInodeList()
	s.nodeCache = make(map[uint64]fs.Node)
	s.disableDcache = opt.DisableDcache

	tokenType, err := s.mw.QueryTokenType(opt.Volname, opt.Token)
	if err != nil {
		log.LogErrorf("QueryTokenType failed! %v", err.Error())
		return nil, err
	}
	if opt.Rdonly && opt.Token==""{
		opt.Rdonly=true
	}else {
		opt.Rdonly = tokenType == proto.ReadOnlyToken
	}
	log.LogInfof("NewSuper: cluster(%v) volname(%v) icacheExpiration(%v) LookupValidDuration(%v) AttrValidDuration(%v)", s.cluster, s.volname, inodeExpiration, LookupValidDuration, AttrValidDuration)
	return s, nil
}

func (s *Super) Root() (fs.Node, error) {
	inode, err := s.InodeGet(RootInode)
	if err != nil {
		return nil, err
	}
	root := NewDir(s, inode)
	return root, nil
}

func (s *Super) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	total, used := s.mw.Statfs()
	resp.Blocks = total / uint64(DefaultBlksize)
	resp.Bfree = (total - used) / uint64(DefaultBlksize)
	resp.Bavail = resp.Bfree
	resp.Bsize = DefaultBlksize
	resp.Namelen = DefaultMaxNameLen
	resp.Frsize = DefaultBlksize
	return nil
}

func (s *Super) GetRate(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(s.ec.GetRate()))
}

func (s *Super) SetRate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if rate := r.FormValue("read"); rate != "" {
		val, err := strconv.Atoi(rate)
		if err != nil {
			w.Write([]byte("Set read rate failed\n"))
		} else {
			msg := s.ec.SetReadRate(val)
			w.Write([]byte(fmt.Sprintf("Set read rate to %v successfully\n", msg)))
		}
	}

	if rate := r.FormValue("write"); rate != "" {
		val, err := strconv.Atoi(rate)
		if err != nil {
			w.Write([]byte("Set write rate failed\n"))
		} else {
			msg := s.ec.SetWriteRate(val)
			w.Write([]byte(fmt.Sprintf("Set write rate to %v successfully\n", msg)))
		}
	}
}

func (s *Super) umpKey(act string) string {
	return fmt.Sprintf("%s_fuseclient_%s", s.cluster, act)
}
