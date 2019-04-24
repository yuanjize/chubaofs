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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chubaofs/cfs/master"
	"github.com/chubaofs/cfs/proto"
	"github.com/chubaofs/cfs/storage"
	"github.com/chubaofs/cfs/third_party/juju/errors"
	"github.com/chubaofs/cfs/util/log"
	"syscall"
)

const (
	DataPartitionPrefix       = "datapartition"
	DataPartitionMetaFileName = "META"
	TimeLayout                = "2006-01-02 15:04:05"
)

var (
	AdminGetDataPartition = master.AdminGetDataPartition
	ErrNotLeader          = errors.New("not leader")
)

type DataPartitionMeta struct {
	VolumeId      string
	PartitionType string
	PartitionId   uint32
	PartitionSize int
	CreateTime    string
}

func (meta *DataPartitionMeta) Validate() (err error) {
	meta.VolumeId = strings.TrimSpace(meta.VolumeId)
	meta.PartitionType = strings.TrimSpace(meta.PartitionType)
	if len(meta.VolumeId) == 0 || len(meta.PartitionType) == 0 ||
		meta.PartitionId == 0 || meta.PartitionSize == 0 {
		err = errors.New("illegal data partition meta")
		return
	}
	return
}

type DataPartition struct {
	volumeId        string
	partitionId     uint32
	partitionStatus int
	partitionSize   int
	replicaHosts    []string
	disk            *Disk
	isLeader        bool
	path            string
	used            int
	extentStore     *storage.ExtentStore
	stopC           chan bool

	runtimeMetrics          *DataPartitionMetrics
	updateReplicationTime   int64
	updatePartitionSizeTime int64

	snapshot               []*proto.File
	snapshotLock           sync.RWMutex
	loadExtentHeaderStatus int
	useTinyExtentCnt       int32
}

func CreateDataPartition(volId string, partitionId uint32, disk *Disk, size int, partitionType string) (dp *DataPartition, err error) {

	if dp, err = newDataPartition(volId, partitionId, disk, size); err != nil {
		return
	}
	// Store meta information into meta file.
	var (
		metaFile *os.File
		metaData []byte
	)
	metaFilePath := path.Join(dp.Path(), DataPartitionMetaFileName)
	if metaFile, err = os.OpenFile(metaFilePath, os.O_CREATE|os.O_RDWR, 0666); err != nil {
		return
	}
	defer metaFile.Close()
	meta := &DataPartitionMeta{
		VolumeId:      volId,
		PartitionId:   partitionId,
		PartitionType: partitionType,
		PartitionSize: size,
		CreateTime:    time.Now().Format(TimeLayout),
	}
	if metaData, err = json.Marshal(meta); err != nil {
		return
	}
	if _, err = metaFile.Write(metaData); err != nil {
		return
	}

	go dp.ForceLoadHeader()

	return
}

// LoadDataPartition load and returns partition instance from specified directory.
// This method will read the partition meta file stored under the specified directory
// and create partition instance.
func LoadDataPartition(partitionDir string, disk *Disk) (dp *DataPartition, err error) {
	var (
		metaFileData []byte
	)
	if metaFileData, err = ioutil.ReadFile(path.Join(partitionDir, DataPartitionMetaFileName)); err != nil {
		return
	}
	meta := &DataPartitionMeta{}
	if err = json.Unmarshal(metaFileData, meta); err != nil {
		return
	}
	if err = meta.Validate(); err != nil {
		return
	}
	dp, err = newDataPartition(meta.VolumeId, meta.PartitionId, disk, meta.PartitionSize)
	return
}

func newDataPartition(volumeId string, partitionId uint32, disk *Disk, size int) (dp *DataPartition, err error) {
	partition := &DataPartition{
		volumeId:               volumeId,
		partitionId:            partitionId,
		disk:                   disk,
		path:                   path.Join(disk.Path, fmt.Sprintf(DataPartitionPrefix+"_%v_%v", partitionId, size)),
		partitionSize:          size,
		replicaHosts:           make([]string, 0),
		stopC:                  make(chan bool, 0),
		partitionStatus:        proto.ReadWrite,
		runtimeMetrics:         NewDataPartitionMetrics(),
		loadExtentHeaderStatus: StartLoadDataPartitionExtentHeader,
	}
	partition.extentStore, err = storage.NewExtentStore(partition.path, partitionId, size)
	if err != nil {
		return
	}
	disk.AttachDataPartition(partition)
	dp = partition
	go partition.statusUpdateScheduler()
	return
}

func (dp *DataPartition) ID() uint32 {
	return dp.partitionId
}

func (dp *DataPartition) GetExtentCount() int {
	return dp.extentStore.GetExtentCount()
}

func (dp *DataPartition) Path() string {
	return dp.path
}

func (dp *DataPartition) IsLeader() bool {
	return dp.isLeader
}

func (dp *DataPartition) ReplicaHosts() []string {
	return dp.replicaHosts
}

func (dp *DataPartition) LoadExtentHeaderStatus() int {
	return dp.loadExtentHeaderStatus
}

func (dp *DataPartition) ReloadSnapshot() {
	if dp.loadExtentHeaderStatus != FinishLoadDataPartitionExtentHeader {
		return
	}
	files, err := dp.extentStore.SnapShotNormalExtent()
	if err != nil {
		return
	}
	dp.snapshotLock.Lock()
	dp.snapshot = files
	dp.snapshotLock.Unlock()
}

func (dp *DataPartition) GetSnapShot() (files []*proto.File) {
	dp.snapshotLock.RLock()
	defer dp.snapshotLock.RUnlock()

	return dp.snapshot
}

func (dp *DataPartition) Stop() {
	if dp.stopC != nil {
		close(dp.stopC)
	}
	// Close all store and backup partition data file.
	dp.extentStore.Close()

}

func (dp *DataPartition) FlushDelete() (err error) {
	err = dp.extentStore.FlushDelete()
	return
}

func (dp *DataPartition) Disk() *Disk {
	return dp.disk
}

func (dp *DataPartition) Status() int {
	return dp.partitionStatus
}

func (dp *DataPartition) Size() int {
	return dp.partitionSize
}

func (dp *DataPartition) Used() int {
	return dp.used
}

func (dp *DataPartition) Available() int {
	return dp.partitionSize - dp.used
}

func (dp *DataPartition) ChangeStatus(status int) {
	switch status {
	case proto.ReadOnly, proto.ReadWrite, proto.Unavaliable:
		dp.partitionStatus = status
	}
}

func (dp *DataPartition) ForceLoadHeader() {
	dp.extentStore.BackEndLoadExtent()
	dp.loadExtentHeaderStatus = FinishLoadDataPartitionExtentHeader
}

func (dp *DataPartition) statusUpdateScheduler() {
	ticker := time.NewTicker(10 * time.Second)
	metricTicker := time.NewTicker(5 * time.Second)
	var index int
	for {
		select {
		case <-ticker.C:
			dp.statusUpdate()
			index++
			if index >= math.MaxUint32 {
				index = 0
			}
			if index%2 == 0 {
				dp.LaunchRepair(proto.TinyExtentMode)
			} else {
				dp.LaunchRepair(proto.NormalExtentMode)
			}
			dp.ReloadSnapshot()
		case <-dp.stopC:
			ticker.Stop()
			metricTicker.Stop()
			return
		case <-metricTicker.C:
			dp.runtimeMetrics.recomputLatency()
		}
	}
}

func (dp *DataPartition) statusUpdate() {
	status := proto.ReadWrite
	dp.computeUsage()
	if dp.used >= dp.partitionSize {
		status = proto.ReadOnly
	}
	if dp.extentStore.GetExtentCount() >= MaxActiveExtents {
		status = proto.ReadOnly
	}
	dp.partitionStatus = int(math.Min(float64(status), float64(dp.disk.Status)))
}

func ParseExtentId(filename string) (extentId uint64, isExtent bool) {
	if isExtent = storage.RegexpExtentFile.MatchString(filename); !isExtent {
		return
	}
	var (
		err error
	)
	if extentId, err = strconv.ParseUint(filename, 10, 64); err != nil {
		isExtent = false
		return
	}
	isExtent = true
	return
}

func (dp *DataPartition) getRealSize(path string, finfo os.FileInfo) (size int64) {
	name := finfo.Name()
	defer func() {
		if size<0 {
			size=0
		}
	}()
	extentid, isExtent := ParseExtentId(name)
	if !isExtent {
		size=0
		return
	}
	if storage.IsTinyExtent(extentid) {
		stat := new(syscall.Stat_t)
		err := syscall.Stat(fmt.Sprintf("%v/%v", path, finfo.Name()), stat)
		if err != nil {
			return finfo.Size()
		}
		size=stat.Blocks * DiskSectorSize
	} else {
		size=finfo.Size()
	}
	return size
}

func (dp *DataPartition) computeUsage() {
	var (
		used  int64
		files []os.FileInfo
		err   error
	)
	if time.Now().Unix()-dp.updatePartitionSizeTime < UpdatePartitionSizeTime {
		return
	}
	if files, err = ioutil.ReadDir(dp.path); err != nil {
		return
	}
	for _, file := range files {
		used += dp.getRealSize(dp.path, file)
	}
	dp.used = int(used)
	dp.updatePartitionSizeTime = time.Now().Unix()
}

func (dp *DataPartition) GetExtentStore() *storage.ExtentStore {
	return dp.extentStore
}

func (dp *DataPartition) String() (m string) {
	return fmt.Sprintf(DataPartitionPrefix+"_%v_%v", dp.partitionId, dp.partitionSize)
}

func (dp *DataPartition) LaunchRepair(fixExtentType uint8) {
	if dp.partitionStatus == proto.Unavaliable {
		return
	}
	select {
	case <-dp.stopC:
		return
	default:
	}
	if err := dp.updateReplicaHosts(); err != nil {
		log.LogErrorf("action[LaunchRepair] err[%v].", err)
		return
	}
	if !dp.isLeader {
		return
	}
	if dp.extentStore.GetUnAvaliExtentLen() == 0 {
		dp.extentStore.MoveAvaliExtentToUnavali(MinFixTinyExtents)
	}
	dp.extentFileRepair(fixExtentType)
}

func (dp *DataPartition) updateReplicaHosts() (err error) {
	if time.Now().Unix()-dp.updateReplicationTime <= UpdateReplicationHostsTime {
		return
	}
	dp.isLeader = false
	isLeader, replicas, err := dp.fetchReplicaHosts()
	if err != nil {
		dp.updateReplicationTime = time.Now().Unix()
		return
	}
	if !dp.compareReplicaHosts(dp.replicaHosts, replicas) {
		log.LogInfof(fmt.Sprintf("action[updateReplicaHosts] partition[%v] replicaHosts changed from [%v] to [%v].",
			dp.partitionId, dp.replicaHosts, replicas))
	}
	dp.isLeader = isLeader
	dp.replicaHosts = replicas
	dp.updateReplicationTime = time.Now().Unix()
	log.LogInfof(fmt.Sprintf("ActionUpdateReplicationHosts partiton[%v]", dp.partitionId))
	return
}

func (dp *DataPartition) compareReplicaHosts(v1, v2 []string) (equals bool) {
	// Compare fetched replica hosts with local stored hosts.
	equals = true
	if len(v1) == len(v2) {
		for i := 0; i < len(v1); i++ {
			if v1[i] != v2[i] {
				equals = false
				return
			}
		}
		equals = true
		return
	}
	equals = false
	return
}

func (dp *DataPartition) fetchReplicaHosts() (isLeader bool, replicaHosts []string, err error) {
	var (
		HostsBuf []byte
	)
	params := make(map[string]string)
	params["id"] = strconv.Itoa(int(dp.partitionId))
	if HostsBuf, err = MasterHelper.Request("GET", AdminGetDataPartition, params, nil); err != nil {
		isLeader = false
		return
	}
	response := &master.DataPartition{}
	replicaHosts = make([]string, 0)
	if err = json.Unmarshal(HostsBuf, &response); err != nil {
		isLeader = false
		replicaHosts = nil
		return
	}
	for _, host := range response.PersistenceHosts {
		replicaHosts = append(replicaHosts, host)
	}
	if response.PersistenceHosts != nil && len(response.PersistenceHosts) >= 1 {
		leaderAddr := response.PersistenceHosts[0]
		leaderAddrParts := strings.Split(leaderAddr, ":")
		if len(leaderAddrParts) == 2 && strings.TrimSpace(leaderAddrParts[0]) == LocalIP {
			isLeader = true
		}
	}
	return
}

func (dp *DataPartition) Load() (response *proto.LoadDataPartitionResponse) {
	response = &proto.LoadDataPartitionResponse{}
	response.PartitionId = uint64(dp.partitionId)
	response.PartitionStatus = dp.partitionStatus
	response.Used = uint64(dp.Used())
	var err error
	if dp.loadExtentHeaderStatus != FinishLoadDataPartitionExtentHeader {
		response.PartitionSnapshot = make([]*proto.File, 0)
	} else {
		response.PartitionSnapshot = dp.GetSnapShot()
	}
	if err != nil {
		response.Status = proto.TaskFail
		response.Result = err.Error()
		return
	}
	return
}

func (dp *DataPartition) MergeExtentStoreRepair(metas *MembersFileMetas) {
	store := dp.extentStore
	for _, addExtent := range metas.NeedAddExtentsTasks {
		if storage.IsTinyExtent(addExtent.FileId) {
			continue
		}
		if store.IsExistExtent(uint64(addExtent.FileId)) {
			fixFileSizeTask := &storage.FileInfo{Source: addExtent.Source, FileId: addExtent.FileId, Size: addExtent.Size, Inode: addExtent.Inode}
			metas.NeedFixExtentSizeTasks = append(metas.NeedFixExtentSizeTasks, fixFileSizeTask)
			continue
		}
		err := store.Create(uint64(addExtent.FileId), addExtent.Inode)
		if err != nil {
			continue
		}
		fixFileSizeTask := &storage.FileInfo{Source: addExtent.Source, FileId: addExtent.FileId, Size: addExtent.Size}
		metas.NeedFixExtentSizeTasks = append(metas.NeedFixExtentSizeTasks, fixFileSizeTask)
	}

	var (
		wg           *sync.WaitGroup
		recoverIndex int
	)
	wg = new(sync.WaitGroup)
	for _, fixExtent := range metas.NeedFixExtentSizeTasks {
		if !store.IsExistExtent(uint64(fixExtent.FileId)) {
			continue
		}
		wg.Add(1)
		go dp.doStreamExtentFixRepair(wg, fixExtent)
		recoverIndex++
		if recoverIndex%SimultaneouslyRecoverFiles == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
	for extentId, extentSize := range metas.LeaderTinyExtentRealSize {
		dp.extentStore.UpdateTinyExtentRealSize(extentId, extentSize)
	}
}

func (dp *DataPartition) AddWriteMetrics(latency uint64) {
	dp.runtimeMetrics.AddWriteMetrics(latency)
}

func (dp *DataPartition) AddReadMetrics(latency uint64) {
	dp.runtimeMetrics.AddReadMetrics(latency)
}
