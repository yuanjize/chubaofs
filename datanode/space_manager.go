package datanode

import (
	"fmt"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util/log"
	"math"
	"sync"
	"time"
)
// 目前来看是sm包含disk，disk和partition有直接关系
type SpaceManager interface {
	LoadDisk(path string, restSize uint64, maxErrs int) (err error)
	GetDisk(path string) (d *Disk, err error)
	PutPartition(dp DataPartition)
	GetPartition(partitionId uint32) (dp DataPartition)
	Stats() *Stats
	GetDisks() []*Disk
	CreatePartition(volId string, partitionId uint32, storeSize int, storeType string) (DataPartition, error)
	DeletePartition(partitionId uint32)
	RangePartitions(f func(partition DataPartition) bool)
	Stop()
}

type spaceManager struct {
	disks       map[string]*Disk
	partitions  map[uint32]DataPartition
	diskMu      sync.RWMutex
	partitionMu sync.RWMutex
	stats       *Stats  // metric用
	stopC       chan bool
}

func NewSpaceManager(rack string) SpaceManager {
	var space *spaceManager
	space = &spaceManager{}
	space.disks = make(map[string]*Disk)
	space.partitions = make(map[uint32]DataPartition)
	space.stats = NewStats(rack)
	space.stopC = make(chan bool, 0)

	go space.statUpdateScheduler()
	go space.fileRepairScheduler()
	go space.flushDeleteScheduler()

	return space
}

func (space *spaceManager) Stop() {
	defer func() {
		recover()
	}()
	close(space.stopC)
}
// 使用f，便利所有的分区
func (space *spaceManager) RangePartitions(f func(partition DataPartition) bool) {
	if f == nil {
		return
	}
	space.partitionMu.RLock()
	partitions := make([]DataPartition, 0)
	for _, dp := range space.partitions {
		partitions = append(partitions, dp)
	}
	space.partitionMu.RUnlock()

	for _, partition := range partitions {
		if !f(partition) {
			break
		}
	}
}
// 获取所有的Disk(map转换slice)
func (space *spaceManager) GetDisks() (disks []*Disk) {
	space.diskMu.RLock()
	defer space.diskMu.RUnlock()
	disks = make([]*Disk, 0)
	for _, disk := range space.disks {
		disks = append(disks, disk)
	}
	return
}
// 获取stats
func (space *spaceManager) Stats() *Stats {
	return space.stats
}
// 恢复disk数据结构，并restore disk中的partition
func (space *spaceManager) LoadDisk(path string, restSize uint64, maxErrs int) (err error) {
	var (
		disk *Disk
	)
	log.LogDebugf("action[LoadDisk] load disk from path[%v].", path)
	if _, err = space.GetDisk(path); err != nil {
		disk = NewDisk(path, restSize, maxErrs)
		disk.RestorePartition(space)
		space.putDisk(disk)
		err = nil
	}
	return
}
// 根据path获取Disk
func (space *spaceManager) GetDisk(path string) (d *Disk, err error) {
	space.diskMu.RLock()
	defer space.diskMu.RUnlock()
	disk, has := space.disks[path]
	if has && disk != nil {
		d = disk
		return
	}
	err = fmt.Errorf("disk[%v] not exsit", path)
	return
}
// 放在map里
func (space *spaceManager) putDisk(d *Disk) {
	space.diskMu.Lock()
	space.disks[d.Path] = d
	space.diskMu.Unlock()

}
// 更新metrics
func (space *spaceManager) updateMetrics() {
	space.diskMu.RLock()
	var (
		total, used, available                                   uint64
		createdPartitionWeights, remainWeightsForCreatePartition uint64
		maxWeightsForCreatePartition, partitionCnt               uint64
	)
	maxWeightsForCreatePartition = 0
	for _, d := range space.disks {
		total += d.Total
		used += d.Used
		available += d.Available
		createdPartitionWeights += d.Allocated
		remainWeightsForCreatePartition += d.Unallocated
		partitionCnt += uint64(d.PartitionCount())
		if maxWeightsForCreatePartition < d.Unallocated {
			maxWeightsForCreatePartition = d.Unallocated
		}
	}
	space.diskMu.RUnlock()
	log.LogDebugf("action[updateMetrics] total[%v] used[%v] available[%v] createdPartitionWeights[%v]  remainWeightsForCreatePartition[%v] "+
		"partitionCnt[%v] maxWeightsForCreatePartition[%v] ", total, used, available, createdPartitionWeights, remainWeightsForCreatePartition, partitionCnt, maxWeightsForCreatePartition)
	space.stats.updateMetrics(total, used, available, createdPartitionWeights,
		remainWeightsForCreatePartition, maxWeightsForCreatePartition, partitionCnt)
}
// 找到包含分区数最少的disk
func (space *spaceManager) getMinPartitionCntDisk() (d *Disk) {
	space.diskMu.Lock()
	defer space.diskMu.Unlock()
	var minPartitionCnt uint64
	minPartitionCnt = math.MaxUint64
	var path string
	for index, disk := range space.disks {
		if uint64(disk.PartitionCount()) < minPartitionCnt {
			minPartitionCnt = uint64(disk.PartitionCount())
			path = index
		}
	}
	if path == "" {
		return nil
	}
	d = space.disks[path]

	return
}
// 每5分钟对每个分区调用一次fileRepair，实际调用所有分区的partition.LaunchRepair()
func (space *spaceManager) fileRepairScheduler() {
	go func() {
		timer := time.NewTimer(5 * time.Minute)
		for {
			select {
			case <-timer.C:
				space.fileRepair()
				timer = time.NewTimer(2 * time.Minute)
			case <-space.stopC:
				timer.Stop()
				return
			}
		}
	}()
}
// 每2分钟调用一次flushDelete
func (space *spaceManager) flushDeleteScheduler() {
	go func() {
		ticker := time.NewTicker(2 * time.Minute)
		for {
			select {
			case <-ticker.C:
				space.flushDelete()
			case <-space.stopC:
				ticker.Stop()
				return
			}
		}
	}()
}
// 每5分钟对每个分区调用一次updateMetrics，调用每个分区的partition.FlushDelete()
func (space *spaceManager) statUpdateScheduler() {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				space.updateMetrics()
			case <-space.stopC:
				ticker.Stop()
				return
			}
		}
	}()
}
// 所有分区都调用LaunchRepair
func (space *spaceManager) fileRepair() {
	partitions := make([]DataPartition, 0)
	space.RangePartitions(func(dp DataPartition) bool {
		partitions = append(partitions, dp)
		return true
	})
	for _, partition := range partitions {
		partition.LaunchRepair()
	}
}
// 所有分区都调用FlushDelete
func (space *spaceManager) flushDelete() {
	partitions := make([]DataPartition, 0)
	space.RangePartitions(func(dp DataPartition) bool {
		partitions = append(partitions, dp)
		return true
	})
	for _, partition := range partitions {
		partition.FlushDelete()
	}
}
// 从map里面获取DataPartition
func (space *spaceManager) GetPartition(partitionId uint32) (dp DataPartition) {
	space.partitionMu.RLock()
	defer space.partitionMu.RUnlock()
	dp = space.partitions[partitionId]

	return
}
// DataPartition放到map里面s
func (space *spaceManager) PutPartition(dp DataPartition) {
	space.partitionMu.Lock()
	defer space.partitionMu.Unlock()
	space.partitions[dp.ID()] = dp
	return
}
// 找到包含的分区数目最小的disk,然后创建分区
func (space *spaceManager) CreatePartition(volId string, partitionId uint32, storeSize int, storeType string) (dp DataPartition, err error) {
	if space.GetPartition(partitionId) != nil {
		return
	}
	disk := space.getMinPartitionCntDisk()
	if disk == nil || disk.Available < uint64(storeSize) {
		return nil, ErrNoDiskForCreatePartition
	}
	if dp, err = CreateDataPartition(volId, partitionId, disk, storeSize, storeType); err != nil {
		return
	}
	space.PutPartition(dp)
	return
}
// 从space和disk中移除分区，然后分区自我销毁
func (space *spaceManager) DeletePartition(dpId uint32) {
	dp := space.GetPartition(dpId)
	if dp == nil {
		return
	}
	space.partitionMu.Lock()
	delete(space.partitions, dpId)
	space.partitionMu.Unlock()
	dp.Destroy()
	dp.Disk().DelDataPartition(dp)
}

func (s *DataNode) fillHeartBeatResponse(response *proto.DataNodeHeartBeatResponse) {
	response.Status = proto.TaskSuccess
	stat := s.space.Stats()
	stat.Lock()
	response.Used = stat.Used
	response.Total = stat.Total
	response.Available = stat.Available
	response.CreatedPartitionCnt = uint32(stat.CreatedPartitionCnt)
	response.CreatedPartitionWeights = stat.CreatedPartitionWeights
	response.MaxWeightsForCreatePartition = stat.MaxWeightsForCreatePartition
	response.RemainWeightsForCreatePartition = stat.RemainWeightsForCreatePartition
	stat.Unlock()

	response.RackName = s.rackName
	response.PartitionInfo = make([]*proto.PartitionReport, 0)
	space := s.space
	space.RangePartitions(func(partition DataPartition) bool {
		vr := &proto.PartitionReport{
			PartitionID:     uint64(partition.ID()),
			PartitionStatus: partition.Status(),
			Total:           uint64(partition.Size()),
			Used:            uint64(partition.Used()),
		}
		response.PartitionInfo = append(response.PartitionInfo, vr)
		return true
	})
}
