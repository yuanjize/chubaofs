package datanode

import (
	"encoding/json"
	"net"
	"strings"
	"sync"
	"time"

	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/util/log"
	"hash/crc32"
)

//every  datapartion  file metas used for auto repairt
type MembersFileMetas struct {
	TaskType               uint8                        //which type task
	files                  map[uint64]*storage.FileInfo //storage file on datapartiondisk meta
	NeedDeleteExtentsTasks []*storage.FileInfo          //generator delete extent file task
	NeedAddExtentsTasks    []*storage.FileInfo          //generator add extent file task
	NeedFixExtentSizeTasks []*storage.FileInfo          //generator fixSize file task
}

func NewMemberFileMetas() (mf *MembersFileMetas) {
	mf = &MembersFileMetas{
		files:                  make(map[uint64]*storage.FileInfo),
		NeedDeleteExtentsTasks: make([]*storage.FileInfo, 0),
		NeedAddExtentsTasks:    make([]*storage.FileInfo, 0),
		NeedFixExtentSizeTasks: make([]*storage.FileInfo, 0),
	}
	return
}

//files repair check
func (dp *dataPartition) extentFileRepair(fixExtentsType uint8) {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[extentFileRepair] partition(%v) start.",
		dp.partitionId)
	tinyExtents := make([]uint64, 0)
	if fixExtentsType == proto.TinyExtentMode {
		tinyExtents = dp.getUnavaliTinyExtents()
		if len(tinyExtents) == 0 {
			return
		}
	}
	// Get all data partition group member about file metas
	allMembers, err := dp.getAllMemberExtentMetas(fixExtentsType, tinyExtents)
	if err != nil {
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionId, err)
		log.LogErrorf(errors.ErrorStack(err))
		dp.putTinyExtentToUnavliCh(fixExtentsType, tinyExtents)
		return
	}
	noNeedFix, needFix := dp.generatorExtentRepairTasks(allMembers) //generator file repair task
	err = dp.NotifyExtentRepair(allMembers)                         //notify host to fix it
	if err != nil {
		dp.putTinyExtentToUnavliCh(fixExtentsType, tinyExtents)
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionId, err)
		log.LogError(errors.ErrorStack(err))
		return
	}
	for _, fixExtentFile := range allMembers[0].NeedFixExtentSizeTasks {
		dp.streamRepairExtent(fixExtentFile) //fix leader filesize
	}
	finishTime := time.Now().UnixNano()
	dp.putAllTinyExtentsToStore(fixExtentsType, noNeedFix, needFix)
	log.LogInfof("action[extentFileRepair] partition(%v) AvaliTinyExtents(%v) UnavaliTinyExtents(%v) finish cost[%vms].",
		dp.partitionId, dp.extentStore.GetAvaliExtentLen(), dp.extentStore.GetUnAvaliExtentLen(), (finishTime-startTime)/int64(time.Millisecond))

}

func (dp *dataPartition) putTinyExtentToUnavliCh(fixExtentType uint8, extents []uint64) {
	if fixExtentType == proto.TinyExtentMode {
		dp.extentStore.PutTinyExtentsToUnAvaliCh(extents)
	}
	return
}

func (dp *dataPartition) putAllTinyExtentsToStore(fixExtentType uint8, noNeedFix, needFix []uint64) {
	if fixExtentType != proto.TinyExtentMode {
		return
	}
	for _, extentId := range noNeedFix {
		if storage.IsTinyExtent(extentId) {
			dp.extentStore.PutTinyExtentToAvaliCh(extentId)
		}
	}
	for _, extentId := range needFix {
		if storage.IsTinyExtent(extentId) {
			dp.extentStore.PutTinyExtentToUnavaliCh(extentId)
		}
	}
}

func (dp *dataPartition) getUnavaliTinyExtents() (tinyExtents []uint64) {
	tinyExtents = make([]uint64, 0)
	fixTinyExtents := 3
	if dp.isFirstFixTinyExtents {
		fixTinyExtents = storage.TinyExtentCount
		dp.isFirstFixTinyExtents = false
	}
	for i := 0; i < fixTinyExtents; i++ {
		extentId, err := dp.extentStore.GetUnavaliTinyExtent()
		if err != nil {
			return
		}
		tinyExtents = append(tinyExtents, extentId)
	}
	return
}

// Get all data partition group ,about all files meta
func (dp *dataPartition) getAllMemberExtentMetas(fixExtentMode uint8, needFixExtents []uint64) (allMemberFileMetas []*MembersFileMetas, err error) {
	allMemberFileMetas = make([]*MembersFileMetas, len(dp.replicaHosts))
	var (
		extentFiles []*storage.FileInfo
	)
	files := make([]*storage.FileInfo, 0)
	// get local extent file metas
	if fixExtentMode == proto.NormalExtentMode {
		extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableExtentFilter())
	} else {
		extentFiles, err = dp.extentStore.GetAllWatermark(storage.GetStableTinyExtentFilter(needFixExtents))
	}
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberExtentMetas extent dataPartition(%v) GetAllWaterMark", dp.partitionId)
		return
	}
	// write blob files meta to extent files meta
	files = append(files, extentFiles...)
	leaderFileMetas := NewMemberFileMetas()
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) GetAllWaterMark", dp.partitionId)
		return
	}
	for _, fi := range files {
		leaderFileMetas.files[fi.FileId] = fi
	}
	allMemberFileMetas[0] = leaderFileMetas
	// leader files meta has ready

	// get remote files meta by opGetAllWaterMarker cmd
	p := NewExtentStoreGetAllWaterMarker(dp.partitionId, fixExtentMode)
	if fixExtentMode == proto.TinyExtentMode {
		p.Data, err = json.Marshal(needFixExtents)
		if err != nil {
			err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) GetAllWaterMark", dp.partitionId)
			return
		}
		p.Size = uint32(len(p.Data))
	}
	for i := 1; i < len(dp.replicaHosts); i++ {
		var conn *net.TCPConn
		target := dp.replicaHosts[i]
		conn, err = gConnPool.Get(target) //get remote connect
		if err != nil {
			err = errors.Annotatef(err, "getAllMemberExtentMetas  dataPartition(%v) get host(%v) connect", dp.partitionId, target)
			return
		}
		err = p.WriteToConn(conn) //write command to remote host
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) write to host(%v)", dp.partitionId, target)
			return
		}
		reply := new(Packet)
		err = reply.ReadFromConn(conn, 60) //read it response
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) read from host(%v)", dp.partitionId, target)
			return
		}
		fileInfos := make([]*storage.FileInfo, 0)
		err = json.Unmarshal(reply.Data[:reply.Size], &fileInfos)
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas dataPartition(%v) unmarshal json(%v)", dp.partitionId, string(p.Data[:p.Size]))
			return
		}
		gConnPool.Put(conn, true)
		slaverFileMetas := NewMemberFileMetas()
		for _, fileInfo := range fileInfos {
			slaverFileMetas.files[fileInfo.FileId] = fileInfo
		}
		allMemberFileMetas[i] = slaverFileMetas
	}
	return
}

//generator file task
func (dp *dataPartition) generatorExtentRepairTasks(allMembers []*MembersFileMetas) (noNeedFix []uint64, needFix []uint64) {
	dp.generatorAddExtentsTasks(allMembers) //add extentTask
	noNeedFix, needFix = dp.generatorFixExtentSizeTasks(allMembers)
	dp.generatorDeleteExtentsTasks(allMembers)
	return
}

/* pasre all extent,select maxExtentSize to member index map
 */
func (dp *dataPartition) mapMaxSizeExtentToIndex(allMembers []*MembersFileMetas) (maxSizeExtentMap map[uint64]int) {
	leader := allMembers[0]
	maxSizeExtentMap = make(map[uint64]int)
	for fileId := range leader.files { //range leader all extentFiles
		maxSizeExtentMap[fileId] = 0
		var maxFileSize uint64
		for index := 0; index < len(allMembers); index++ {
			member := allMembers[index]
			_, ok := member.files[fileId]
			if !ok {
				continue
			}
			if maxFileSize < member.files[fileId].Size {
				maxFileSize = member.files[fileId].Size
				maxSizeExtentMap[fileId] = index //map maxSize extentId to allMembers index
			}
		}
	}
	return
}

/*generator add extent if follower not have this extent*/
func (dp *dataPartition) generatorAddExtentsTasks(allMembers []*MembersFileMetas) {
	leader := allMembers[0]
	leaderAddr := dp.replicaHosts[0]
	for fileId, leaderFile := range leader.files {
		if storage.IsTinyExtent(fileId) {
			continue
		}
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.files[fileId]; !ok {
				addFile := &storage.FileInfo{Source: leaderAddr, FileId: fileId, Size: leaderFile.Size, Inode: leaderFile.Inode}
				follower.NeedAddExtentsTasks = append(follower.NeedAddExtentsTasks, addFile)
				log.LogInfof("action[generatorAddExtentsTasks] partition(%v) addFile(%v).", dp.partitionId, addFile)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorFixExtentSizeTasks(allMembers []*MembersFileMetas) (noNeedFix []uint64, needFix []uint64) {
	leader := allMembers[0]
	maxSizeExtentMap := dp.mapMaxSizeExtentToIndex(allMembers) //map maxSize extentId to allMembers index
	noNeedFix = make([]uint64, 0)
	needFix = make([]uint64, 0)
	for fileId, leaderFile := range leader.files {
		maxSizeExtentIdIndex := maxSizeExtentMap[fileId]
		maxSize := allMembers[maxSizeExtentIdIndex].files[fileId].Size
		sourceAddr := dp.replicaHosts[maxSizeExtentIdIndex]
		inode := leaderFile.Inode
		isFix := true
		for index := 0; index < len(allMembers); index++ {
			if index == maxSizeExtentIdIndex {
				continue
			}
			extentInfo, ok := allMembers[index].files[fileId]
			if !ok {
				continue
			}
			if extentInfo.Size < maxSize {
				fixExtent := &storage.FileInfo{Source: sourceAddr, FileId: fileId, Size: maxSize, Inode: inode}
				allMembers[index].NeedFixExtentSizeTasks = append(allMembers[index].NeedFixExtentSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] partition(%v) fixExtent(%v).", dp.partitionId, fixExtent)
				isFix = false
			}
		}
		if isFix {
			noNeedFix = append(noNeedFix, fileId)
		} else {
			needFix = append(needFix, fileId)
		}
	}
	return
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *dataPartition) generatorDeleteExtentsTasks(allMembers []*MembersFileMetas) {
	store := dp.extentStore
	deletes := store.GetDelObjects()
	leaderAddr := dp.replicaHosts[0]
	for _, deleteFileId := range deletes {
		for index := 1; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.files[deleteFileId]; ok {
				deleteFile := &storage.FileInfo{Source: leaderAddr, FileId: deleteFileId, Size: 0}
				follower.NeedDeleteExtentsTasks = append(follower.NeedDeleteExtentsTasks, deleteFile)
				log.LogInfof("action[generatorDeleteExtentsTasks] partition(%v) deleteFile(%v).", dp.partitionId, deleteFile)
			}
		}
	}
}

/*notify follower to repair dataPartition extentStore*/
func (dp *dataPartition) NotifyExtentRepair(members []*MembersFileMetas) (err error) {
	var wg sync.WaitGroup
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := NewNotifyExtentRepair(dp.partitionId) //notify all follower to repairt task,send opnotifyRepair command
			var conn *net.TCPConn
			target := dp.replicaHosts[index]
			conn, err = gConnPool.Get(target)
			if err != nil {
				return
			}
			p.Data, err = json.Marshal(members[index])
			p.Size = uint32(len(p.Data))
			if err = p.WriteToConn(conn); err != nil {
				gConnPool.Put(conn, true)
				return
			}
			if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
				gConnPool.Put(conn, true)
				return
			}
			gConnPool.Put(conn, true)
		}(i)
	}
	wg.Wait()

	return
}

/*notify follower to repair dataPartition extentStore*/
func (dp *dataPartition) NotifyRaftFollowerRepair(members *MembersFileMetas) (err error) {
	var wg sync.WaitGroup

	for i := 0; i < len(dp.replicaHosts); i++ {
		replicaAddr := dp.replicaHosts[i]
		replicaAddrParts := strings.Split(replicaAddr, ":")
		if strings.TrimSpace(replicaAddrParts[0]) == LocalIP {
			continue //local is leader not need send notify repair
		}
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := NewNotifyExtentRepair(dp.partitionId)
			var conn *net.TCPConn
			target := dp.replicaHosts[index]
			conn, err = gConnPool.Get(target)
			if err != nil {
				return
			}
			p.Data, err = json.Marshal(members)
			p.Size = uint32(len(p.Data))
			err = p.WriteToConn(conn)
			if err != nil {
				gConnPool.Put(conn, true)
				return
			}

			if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
				gConnPool.Put(conn, true)
				return
			}
			gConnPool.Put(conn, true)
		}(i)
	}
	wg.Wait()

	return
}

// DoStreamExtentFixRepair executed on follower node of data partition.
// It receive from leader notifyRepair command extent file repair.
func (dp *dataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.FileInfo) {
	defer wg.Done()
	err := dp.streamRepairExtent(remoteExtentInfo)

	if err != nil {
		err = errors.Annotatef(err, "doStreamExtentFixRepair %v", dp.applyRepairKey(int(remoteExtentInfo.FileId)))
		localExtentInfo, opErr := dp.GetExtentStore().GetWatermark(uint64(remoteExtentInfo.FileId), false)
		if opErr != nil {
			err = errors.Annotatef(err, opErr.Error())
		}
		err = errors.Annotatef(err, "partition(%v) remote(%v) local(%v)",
			dp.partitionId, remoteExtentInfo, localExtentInfo)
		log.LogErrorf("action[doStreamExtentFixRepair] err(%v).", err)
	}
}

func (dp *dataPartition) applyRepairKey(fileId int) (m string) {
	return fmt.Sprintf("ApplyRepairKey(%v_%v)", dp.ID(), fileId)
}

//extent file repair function,do it on follower host
func (dp *dataPartition) streamRepairExtent(remoteExtentInfo *storage.FileInfo) (err error) {
	store := dp.GetExtentStore()
	if !store.IsExistExtent(remoteExtentInfo.FileId) {
		return
	}

	// Get local extent file info
	localExtentInfo, err := store.GetWatermark(remoteExtentInfo.FileId, false)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}

	// Get need fix size for this extent file
	needFixSize := remoteExtentInfo.Size - localExtentInfo.Size

	// Create streamRead packet, it offset is local extentInfoSize, size is needFixSize
	request := NewStreamReadPacket(dp.ID(), remoteExtentInfo.FileId, int(localExtentInfo.Size), int(needFixSize))
	var conn *net.TCPConn

	// Get a connection to leader host
	conn, err = gConnPool.Get(remoteExtentInfo.Source)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent get conn from host[%v] error", remoteExtentInfo.Source)
	}
	defer gConnPool.Put(conn, true)

	// Write OpStreamRead command to leader
	if err = request.WriteToConn(conn); err != nil {
		err = errors.Annotatef(err, "streamRepairExtent send streamRead to host[%v] error", remoteExtentInfo.Source)
		log.LogErrorf("action[streamRepairExtent] err[%v].", err)
		return
	}
	currFixOffset := localExtentInfo.Size
	for currFixOffset < remoteExtentInfo.Size {
		// If local extent size has great remoteExtent file size ,then break
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}
		localExtentInfo, err = store.GetWatermark(remoteExtentInfo.FileId, false)
		if err != nil {
			err = errors.Annotatef(err, "streamRepairExtent GetWatermark error")
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return err
		}
		if localExtentInfo.Size > currFixOffset {
			err = errors.Annotatef(err, "streamRepairExtent unavali fix localSize(%v) "+
				"remoteSize(%v) want fixOffset(%v) data error", localExtentInfo.Size, remoteExtentInfo.Size, currFixOffset)
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return err
		}

		reply := NewPacket()
		// Read 64k stream repair packet
		if err = reply.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent receive data error")
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Annotatef(err, "streamRepairExtent receive opcode error(%v) ", string(reply.Data[:reply.Size]))
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID || reply.FileID != request.FileID {
			err = errors.Annotatef(err, "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogError("action[streamRepairExtent] err(%v).", err)
			return
		}

		log.LogInfof("action[streamRepairExtent] partition(%v) extent(%v) start fix from (%v)"+
			" remoteSize(%v) localSize(%v).", dp.ID(), remoteExtentInfo.FileId,
			remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset)

		if reply.Crc != crc32.ChecksumIEEE(reply.Data[:reply.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch partition(%v) extent(%v) start fix from (%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v)", dp.ID(), remoteExtentInfo.FileId,
				remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return errors.Annotatef(err, "streamRepairExtent receive data error")
		}
		// Write it to local extent file
		if err = store.Write(uint64(localExtentInfo.FileId), int64(currFixOffset), int64(reply.Size), reply.Data, reply.Crc); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent repair data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}
		currFixOffset += uint64(reply.Size)

	}
	return

}
