package datanode

import (
	"encoding/json"
	"net"
	"sync"
	"time"

	"fmt"
	"github.com/tiglabs/containerfs/proto"
	"github.com/tiglabs/containerfs/storage"
	"github.com/tiglabs/containerfs/third_party/juju/errors"
	"github.com/tiglabs/containerfs/util/log"
	"hash/crc32"
)

//every  datapartion  file metas used for auto repairt
type MembersFileMetas struct {
	TaskType               uint8                        //which type task
	files                  map[uint64]*storage.FileInfo //storage file on datapartiondisk meta
	NeedAddExtentsTasks    []*storage.FileInfo          //generator add extent file task
	NeedFixExtentSizeTasks []*storage.FileInfo          //generator fixSize file task
}

func NewMemberFileMetas() (mf *MembersFileMetas) {
	mf = &MembersFileMetas{
		files:                  make(map[uint64]*storage.FileInfo),
		NeedAddExtentsTasks:    make([]*storage.FileInfo, 0),
		NeedFixExtentSizeTasks: make([]*storage.FileInfo, 0),
	}
	return
}

//files repair check
func (dp *DataPartition) extentFileRepair(fixExtentsType uint8) {
	startTime := time.Now().UnixNano()
	log.LogInfof("action[extentFileRepair] partition(%v) start.",
		dp.partitionId)
	unavaliTinyExtents := make([]uint64, 0)
	if fixExtentsType == proto.TinyExtentMode {
		unavaliTinyExtents = dp.getUnavaliTinyExtents()
		if len(unavaliTinyExtents) == 0 {
			return
		}
	}
	// Get all data partition group member about file metas
	allMembers, err := dp.getAllMemberExtentMetas(fixExtentsType, unavaliTinyExtents)
	if err != nil {
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionId, err)
		log.LogErrorf(errors.ErrorStack(err))
		dp.putTinyExtentToUnavliCh(fixExtentsType, unavaliTinyExtents)
		return
	}
	noNeedFix, needFix := dp.generatorExtentRepairTasks(allMembers) //generator file repair task
	err = dp.NotifyExtentRepair(allMembers)                         //notify host to fix it
	if err != nil {
		dp.putAllTinyExtentsToStore(fixExtentsType, noNeedFix, needFix)
		log.LogErrorf("action[extentFileRepair] partition(%v) err(%v).",
			dp.partitionId, err)
		log.LogError(errors.ErrorStack(err))
		return
	}
	for _, addExtent := range allMembers[0].NeedAddExtentsTasks {
		dp.extentStore.Create(addExtent.FileId, addExtent.Inode)
	}
	for _, fixExtentFile := range allMembers[0].NeedFixExtentSizeTasks {
		dp.streamRepairExtent(fixExtentFile) //fix leader filesize
	}
	finishTime := time.Now().UnixNano()
	dp.putAllTinyExtentsToStore(fixExtentsType, noNeedFix, needFix)
	if dp.extentStore.GetAvaliExtentLen()+dp.extentStore.GetUnAvaliExtentLen() > storage.TinyExtentCount {
		log.LogWarnf("action[extentFileRepair] partition(%v) AvaliTinyExtents(%v) UnavaliTinyExtents(%v) finish cost[%vms].",
			dp.partitionId, dp.extentStore.GetAvaliExtentLen(), dp.extentStore.GetUnAvaliExtentLen(), (finishTime-startTime)/int64(time.Millisecond))
	}
	log.LogInfof("action[extentFileRepair] partition(%v) AvaliTinyExtents(%v) UnavaliTinyExtents(%v) finish cost[%vms].",
		dp.partitionId, dp.extentStore.GetAvaliExtentLen(), dp.extentStore.GetUnAvaliExtentLen(), (finishTime-startTime)/int64(time.Millisecond))

}

func (dp *DataPartition) putTinyExtentToUnavliCh(fixExtentType uint8, extents []uint64) {
	if fixExtentType == proto.TinyExtentMode {
		dp.extentStore.PutTinyExtentsToUnAvaliCh(extents)
	}
	return
}

func (dp *DataPartition) putAllTinyExtentsToStore(fixExtentType uint8, noNeedFix, needFix []uint64) {
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

func (dp *DataPartition) getUnavaliTinyExtents() (unavaliTinyExtents []uint64) {
	unavaliTinyExtents = make([]uint64, 0)
	fixTinyExtents := MinFixTinyExtents
	if dp.isFirstFixTinyExtents {
		fixTinyExtents = storage.TinyExtentCount
		dp.isFirstFixTinyExtents = false
	}
	for i := 0; i < fixTinyExtents; i++ {
		extentId, err := dp.extentStore.GetUnavaliTinyExtent()
		if err != nil {
			return
		}
		unavaliTinyExtents = append(unavaliTinyExtents, extentId)
	}
	return
}

// Get all data partition group ,about all files meta
func (dp *DataPartition) getAllMemberExtentMetas(fixExtentMode uint8, needFixExtents []uint64) (allMemberFileMetas []*MembersFileMetas, err error) {
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
		err = errors.Annotatef(err, "getAllMemberExtentMetas extent DataPartition(%v) GetAllWaterMark", dp.partitionId)
		return
	}
	// write blob files meta to extent files meta
	files = append(files, extentFiles...)
	leaderFileMetas := NewMemberFileMetas()
	if err != nil {
		err = errors.Annotatef(err, "getAllMemberExtentMetas DataPartition(%v) GetAllWaterMark", dp.partitionId)
		return
	}
	for _, fi := range files {
		fi.Source = dp.replicaHosts[0]
		leaderFileMetas.files[fi.FileId] = fi
	}
	allMemberFileMetas[0] = leaderFileMetas
	// leader files meta has ready

	// get remote files meta by opGetAllWaterMarker cmd
	p := NewExtentStoreGetAllWaterMarker(dp.partitionId, fixExtentMode)
	if fixExtentMode == proto.TinyExtentMode {
		p.Data, err = json.Marshal(needFixExtents)
		if err != nil {
			err = errors.Annotatef(err, "getAllMemberExtentMetas DataPartition(%v) GetAllWaterMark", dp.partitionId)
			return
		}
		p.Size = uint32(len(p.Data))
	}
	for i := 1; i < len(dp.replicaHosts); i++ {
		var conn *net.TCPConn
		target := dp.replicaHosts[i]
		conn, err = gConnPool.Get(target) //get remote connect
		if err != nil {
			err = errors.Annotatef(err, "getAllMemberExtentMetas  DataPartition(%v) get host(%v) connect", dp.partitionId, target)
			return
		}
		err = p.WriteToConn(conn) //write command to remote host
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas DataPartition(%v) write to host(%v)", dp.partitionId, target)
			return
		}
		reply := new(Packet)
		err = reply.ReadFromConn(conn, 60) //read it response
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas DataPartition(%v) read from host(%v)", dp.partitionId, target)
			return
		}
		fileInfos := make([]*storage.FileInfo, 0)
		err = json.Unmarshal(reply.Data[:reply.Size], &fileInfos)
		if err != nil {
			gConnPool.Put(conn, true)
			err = errors.Annotatef(err, "getAllMemberExtentMetas DataPartition(%v) unmarshal json(%v)", dp.partitionId, string(p.Data[:p.Size]))
			return
		}
		gConnPool.Put(conn, true)
		slaverFileMetas := NewMemberFileMetas()
		for _, fileInfo := range fileInfos {
			fileInfo.Source = target
			slaverFileMetas.files[fileInfo.FileId] = fileInfo
		}
		allMemberFileMetas[i] = slaverFileMetas
	}
	return
}

//generator file task
func (dp *DataPartition) generatorExtentRepairTasks(allMembers []*MembersFileMetas) (noNeedFix []uint64, needFix []uint64) {
	maxSizeExtentMap := dp.mapMaxSizeExtentToIndex(allMembers) //map maxSize extentId to allMembers index
	dp.generatorAddExtentsTasks(allMembers, maxSizeExtentMap)  //add extentTask
	noNeedFix, needFix = dp.generatorFixExtentSizeTasks(allMembers, maxSizeExtentMap)
	return
}

/* pasre all extent,select maxExtentSize to member index map
 */
func (dp *DataPartition) mapMaxSizeExtentToIndex(allMembers []*MembersFileMetas) (maxSizeExtentMap map[uint64]*storage.FileInfo) {
	maxSizeExtentMap = make(map[uint64]*storage.FileInfo)
	for index := 0; index < len(allMembers); index++ {
		member := allMembers[index]
		for fileId, fileInfo := range member.files {
			maxFileInfo, ok := maxSizeExtentMap[fileId]
			if !ok {
				maxSizeExtentMap[fileId] = fileInfo
			} else {
				orgInode := maxSizeExtentMap[fileId].Inode
				if fileInfo.Size > maxFileInfo.Size {
					if fileInfo.Inode == 0 && orgInode != 0 {
						fileInfo.Inode = orgInode
					}
					maxSizeExtentMap[fileId] = fileInfo
				}
			}
		}
	}
	return
}

/*generator add extent if follower not have this extent*/
func (dp *DataPartition) generatorAddExtentsTasks(allMembers []*MembersFileMetas, maxSizeExtentMap map[uint64]*storage.FileInfo) {
	for fileId, maxFileInfo := range maxSizeExtentMap {
		if storage.IsTinyExtent(fileId) {
			continue
		}
		for index := 0; index < len(allMembers); index++ {
			follower := allMembers[index]
			if _, ok := follower.files[fileId]; !ok && maxFileInfo.Deleted == false {
				if maxFileInfo.Inode == 0 {
					continue
				}
				addFile := &storage.FileInfo{Source: maxFileInfo.Source, FileId: fileId, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				follower.NeedAddExtentsTasks = append(follower.NeedAddExtentsTasks, addFile)
				follower.NeedFixExtentSizeTasks = append(follower.NeedFixExtentSizeTasks, addFile)
				log.LogInfof("action[generatorAddExtentsTasks] partition(%v) addFile(%v) on Index(%v).", dp.partitionId, addFile, index)
			}
		}
	}
}

/*generator fix extent Size ,if all members  Not the same length*/
func (dp *DataPartition) generatorFixExtentSizeTasks(allMembers []*MembersFileMetas, maxSizeExtentMap map[uint64]*storage.FileInfo) (noNeedFix []uint64, needFix []uint64) {
	noNeedFix = make([]uint64, 0)
	needFix = make([]uint64, 0)
	for fileId, maxFileInfo := range maxSizeExtentMap {
		isFix := true
		for index := 0; index < len(allMembers); index++ {
			extentInfo, ok := allMembers[index].files[fileId]
			if !ok {
				continue
			}
			if extentInfo.Size < maxFileInfo.Size {
				fixExtent := &storage.FileInfo{Source: maxFileInfo.Source, FileId: fileId, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				allMembers[index].NeedFixExtentSizeTasks = append(allMembers[index].NeedFixExtentSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] partition(%v) fixExtent(%v).", dp.partitionId, fixExtent)
				isFix = false
			}
			if maxFileInfo.Inode != 0 && extentInfo.Inode == 0 {
				fixExtent := &storage.FileInfo{Source: maxFileInfo.Source, FileId: fileId, Size: maxFileInfo.Size, Inode: maxFileInfo.Inode}
				allMembers[index].NeedFixExtentSizeTasks = append(allMembers[index].NeedFixExtentSizeTasks, fixExtent)
				log.LogInfof("action[generatorFixExtentSizeTasks] partition(%v) Modify Ino fixExtent(%v).", dp.partitionId, fixExtent)
			}
		}
		if storage.IsTinyExtent(fileId) {
			if isFix {
				noNeedFix = append(noNeedFix, fileId)
			} else {
				needFix = append(needFix, fileId)
			}
		}
	}
	return
}

func (dp *DataPartition) notifyFollower(wg *sync.WaitGroup, index int, members []*MembersFileMetas) (err error) {
	p := NewNotifyExtentRepair(dp.partitionId) //notify all follower to repairt task,send opnotifyRepair command
	var conn *net.TCPConn
	target := dp.replicaHosts[index]
	p.Data, _ = json.Marshal(members[index])
	conn, err = gConnPool.Get(target)
	defer func() {
		wg.Done()
		log.LogErrorf(ActionNotifyFollowerRepair, fmt.Sprintf(" to %v task %v failed %v", target, string(p.Data), err.Error()))
	}()
	if err != nil {
		return err
	}
	p.Size = uint32(len(p.Data))
	if err = p.WriteToConn(conn); err != nil {
		gConnPool.Put(conn, true)
		return err
	}
	if err = p.ReadFromConn(conn, proto.NoReadDeadlineTime); err != nil {
		gConnPool.Put(conn, true)
		return err
	}
	gConnPool.Put(conn, true)
	return nil
}

/*notify follower to repair DataPartition extentStore*/
func (dp *DataPartition) NotifyExtentRepair(members []*MembersFileMetas) (err error) {
	wg := new(sync.WaitGroup)
	for i := 1; i < len(members); i++ {
		wg.Add(1)
		go dp.notifyFollower(wg, i, members)
	}
	wg.Wait()

	return
}

// DoStreamExtentFixRepair executed on follower node of data partition.
// It receive from leader notifyRepair command extent file repair.
func (dp *DataPartition) doStreamExtentFixRepair(wg *sync.WaitGroup, remoteExtentInfo *storage.FileInfo) {
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

func (dp *DataPartition) applyRepairKey(fileId int) (m string) {
	return fmt.Sprintf("ApplyRepairKey(%v_%v)", dp.ID(), fileId)
}

//extent file repair function,do it on follower host
func (dp *DataPartition) streamRepairExtent(remoteExtentInfo *storage.FileInfo) (err error) {
	store := dp.GetExtentStore()
	if !store.IsExistExtent(remoteExtentInfo.FileId) {
		return
	}

	defer func() {
		store.GetWatermark(remoteExtentInfo.FileId, true)
	}()

	// Get local extent file info
	localExtentInfo, err := store.GetWatermark(remoteExtentInfo.FileId, true)
	if err != nil {
		return errors.Annotatef(err, "streamRepairExtent GetWatermark error")
	}
	if localExtentInfo.Inode == 0 && remoteExtentInfo.Inode != 0 {
		store.ModifyInode(remoteExtentInfo.Inode, remoteExtentInfo.FileId)
		log.LogInfof("%v Modify Inode to %v", dp.applyRepairKey(int(remoteExtentInfo.FileId)), remoteExtentInfo.Inode)
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
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}
		reply := NewPacket()
		// Read 64k stream repair packet
		if err = reply.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			err = errors.Annotatef(err, "streamRepairExtent receive data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Annotatef(err, "streamRepairExtent receive opcode error(%v) ", string(reply.Data[:reply.Size]))
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.FileID != request.FileID || reply.Size == 0 || reply.Offset != int64(currFixOffset) {
			err = errors.Annotatef(err, "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}

		log.LogInfof("action[streamRepairExtent] partition(%v) extent(%v) start fix from (%v)"+
			" remoteSize(%v) localSize(%v) reply(%v).", dp.ID(), remoteExtentInfo.FileId,
			remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, reply.GetUniqueLogId())

		if reply.Crc != crc32.ChecksumIEEE(reply.Data[:reply.Size]) {
			err = fmt.Errorf("streamRepairExtent crc mismatch partition(%v) extent(%v) start fix from (%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v)", dp.ID(), remoteExtentInfo.FileId,
				remoteExtentInfo.Source, remoteExtentInfo.Size, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return errors.Annotatef(err, "streamRepairExtent receive data error")
		}
		// Write it to local extent file
		if storage.IsTinyExtent(uint64(localExtentInfo.FileId)) {
			err = store.WriteTinyRecover(uint64(localExtentInfo.FileId), int64(currFixOffset), int64(reply.Size), reply.Data, reply.Crc)
		} else {
			err = store.Write(uint64(localExtentInfo.FileId), int64(currFixOffset), int64(reply.Size), reply.Data, reply.Crc)
		}
		if err != nil {
			err = errors.Annotatef(err, "streamRepairExtent repair data error")
			log.LogErrorf("action[streamRepairExtent] err(%v).", err)
			return
		}
		currFixOffset += uint64(reply.Size)
		if currFixOffset >= remoteExtentInfo.Size {
			break
		}

	}
	return

}
