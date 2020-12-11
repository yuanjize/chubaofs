package storage

import (
	"os"
	"time"

	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/util"
	"io/ioutil"
	"strconv"
)

const (
	TinyChunkCount    = 1
	ChunkOpenOpt      = os.O_CREATE | os.O_RDWR | os.O_APPEND
	CompactThreshold  = 40
	CompactMaxWait    = time.Second * 10
	ReBootStoreMode   = false
	NewStoreMode      = true
	MinWriteAbleChunk = 1
	ObjectIdLen       = 8
)

// TinyStore is a store implement for tiny file storage which container 40 chunk files.
// This store will choose a available chunk file and append data to it.
type TinyStore struct {
	dataDir        string
	chunks         map[int]*Chunk
	availChunkCh   chan int  // 个人觉得这个就是个chunk锁，写的时候从这里取出来ID然后别的人就没法写了。写完了之后再把ID放回去
	unavailChunkCh chan int
	storeSize      int
	chunkSize      int
	fullChunks     *util.Set
}
// 加载了一个chunk
func NewTinyStore(dataDir string, storeSize int) (s *TinyStore, err error) {
	s = new(TinyStore)
	s.dataDir = dataDir
	if err = CheckAndCreateSubdir(dataDir); err != nil {
		return nil, fmt.Errorf("NewTinyStore [%v] err[%v]", dataDir, err)
	}
	s.chunks = make(map[int]*Chunk)
	if err = s.initChunkFile(); err != nil {
		return nil, fmt.Errorf("NewTinyStore [%v] err[%v]", dataDir, err)
	}

	s.availChunkCh = make(chan int, TinyChunkCount+1)
	s.unavailChunkCh = make(chan int, TinyChunkCount+1)
	for i := 1; i <= TinyChunkCount; i++ {
		s.unavailChunkCh <- i
	}
	s.storeSize = storeSize
	s.chunkSize = storeSize / TinyChunkCount
	s.fullChunks = util.NewSet()

	return
}
// 清空当前store(不会删除底层chunk)
func (s *TinyStore) DeleteStore() {
	for index, c := range s.chunks {
		c.file.Close()
		c.tree.idxFile.Close()
		delete(s.chunks, index)
	}
}

func (s *TinyStore) UseSize() (size int64) {
	// TODO: implement this
	return 0
}
// 初始化chunk，目前只有一个chunk
func (s *TinyStore) initChunkFile() (err error) {
	for i := 1; i <= TinyChunkCount; i++ {
		var c *Chunk
		if c, err = NewChunk(s.dataDir, i); err != nil {
			return fmt.Errorf("initChunkFile Error %s", err.Error())
		}
		s.chunks[i] = c
	}

	return
}
// 检查chunk是否存在
func (s *TinyStore) chunkExist(chunkId uint32) (exist bool) {
	name := s.dataDir + "/" + strconv.Itoa(int(chunkId))
	if _, err := os.Stat(name); err == nil {
		exist = true
	}

	return
}
// 把objectId标记为删除
func (s *TinyStore) WriteDeleteDentry(objectId uint64, chunkId int, crc uint32) (err error) {
	var (
		fi os.FileInfo
	)
	c, ok := s.chunks[chunkId]
	if !ok {
		return ErrorFileNotFound
	}
	if !c.compactLock.TryLock() {
		return ErrorAgain
	}
	defer c.compactLock.Unlock()
	if fi, err = c.file.Stat(); err != nil {
		return
	}
	o := &Object{Oid: objectId, Size: TombstoneFileSize, Offset: uint32(fi.Size()), Crc: crc}
	if err = c.tree.appendToIdxFile(o); err == nil {
		if c.loadLastOid() < objectId {
			c.storeLastOid(objectId)
		}
	}

	return
}
// 写操作就是数据写到数据文件末尾，然后创建新的object.如果该objectid之前已经存在，那么干掉老的然后插入新的
func (s *TinyStore) Write(fileId uint32, objectId uint64, size int64, data []byte, crc uint32) (err error) {
	var (
		fi os.FileInfo
	)
	chunkId := int(fileId)
	c, ok := s.chunks[chunkId]
	if !ok {
		return ErrorFileNotFound
	}

	if !c.compactLock.TryLock() {
		return ErrorAgain
	}
	defer c.compactLock.Unlock()

	if objectId < c.loadLastOid() { // object应该不能代表一个文件
		msg := fmt.Sprintf("Object id smaller than last oid. DataDir[%v] FileId[%v]"+
			" ObjectId[%v] Size[%v]", s.dataDir, chunkId, objectId, c.loadLastOid())
		err = errors.New(msg)
		return ErrObjectSmaller
	}

	if fi, err = c.file.Stat(); err != nil {
		return
	}

	newOffset := fi.Size()
	if _, err = c.file.WriteAt(data[:size], newOffset); err != nil {
		return
	}

	if _, _, err = c.tree.set(objectId, uint32(newOffset), uint32(size), crc); err == nil {
		if c.loadLastOid() < objectId {
			c.storeLastOid(objectId)
		}
	}
	return
}
// 读一个object，size必须和文件中的object.size一样
func (s *TinyStore) Read(fileId uint32, offset, size int64, nbuf []byte) (crc uint32, err error) {
	chunkId := int(fileId)
	objectId := uint64(offset)
	c, ok := s.chunks[chunkId]
	if !ok {
		return 0, ErrorFileNotFound
	}

	lastOid := c.loadLastOid()
	if lastOid < objectId {
		return 0, ErrorFileNotFound
	}

	c.commitLock.RLock()
	defer c.commitLock.RUnlock()

	var fi os.FileInfo
	if fi, err = c.file.Stat(); err != nil {
		return
	}

	o, ok := c.tree.get(objectId)
	if !ok {
		return 0, ErrorObjNotFound
	}

	if int64(o.Size) != size || int64(o.Offset)+size > fi.Size() {
		return 0, ErrorParamMismatch
	}

	if _, err = c.file.ReadAt(nbuf[:size], int64(o.Offset)); err != nil {
		return
	}
	crc = o.Crc

	return
}
// 刷新chunk文件的索引文件和数据文件
func (s *TinyStore) Sync(fileId uint32) (err error) {
	chunkId := (int)(fileId)
	c, ok := s.chunks[chunkId]
	if !ok {
		return ErrorFileNotFound
	}

	err = c.tree.idxFile.Sync()
	if err != nil {
		return
	}

	return c.file.Sync()
}
// 所有chunk的fileInfo
func (s *TinyStore) GetAllWatermark() (chunks []*FileInfo, err error) {
	chunks = make([]*FileInfo, 0)
	for chunkId, c := range s.chunks {
		ci := &FileInfo{FileId: chunkId, Size: c.loadLastOid()}
		chunks = append(chunks, ci)
	}

	return
}
// 获得某个chunk的chunkinfo
func (s *TinyStore) GetWatermark(fileId uint64) (chunkInfo *FileInfo, err error) {
	chunkId := (int)(fileId)
	c, ok := s.chunks[chunkId]
	if !ok {
		return nil, ErrorFileNotFound
	}
	chunkInfo = &FileInfo{FileId: chunkId, Size: c.loadLastOid()}

	return
}
// 可用的chunk
func (s *TinyStore) GetAvailChunk() (chunkId int, err error) {
	select {
	case chunkId = <-s.availChunkCh:
	default:
		err = ErrorNoAvaliFile
	}

	return
}
// 可用的chunk
func (s *TinyStore) GetChunkForWrite() (chunkId int, err error) {
	chLen := len(s.availChunkCh)
	for i := 0; i < chLen; i++ {
		select {
		case chunkId = <-s.availChunkCh:
			return chunkId, nil
		default:
			return -1, ErrorNoAvaliFile
		}
	}

	return
}
// 所有chunk的索引文件和数据文件都刷新到磁盘
func (s *TinyStore) SyncAll() {
	for _, chunkFp := range s.chunks {
		chunkFp.tree.idxFile.Sync()
		chunkFp.file.Sync()
	}
}
// 关闭所有chunk文件
func (s *TinyStore) CloseAll() {
	for _, chunkFp := range s.chunks {
		chunkFp.tree.idxFile.Close()
		chunkFp.file.Close()
	}
}
// 该chunk变为可用状态
func (s *TinyStore) PutAvailChunk(chunkId int) {
	s.availChunkCh <- chunkId
}

// 获取不可用chunk
func (s *TinyStore) GetUnAvailChunk() (chunkId int, err error) {
	select {
	case chunkId = <-s.unavailChunkCh:
	default:
		err = ErrorNoUnAvaliFile
	}

	return
}

// 该chunk状态置为不可用
func (s *TinyStore) PutUnAvailChunk(chunkId int) {
	s.unavailChunkCh <- chunkId
}

func (s *TinyStore) GetStoreChunkCount() (files int, err error) {
	return TinyChunkCount, nil
}
// 删除某个object
func (s *TinyStore) MarkDelete(fileId uint32, offset, size int64) error {
	chunkId := int(fileId)
	objectId := uint64(offset)
	c, ok := s.chunks[chunkId]
	if !ok {
		return ErrorFileNotFound
	}

	return c.tree.delete(objectId)
}
// UnAvailChan长度
func (s *TinyStore) GetUnAvailChanLen() (chanLen int) {
	return len(s.unavailChunkCh)
}

// AvailChan长度
func (s *TinyStore) GetAvailChanLen() (chanLen int) {
	return len(s.availChunkCh)
}

func (s *TinyStore) AllocObjectId(fileId uint32) (uint64, error) {
	chunkId := int(fileId)
	c, ok := s.chunks[chunkId]
	if !ok {
		return 0, ErrorFileNotFound //0 is an invalid object id
	}
	return c.loadLastOid() + 1, nil
}

func (s *TinyStore) GetLastOid(fileId uint32) (objectId uint64, err error) {
	c, ok := s.chunks[int(fileId)]
	if !ok {
		return 0, ErrorFileNotFound
	}

	return c.loadLastOid(), nil
}

func (s *TinyStore) GetObject(fileId uint32, objectId uint64) (o *Object, err error) {
	c, ok := s.chunks[int(fileId)]
	if !ok {
		return nil, ErrorFileNotFound
	}

	o, ok = c.tree.get(objectId)
	if !ok {
		return nil, ErrorObjNotFound
	}

	return
}

func (s *TinyStore) GetDelObjects(fileId uint32) (objects []uint64) {
	objects = make([]uint64, 0)
	c, ok := s.chunks[int(fileId)]
	if !ok {
		return
	}

	syncLastOid := c.loadLastOid()
	c.storeSyncLastOid(syncLastOid)

	c.commitLock.RLock()
	WalkIndexFile(c.tree.idxFile, func(oid uint64, offset, size, crc uint32) error {
		if oid > syncLastOid {
			return errors.New("Exceed syncLastOid")
		}
		if size == TombstoneFileSize {
			objects = append(objects, oid)
		}
		return nil
	})
	c.commitLock.RUnlock()

	return
}

func (s *TinyStore) ApplyDelObjects(chunkId uint32, objects []uint64) (err error) {
	c, ok := s.chunks[int(chunkId)]
	if !ok {
		return ErrorFileNotFound
	}
	err = c.applyDelObjects(objects)
	return
}

func (s *TinyStore) UpdateStoreInfo() {
	for chunkId, c := range s.chunks {
		finfo, err := c.file.Stat()
		if err != nil {
			continue
		}
		if finfo.Size() >= int64(s.chunkSize) {
			s.fullChunks.Add(chunkId)
		} else {
			s.fullChunks.Remove(chunkId)
		}
	}

	return
}

// make sure chunkId is valid
func (s *TinyStore) IsReadyToCompact(chunkId int) bool {
	c := s.chunks[chunkId]
	tree := c.tree

	if s.fullChunks.Has(chunkId) {
		if tree.fileBytes < uint64(s.chunkSize) {
			return true
		} else {
			return false
		}
	}

	if tree.deleteBytes*100/(tree.fileBytes+1) >= uint64(CompactThreshold) {
		return true
	}

	return false
}

func (s *TinyStore) DoCompactWork(chunkID int) (err error, released uint64) {
	_, ok := s.chunks[chunkID]
	if !ok {
		return ErrorFileNotFound, 0
	}

	err, released = s.doCompactAndCommit(chunkID)
	if err != nil {
		return err, 0
	}
	err = s.Sync(uint32(chunkID))
	if err != nil {
		return err, 0
	}

	return nil, released
}

func (s *TinyStore) MoveChunkToUnavailChan() {
	if len(s.unavailChunkCh) >= 3 {
		return
	}
	for i := 0; i < 3; i++ {
		select {
		case chunkId := <-s.availChunkCh:
			s.unavailChunkCh <- chunkId
		default:
			return
		}
	}
}

func (s *TinyStore) doCompactAndCommit(chunkID int) (err error, released uint64) {
	cc := s.chunks[chunkID]
	// prevent write and delete operations
	if !cc.compactLock.TryLockTimed(CompactMaxWait) {
		return nil, 0
	}
	defer cc.compactLock.Unlock()

	sizeBeforeCompact := cc.tree.FileBytes()
	if err = cc.doCompact(); err != nil {
		return ErrorCompaction, 0
	}

	cc.commitLock.Lock()
	defer cc.commitLock.Unlock()

	err = cc.doCommit()
	if err != nil {
		return ErrorCommit, 0
	}

	sizeAfterCompact := cc.tree.FileBytes()
	released = sizeBeforeCompact - sizeAfterCompact
	return nil, released
}

func CheckAndCreateSubdir(name string) (err error) {
	return os.MkdirAll(name, 0755)
}

func (s *TinyStore) GetChunkInCore(fileID uint32) (*Chunk, error) {
	chunkID := (int)(fileID)
	cc, ok := s.chunks[chunkID]
	if !ok {
		return nil, ErrorFileNotFound
	}
	return cc, nil
}

func (s *TinyStore) Snapshot() ([]*proto.File, error) {
	fList, err := ioutil.ReadDir(s.dataDir)
	if err != nil {
		return nil, err
	}
	var (
		ccID int
	)
	files := make([]*proto.File, 0)
	for _, info := range fList {
		var cc *Chunk
		if ccID, err = strconv.Atoi(info.Name()); err != nil {
			continue
		}
		if ccID > TinyChunkCount {
			continue
		}
		if cc, err = s.GetChunkInCore(uint32(ccID)); err != nil {
			continue
		}

		crc, lastOid, vcCnt := cc.getCheckSum()
		f := &proto.File{Name: info.Name(), Crc: crc, Modified: info.ModTime().Unix(), MarkDel: false, LastObjID: lastOid, NeedleCnt: vcCnt}
		files = append(files, f)
	}

	return files, nil
}
