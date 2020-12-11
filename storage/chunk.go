package storage

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/tiglabs/baudstorage/util"
)
// 一个chunk对应一个物理文件，里面包含很多小文件
type Chunk struct {
	file        *os.File
	tree        *ObjectTree
	lastOid     uint64
	syncLastOid uint64
	commitLock  sync.RWMutex
	compactLock util.TryMutex
}
// 加载chunk索引文件
func NewChunk(dataDir string, chunkId int) (c *Chunk, err error) {
	c = new(Chunk)
	name := dataDir + "/" + strconv.Itoa(chunkId)
	maxOid, err := c.loadTree(name)
	if err != nil {
		return nil, err
	}

	c.storeLastOid(maxOid)
	return c, nil
}
// 把要删除的oid从b树删除
func (c *Chunk) applyDelObjects(objects []uint64) (err error) {
	for _, needle := range objects {
		c.tree.delete(needle)
	}

	c.storeSyncLastOid(c.loadLastOid())
	return
}
// 加载chunkid.idx b树索引文件，打开chunkid文件
func (c *Chunk) loadTree(name string) (maxOid uint64, err error) {
	if c.file, err = os.OpenFile(name, ChunkOpenOpt, 0666); err != nil {
		return
	}
	var idxFile *os.File
	idxName := name + ".idx"
	if idxFile, err = os.OpenFile(idxName, ChunkOpenOpt, 0666); err != nil {
		c.file.Close()
		return
	}

	tree := NewObjectTree(idxFile)
	if maxOid, err = tree.Load(); err == nil {
		c.tree = tree
	} else {
		idxFile.Close()
		c.file.Close()
	}

	return
}

// returns count of valid objects calculated for CRC // 返回校验值，和校验的object的数目
func (c *Chunk) getCheckSum() (fullCRC uint32, syncLastOid uint64, count int) {
	syncLastOid = c.loadSyncLastOid()
	if syncLastOid == 0 {
		syncLastOid = c.loadLastOid()
	}

	c.tree.idxFile.Sync()
	crcBuffer := make([]byte, 0)
	buf := make([]byte, 4)
	c.commitLock.RLock()
	WalkIndexFile(c.tree.idxFile, func(oid uint64, offset, size, crc uint32) error {
		if oid > syncLastOid {
			return nil
		}
		o, ok := c.tree.get(oid)
		if !ok {
			return nil
		}
		if !o.Check(offset, size, crc) {
			return nil
		}
		binary.BigEndian.PutUint32(buf, o.Crc)
		crcBuffer = append(crcBuffer, buf...)
		count++
		return nil
	})
	c.commitLock.RUnlock()

	fullCRC = crc32.ChecksumIEEE(crcBuffer)
	return
}

func (c *Chunk) loadLastOid() uint64 {
	return atomic.LoadUint64(&c.lastOid)
}

func (c *Chunk) storeLastOid(val uint64) {
	atomic.StoreUint64(&c.lastOid, val)
	return
}

func (c *Chunk) incLastOid() uint64 {
	return atomic.AddUint64(&c.lastOid, uint64(1))
}

func (c *Chunk) loadSyncLastOid() uint64 {
	return atomic.LoadUint64(&c.syncLastOid)
}

func (c *Chunk) storeSyncLastOid(val uint64) {
	atomic.StoreUint64(&c.syncLastOid, val)
	return
}
// 对数据文件和索引文件分别进行合并,其实就是把被删除的文件干掉
func (c *Chunk) doCompact() (err error) {
	var (
		newIdxFile *os.File
		newDatFile *os.File
		tree       *ObjectTree
	)

	name := c.file.Name()
	newIdxName := name + ".cpx"
	newDatName := name + ".cpd"
	if newIdxFile, err = os.OpenFile(newIdxName, ChunkOpenOpt|os.O_TRUNC, 0644); err != nil {
		return err
	}
	defer newIdxFile.Close()

	if newDatFile, err = os.OpenFile(newDatName, ChunkOpenOpt|os.O_TRUNC, 0644); err != nil {
		return err
	}
	defer newDatFile.Close()

	tree = NewObjectTree(newIdxFile)

	if err = c.copyValidData(tree, newDatFile); err != nil {
		return err
	}

	return nil
}
// 复制idx文件和数据文件，如果索引是delete，那么只复制该object的索引，不复制数据。
func (c *Chunk) copyValidData(dstNm *ObjectTree, dstDatFile *os.File) (err error) {
	srcNm := c.tree
	srcDatFile := c.file
	srcIdxFile := srcNm.idxFile
	deletedSet := make(map[uint64]struct{})
	_, err = WalkIndexFile(srcIdxFile, func(oid uint64, offset, size, crc uint32) error {
		var (
			o *Object
			e error

			newOffset int64
		)

		_, ok := deletedSet[oid]
		if size == TombstoneFileSize && !ok {
			o = &Object{Oid: oid, Offset: offset, Size: size, Crc: crc}
			if e = dstNm.appendToIdxFile(o); e != nil {
				return e
			}
			deletedSet[oid] = struct{}{}
			return nil
		}

		o, ok = srcNm.get(oid)
		if !ok {
			return nil
		}

		if !o.Check(offset, size, crc) {
			return nil
		}

		realsize := o.Size
		if newOffset, e = dstDatFile.Seek(0, 2); e != nil {
			return e
		}

		dataInFile := make([]byte, realsize)
		if _, e = srcDatFile.ReadAt(dataInFile, int64(o.Offset)); e != nil {
			return e
		}

		if _, e = dstDatFile.Write(dataInFile); e != nil {
			return e
		}

		o.Offset = uint32(newOffset)
		if e = dstNm.appendToIdxFile(o); e != nil {
			return e
		}

		return nil
	})

	return err
}
// 就是文件重命名
func (c *Chunk) doCommit() (err error) {
	name := c.file.Name()
	c.tree.idxFile.Close()
	c.file.Close()

	err = catchupDeleteIndex(name+".idx", name+".cpx")
	if err != nil {
		return
	}

	err = os.Rename(name+".cpd", name)
	if err != nil {
		return
	}
	err = os.Rename(name+".cpx", name+".idx")
	if err != nil {
		return
	}

	maxOid, err := c.loadTree(name)
	if err == nil && maxOid > c.loadLastOid() {
		// shold not happen, just in case
		c.storeLastOid(maxOid)
	}
	return err
}
// 把老文件末尾的删除部分拷贝到新文件？？
func catchupDeleteIndex(oldIdxName, newIdxName string) error {
	var (
		oldIdxFile, newIdxFile *os.File
		err                    error
	)
	if oldIdxFile, err = os.OpenFile(oldIdxName, os.O_RDONLY, 0644); err != nil {
		return err
	}
	defer oldIdxFile.Close()

	if newIdxFile, err = os.OpenFile(newIdxName, os.O_RDWR, 0644); err != nil {
		return err
	}
	defer newIdxFile.Close()

	newinfo, err := newIdxFile.Stat()
	if err != nil {
		return err
	}

	data := make([]byte, ObjectHeaderSize) // 读出来新文件的最后一项
	_, err = newIdxFile.ReadAt(data, newinfo.Size()-ObjectHeaderSize)
	if err != nil {
		return err
	}

	lastIndexEntry := &Object{}
	lastIndexEntry.Unmarshal(data)

	oldInfo, err := oldIdxFile.Stat()
	if err != nil {
		return err
	}

	catchup := make([]byte, 0)  // 从后往前便利老文件，找到第一个非delet或者和新文件对其的地方
	for offset := oldInfo.Size() - ObjectHeaderSize; offset >= 0; offset -= ObjectHeaderSize {
		_, err = oldIdxFile.ReadAt(data, offset)
		if err != nil {
			return err
		}

		ni := &Object{}
		ni.Unmarshal(data)
		if ni.Size != TombstoneFileSize || ni.IsIdentical(lastIndexEntry) {
			break
		}
		result := make([]byte, len(catchup)+ObjectHeaderSize)
		copy(result, data)
		copy(result[ObjectHeaderSize:], catchup)
		catchup = result
	}

	_, err = newIdxFile.Seek(0, 2)
	if err != nil {
		return err
	}
	_, err = newIdxFile.Write(catchup)
	if err != nil {
		return err
	}

	return nil
}
