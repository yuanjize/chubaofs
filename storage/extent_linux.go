package storage

import (
	"syscall"
)

const (
	FALLOC_FL_KEEP_SIZE  = 1
	FALLOC_FL_PUNCH_HOLE = 2
)
// （增加）对[off,off+len)的数据初始化为0,但是不改变文件大小，对后续的apped有优化操作
func (e *fsExtent) tryKeepSize(fd int, off int64, len int64) (err error) {
	err = syscall.Fallocate(fd, FALLOC_FL_KEEP_SIZE, off, len)
	return
}
// 打洞，干掉[off,off+len)的数据，占用的磁盘减小
func (e *fsExtent) tryPunchHole(fd int, off int64, len int64) (err error) {
	err = syscall.Fallocate(fd, FALLOC_FL_PUNCH_HOLE, off, len)
	return
}
