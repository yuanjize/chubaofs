package cmd

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"os"
)

var (
	MasterAddr string
	VolName    string
	InodesFile string
	DensFile   string
)

func init() {
	flag.StringVar(&MasterAddr, "master", "", "master address")
	flag.StringVar(&VolName, "vol", "", "volume name")
	flag.StringVar(&InodesFile, "inodes", "", "inode list file")
	flag.StringVar(&DensFile, "dens", "", "dentry list file")
}

var (
	inodeDumpFileName          string = "inode.dump"
	dentryDumpFileName         string = "dentry.dump"
	inodeUpdateDumpFileName    string = "inode.dump.update"
	obsoleteInodeDumpFileName  string = "inode.dump.obsolete"
	obsoleteDentryDumpFileName string = "dentry.dump.obsolete"
)

type VolStat struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

type MetaPartition struct {
	PartitionID uint64
	LeaderAddr  string
	Members     []string
}

func (mp MetaPartition) String() string {
	data, err := json.Marshal(mp)
	if err != nil {
		return ""
	}
	return string(data)
}

type VolInfo struct {
	MetaPartitions []MetaPartition
}

func (vi *VolInfo) String() string {
	data, err := json.Marshal(vi)
	if err != nil {
		return ""
	}
	return string(data)
}

type ExtentKey struct {
	PartitionId  uint32
	ExtentId     uint64
	Size         uint32
	ExtentOffset uint32
}

type Inode struct {
	Inode      uint64
	Type       uint32
	Size       uint64
	Generation uint64
	CreateTime int64
	AccessTime int64
	ModifyTime int64
	LinkTarget []byte
	NLink      uint32
	MarkDelete uint8
	//Extents    []ExtentKey

	Dentries []*Dentry
	Valid    bool
}

func (i *Inode) String() string {
	data, err := json.Marshal(i)
	if err != nil {
		return ""
	}
	return string(data)
}

type Dentry struct {
	ParentId uint64
	Name     string
	Inode    uint64
	Type     uint32

	Valid bool
}

func (d *Dentry) String() string {
	data, err := json.Marshal(d)
	if err != nil {
		return ""
	}
	return string(data)
}

func walkRawFile(fp *os.File, f func(data []byte) error) (err error) {
	var buf []byte

	reader := bufio.NewReader(fp)
	for {
		var (
			line     []byte
			isPrefix bool
		)

		line, isPrefix, err = reader.ReadLine()

		buf = append(buf, line...)
		if isPrefix {
			continue
		}

		if err != nil {
			if err == io.EOF {
				if len(buf) == 0 {
					err = nil
				} else {
					err = f(buf)
				}
			}
			break
		}

		if err = f(buf); err != nil {
			break
		}
		buf = buf[:0]
	}

	return
}
