package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	adminHost string
	volName   string
	statsTime int64
)

type volStat struct {
	Name      string
	TotalSize uint64
	UsedSize  uint64
}

type MetaPartition struct {
	PartitionID uint64
	LeaderAddr  string
	Members     []string
}

type vol struct {
	MetaPartitions []MetaPartition
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
}

type Dentry struct {
	ParentId uint64
	Name     string
	Inode    uint64
	Type     uint32
}

func (d *Dentry) reset() {
	d.ParentId = 0
	d.Name = ""
	d.Inode = 0
	d.Type = 0
}

func init() {
	flag.StringVar(&adminHost, "h", "", "master address")
	flag.StringVar(&volName, "vol", "", "volume name")
	flag.Int64Var(&statsTime, "t", time.Now().Unix(), "stats time: default now")
	flag.Parse()
}

func getVolStat(adminHosts, volName string) (stat *volStat, err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/client/volStat?name=%s",
		adminHosts, volName))
	if err != nil {
		return
	}
	if resp.StatusCode != 200 {
		err = fmt.Errorf("unavali code %v", resp.StatusCode)
		return
	}
	stat = &volStat{}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err = dec.Decode(stat); err != nil {
		err = fmt.Errorf("unmarshal volStat%v", resp.StatusCode)
		return
	}
	resp.Body.Close()
	return
}

func getMetaPartition(adminHosts, volName string) (metaHost string, metaId uint64, err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/client/vol?name=%s",
		adminHosts, volName))
	if err != nil {
		return
	}
	vols := &vol{}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err = dec.Decode(vols); err != nil {
		err = fmt.Errorf("unmarshal vol %v", resp.StatusCode)
		return
	}
	resp.Body.Close()
	// get meta inode and stats
	if len(vols.MetaPartitions) == 0 {
		err = fmt.Errorf("vols metaPartitions is emtpy")
		return
	}

	metaHost = vols.MetaPartitions[0].LeaderAddr
	metaId = vols.MetaPartitions[0].PartitionID

	return
}

func compare(inoMap map[uint64]*Inode, dMap map[uint64]*Dentry) (unavaliInode []*Inode,
	unavaliDentry []*Dentry, lackPid []*Inode, unavaliInodeSize, unavaliDentryCnt, lackPidSize, validSize uint64) {
	unavaliInode = make([]*Inode, 0)
	unavaliDentry = make([]*Dentry, 0)
	lackPid = make([]*Inode, 0)

	for ino, iinfo := range inoMap {
		d := dMap[ino]
		if d == nil {
			unavaliInode = append(unavaliInode, iinfo)
			unavaliInodeSize += iinfo.Size
		}
	}

	for ino, iinfo := range inoMap {
		d := dMap[ino]
		if d != nil {
			validSize += iinfo.Size
		}
	}

	for ino, dentry := range dMap {
		_, ok := inoMap[ino]
		if !ok {
			unavaliDentry = append(unavaliDentry, dentry)
			unavaliDentryCnt++
		}
	}

	for _, dentry := range dMap {
		_, ok := inoMap[dentry.ParentId]
		if !ok {
			inode, ok1 := inoMap[dentry.Inode]
			if ok1 {
				lackPid = append(lackPid, inode)
				lackPidSize += inode.Size
			}
		}
	}

	return
}

func main() {
	if adminHost == "" || volName == "" {
		flag.Usage()
		return
	}

	vstat, err := getVolStat(adminHost, volName)
	if err != nil {
		fmt.Println(fmt.Sprintf("get vol stat error %v", err))
		return
	}

	metaHost, metaId, err := getMetaPartition(adminHost, volName)
	if err != nil {
		fmt.Println(fmt.Sprintf("get vol stat error %v", err))
		return
	}

	// get all inodes
	inoMap, metaSize, err := getAllInodes(metaHost, metaId)
	if err != nil {
		fmt.Println("get all inodes: ", err.Error())
		return
	}

	dMap, err := getAllDentry(metaHost, metaId)
	if err != nil {
		fmt.Println("get all dentry: ", err.Error())
	}

	UnavaliInode, UnavaliDentry, LackPid, UnavaliInodeSize, UnavaliDentryCnt, LackPidSize, validSize := compare(inoMap, dMap)

	fp, err := os.OpenFile("./UnavaliInode.txt",
		os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for _, inode := range UnavaliInode {
		data, _ := json.Marshal(inode)
		data = append(data, byte('\n'))
		fp.Write(data)
	}
	fp.Close()

	fp, err = os.OpenFile("./UnavaliDentry.txt",
		os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for _, dentry := range UnavaliDentry {
		data, _ := json.Marshal(dentry)
		data = append(data, byte('\n'))
		fp.Write(data)
	}
	fp.Close()

	fp, err = os.OpenFile("./LackPid.txt",
		os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for _, inode := range LackPid {
		data, _ := json.Marshal(inode)
		data = append(data, byte('\n'))
		fp.Write(data)
	}
	fp.Close()

	data, err := json.Marshal(map[string]interface{}{
		"master": vstat,
		"meta": map[string]interface{}{
			"total":            metaSize,
			"validUsed":        validSize,
			"invalidTotal":     metaSize - validSize,
			"UnavaliDentryCnt": UnavaliDentryCnt,
			"invalidInoSize":   UnavaliInodeSize,
			"LackPidSize":      LackPidSize,
		},
	})
	if err != nil {
		fmt.Println("marshal stats: ", err.Error())
		return
	}
	fmt.Printf("%s\n", data)
}

func getAllInodes(host string, pid uint64) (imap map[uint64]*Inode,
	size uint64, err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:9092/getInodeRange?id=%d",
		strings.Split(host, ":")[0], pid))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	imap = make(map[uint64]*Inode)
	var buf []byte
	for {
		var (
			line     []byte
			isPrefix bool
		)
		line, isPrefix, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		buf = append(buf, line...)
		if isPrefix {
			continue
		}
		ino := &Inode{}
		if err = json.Unmarshal(buf, ino); err != nil {
			return
		}
		buf = buf[:0]
		imap[ino.Inode] = ino
		size += ino.Size
	}
	return
}

func getAllDentry(host string, pid uint64) (dmap map[uint64]*Dentry, err error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:9092/getDentry?pid=%d",
		strings.Split(host, ":")[0], pid))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	var buf []byte
	den := &Dentry{}
	dmap = make(map[uint64]*Dentry)
	for {
		var (
			line     []byte
			isPrefix bool
		)
		line, isPrefix, err = reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				err = nil
				return
			}
			return
		}
		buf = append(buf, line...)
		if isPrefix {
			continue
		}
		den = &Dentry{}
		if err = json.Unmarshal(buf, den); err != nil {
			return
		}
		buf = buf[:0]
		dmap[den.Inode] = den
	}
}
