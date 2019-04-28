package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"github.com/chubaofs/chubaofs/third_party/pool"
	"io"
	"net/http"
	"os"
	"time"
)

var (
	adminHost        string
	volName          string
	invalidTime      int64
	do0link          bool
	gConnPool        = pool.NewConnectPool()
	inodeFile        = flag.String("ino", "/tmp/inode.txt", "default inode file")
	dentryFile       = flag.String("dentry", "/tmp/dentry.txt", "default dentry file")
	unavaliInodeFile = flag.String("unavali", "/tmp/unavali.txt", "unavali inode file")
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
	flag.Int64Var(&invalidTime, "t", time.Now().Unix(), "stats time: default now")
	flag.BoolVar(&do0link, "d0", false, "delete nlink 0, default: false")
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
	if metaHost == "" {
		err = fmt.Errorf("meta leader addr is empty")
		return
	}
	metaId = vols.MetaPartitions[0].PartitionID

	return
}

func EvictInode(pID uint64, metaHost string, ino *Inode) (n uint64, err error) {
	p := proto.NewPacket()
	p.Opcode = proto.OpMetaEvictInode

	req := &proto.EvictInodeRequest{
		VolName:     volName,
		PartitionID: pID,
		Inode:       ino.Inode,
	}

	data, err := json.Marshal(req)
	if err == nil {
		p.Data = data
		p.Size = uint32(len(p.Data))
	} else {
		err = fmt.Errorf("ievict ino(%v) json error: err(%v)", ino.Inode, err)
		return
	}
	conn, err := gConnPool.Get(metaHost)
	if err != nil {
		err = fmt.Errorf("ievict ino(%v) get connnect to (%v) error: err(%v)",
			ino.Inode, metaHost, err)
		return
	}
	defer func() {
		if err != nil {
			gConnPool.Put(conn, true)
		} else {
			gConnPool.Put(conn, false)
		}
	}()
	err = p.WriteToConn(conn)
	if err != nil {
		err = fmt.Errorf("ievict ino(%v) write to  host  (%v) error: err(%v)",
			ino.Inode, metaHost, err)
		return
	}
	err = p.ReadFromConn(conn, -1)
	if err != nil {
		err = fmt.Errorf("ievict ino(%v) read from   host  (%v) error: err("+
			"%v)", ino.Inode, metaHost, err)
		return
	}
	if !p.IsOkReply() {
		err = fmt.Errorf("ievict ino(%v) meta   host  (%v) deal error: err("+
			"%v)", ino.Inode, metaHost, string(p.Data[:p.Size]))
		return
	}
	n = ino.Size
	return
}

func compare(inoMap map[uint64]*Inode, dMap map[uint64]*Dentry) (unavaliInode []*Inode,
	unavaliDentry []*Dentry, lackPid []*Inode, nLink0Inode []*Inode, unavaliInodeSize, unavaliDentryCnt, lackPidSize, validSize, nLink0Size uint64) {
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

	for _, iinfo := range inoMap {
		if iinfo.NLink == 0 {
			nLink0Size += iinfo.Size
			nLink0Inode = append(nLink0Inode, iinfo)
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
		fmt.Println(fmt.Sprintf("get meta info error %v", err))
		return
	}

	// get all inodes
	inoMap, metaSize, err := getAllInodes(*inodeFile)
	if err != nil {
		fmt.Println("get all inodes: ", err.Error())
		return
	}

	dMap, err := getAllDentry(*dentryFile)
	if err != nil {
		fmt.Println("get all dentry: ", err.Error())
	}

	UnavaliInode, UnavaliDentry, LackPid, NLink0Inodes, UnavaliInodeSize, UnavaliDentryCnt, LackPidSize, validSize, NLink0Size := compare(inoMap, dMap)

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

	fp, err = os.OpenFile("./NLink0Inode.txt",
		os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0644)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	releaseSize := uint64(0)
	for _, inode := range NLink0Inodes {
		data, _ := json.Marshal(inode)
		if do0link {
			if inode.AccessTime < invalidTime && inode.
				CreateTime < invalidTime && inode.ModifyTime < invalidTime {
				n, err := EvictInode(metaId, metaHost, inode)
				if err != nil {
					fmt.Println(fmt.Sprintf("delete nlink0 err:(%v) ", err.Error()))
				} else {
					releaseSize += n
					data = append(data, byte('Y'))
				}

			}
		}
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
			"Total":             metaSize,
			"ValidUsed":         validSize,
			"UnvalidTotal":      metaSize - validSize,
			"UnavaliDentryCnt":  UnavaliDentryCnt,
			"UnavalidInoSize":   UnavaliInodeSize,
			"LackPidSize":       LackPidSize,
			"NLink0InodeCnt":    len(NLink0Inodes),
			"NLink0Size":        NLink0Size,
			"NLink0ReleaseSize": releaseSize,
		},
	})
	if err != nil {
		fmt.Println("marshal stats: ", err.Error())
		return
	}
	fmt.Printf("%s\n", data)
}

func getAllInodes(inodeFile string) (imap map[uint64]*Inode,
	size uint64, err error) {

	fp, err := os.Open(inodeFile)
	if err != nil {
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
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

func getAllDentry(dentryFile string) (dmap map[uint64]*Dentry, err error) {
	//resp, err := http.Get(fmt.Sprintf("http://%s:9092/getDentry?pid=%d",
	//	strings.Split(host, ":")[0], pid))
	//if err != nil {
	//	return
	//}
	//defer resp.Body.Close()

	fp, err := os.Open(dentryFile)
	if err != nil {
		return
	}
	defer fp.Close()
	reader := bufio.NewReader(fp)
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
