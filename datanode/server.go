package datanode

import (
	"encoding/json"
	"fmt"
	"github.com/juju/errors"
	"github.com/tiglabs/baudstorage/master"
	"github.com/tiglabs/baudstorage/proto"
	"github.com/tiglabs/baudstorage/storage"
	"github.com/tiglabs/baudstorage/util"
	"github.com/tiglabs/baudstorage/util/config"
	"github.com/tiglabs/baudstorage/util/log"
	"github.com/tiglabs/baudstorage/util/pool"
	"github.com/tiglabs/baudstorage/util/ump"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrStoreTypeMismatch        = errors.New("store type error")
	ErrPartitionNotExist        = errors.New("dataPartition not exists")
	ErrChunkOffsetMismatch      = errors.New("chunk offset not mismatch")
	ErrNoDiskForCreatePartition = errors.New("no disk for create dataPartition")
	ErrBadConfFile              = errors.New("bad config file")

	LocalIP      string
	gConnPool    = pool.NewConnPool()
	MasterHelper = util.NewMasterHelper()
)

const (
	GetIpFromMaster = master.AdminGetIp
	DefaultRackName = "huitian_rack1"
)

const (
	UmpModuleName = "dataNode"
)

const (
	ConfigKeyPort       = "port"       // int
	ConfigKeyClusterID  = "clusterID"  // string
	ConfigKeyMasterAddr = "masterAddr" // array
	ConfigKeyRack       = "rack"       // string
	ConfigKeyDisks      = "disks"      // array
)

type DataNode struct {
	space          SpaceManager
	port           string
	rackName       string
	clusterId      string
	localIp        string
	localServeAddr string
	tcpListener    net.Listener
	stopC          chan bool
	state          uint32
	wg             sync.WaitGroup
}

func NewServer() *DataNode {
	return &DataNode{}
}

func (s *DataNode) Start(cfg *config.Config) (err error) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	if atomic.CompareAndSwapUint32(&s.state, Standby, Start) {
		defer func() {
			if err != nil {
				atomic.StoreUint32(&s.state, Standby)
			} else {
				atomic.StoreUint32(&s.state, Running)
			}
		}()
		if err = s.onStart(cfg); err != nil {
			return
		}
		s.wg.Add(1)
	}
	return
}

func (s *DataNode) Shutdown() {
	if atomic.CompareAndSwapUint32(&s.state, Running, Shutdown) {
		s.onShutdown()
		s.wg.Done()
		atomic.StoreUint32(&s.state, Stopped)
	}
}

func (s *DataNode) Sync() {
	if atomic.LoadUint32(&s.state) == Running {
		s.wg.Wait()
	}
}
/*
	1.parse配置
	2.注册http api
	3.启动startSpaceManager
	4.启动tcp服务
	5.把自己注册到master
*/
func (s *DataNode) onStart(cfg *config.Config) (err error) {
	s.stopC = make(chan bool, 0)
	if err = s.parseConfig(cfg); err != nil {
		return
	}

	go s.registerProfHandler()

	if err = s.startSpaceManager(cfg); err != nil {
		return
	}
	if err = s.startTcpService(); err != nil {
		return
	}

	go s.registerToMaster()
	ump.InitUmp(UmpModuleName)
	return
}

func (s *DataNode) onShutdown() {
	close(s.stopC)
	s.stopTcpService()
	return
}

func (s *DataNode) parseConfig(cfg *config.Config) (err error) {
	var (
		port       string
		regexpPort *regexp.Regexp
	)
	port = cfg.GetString(ConfigKeyPort)
	if regexpPort, err = regexp.Compile("^(\\d)+$"); err != nil {
		return
	}
	if !regexpPort.MatchString(port) {
		err = ErrBadConfFile
		return
	}
	s.port = port
	s.clusterId = cfg.GetString(ConfigKeyClusterID)
	if len(cfg.GetArray(ConfigKeyMasterAddr)) == 0 {
		return ErrBadConfFile
	}
	for _, ip := range cfg.GetArray(ConfigKeyMasterAddr) {
		MasterHelper.AddNode(ip.(string))
	}
	s.rackName = cfg.GetString(ConfigKeyRack)
	if s.rackName == "" {
		s.rackName = DefaultRackName
	}
	log.LogDebugf("action[parseConfig] load masterAddrs[%v].", MasterHelper.Nodes())
	log.LogDebugf("action[parseConfig] load port[%v].", s.port)
	log.LogDebugf("action[parseConfig] load clusterId[%v].", s.clusterId)
	log.LogDebugf("action[parseConfig] load rackName[%v].", s.rackName)
	return
}
// 初始化一下sm，然后根据ConfigKeyDisks配置，把disk加载出来(同时会加载disk下面的partition)
func (s *DataNode) startSpaceManager(cfg *config.Config) (err error) {
	s.space = NewSpaceManager(s.rackName)
	if err != nil || len(strings.TrimSpace(s.port)) == 0 {
		err = ErrBadConfFile
		return
	}
	var wg sync.WaitGroup
	for _, d := range cfg.GetArray(ConfigKeyDisks) {
		log.LogDebugf("action[startSpaceManager] load disk raw config[%v].", d)
		// Format "PATH:RESET_SIZE:MAX_ERR
		arr := strings.Split(d.(string), ":")
		if len(arr) != 3 {
			return ErrBadConfFile
		}
		path := arr[0]
		restSize, err := strconv.ParseUint(arr[1], 10, 64)
		if err != nil {
			return ErrBadConfFile
		}
		maxErr, err := strconv.Atoi(arr[2])
		if err != nil {
			return ErrBadConfFile
		}
		wg.Add(1)
		go func(wg *sync.WaitGroup, path string, restSize uint64, maxErrs int) {
			defer wg.Done()
			s.space.LoadDisk(path, restSize, maxErrs)
		}(&wg, path, restSize, maxErr)
	}
	wg.Wait()
	return nil
}
// 1.从master手里拿到集群的info(GetIpFromMaster)接口
// 2.调用naster的AddDataNode函数，注册自己
func (s *DataNode) registerToMaster() {
	var (
		err  error
		data []byte
	)
	// Get IP address and cluster ID from master.
	for {
		timer := time.NewTimer(0)
		select {
		case <-timer.C:
			data, err = MasterHelper.Request(http.MethodGet, GetIpFromMaster, nil, nil)
			masterAddr := MasterHelper.Leader()
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot get ip from master[%v] err[%v].",
					masterAddr, err)
				timer = time.NewTimer(5 * time.Second)
				continue
			}
			cInfo := new(proto.ClusterInfo)
			json.Unmarshal(data, cInfo)
			LocalIP = string(cInfo.Ip)
			s.clusterId = cInfo.Cluster
			s.localServeAddr = fmt.Sprintf("%s:%v", LocalIP, s.port)
			if !util.IP(LocalIP) {
				log.LogErrorf("action[registerToMaster] got an invalid local ip[%v] from master[%v].",
					LocalIP, masterAddr)
				timer = time.NewTimer(5 * time.Second)
				continue
			}
			// Register this data node to master.
			params := make(map[string]string)
			params["addr"] = fmt.Sprintf("%s:%v", LocalIP, s.port)
			data, err = MasterHelper.Request(http.MethodPost, master.AddDataNode, params, nil)
			if err != nil {
				log.LogErrorf("action[registerToMaster] cannot register this node to master[%] err[%v].",
					masterAddr, err)
				continue
			}
			return
		case <-s.stopC:
			timer.Stop()
			return
		}

	}

}
// 注册一些http api
func (s *DataNode) registerProfHandler() {
	http.HandleFunc("/disks", s.apiGetDisk)
	http.HandleFunc("/partitions", s.apiGetPartitions)
	http.HandleFunc("/partition", s.apiGetPartition)
	http.HandleFunc("/extent", s.apiGetExtent)
	http.HandleFunc("/stats", s.apiGetStat)
}
// 启动tcp服务
func (s *DataNode) startTcpService() (err error) {
	log.LogInfo("Start: startTcpService")
	addr := fmt.Sprintf(":%v", s.port)
	l, err := net.Listen(NetType, addr)
	log.LogDebugf("action[startTcpService] listen %v address[%v].", NetType, addr)
	if err != nil {
		log.LogError("failed to listen, err:", err)
		return
	}
	s.tcpListener = l
	go func(ln net.Listener) {
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.LogErrorf("action[startTcpService] failed to accept, err:%s", err.Error())
				break
			}
			log.LogDebugf("action[startTcpService] accept connection from %s.", conn.RemoteAddr().String())
			go s.serveConn(conn)
		}
	}(l)
	return
}

func (s *DataNode) stopTcpService() (err error) {
	if s.tcpListener != nil {
		s.tcpListener.Close()
		log.LogDebugf("action[stopTcpService] stop tcp service.")
	}
	return
}

func (s *DataNode) serveConn(conn net.Conn) {
	space := s.space
	space.Stats().AddConnection()
	c, _ := conn.(*net.TCPConn)
	c.SetKeepAlive(true)
	c.SetNoDelay(true)

	msgH := NewMsgHandler(c)
	go s.handleRequest(msgH)
	go s.writeToCli(msgH)

	var (
		err error
	)

	defer func() {
		if err != nil && err != io.EOF &&
			!strings.Contains(err.Error(), "closed connection") &&
			!strings.Contains(err.Error(), "reset by peer") {
			log.LogErrorf("action[serveConn] err[%v].", err)
		}
		space.Stats().RemoveConnection()
		conn.Close()
	}()

	for {
		select {
		case <-msgH.exitC:
			log.LogDebugf("action[DataNode.serveConn] event loop for %v exit.", conn.RemoteAddr())
			return
		default:
			if err = s.readFromCliAndDeal(msgH); err != nil {
				msgH.Stop()
				return
			}
		}
	}
}

func (s *DataNode) AddCompactTask(t *CompactTask) (err error) {
	dp := s.space.GetPartition(t.partitionId)
	if dp == nil {
		return
	}
	d := dp.Disk()
	if d == nil {
		return
	}
	err = d.addTask(t)
	if err != nil {
		err = errors.Annotatef(err, "Task[%v] ", t.toString())
	}
	return
}

func (s *DataNode) checkChunkInfo(pkg *Packet) (err error) {
	var (
		chunkInfo *storage.FileInfo
	)
	chunkInfo, err = pkg.DataPartition.GetTinyStore().GetWatermark(pkg.FileID)
	if err != nil {
		return
	}
	leaderObjId := uint64(pkg.Offset)
	localObjId := chunkInfo.Size
	if (leaderObjId - 1) != chunkInfo.Size {
		err = ErrChunkOffsetMismatch
		msg := fmt.Sprintf("Err[%v] leaderObjId[%v] localObjId[%v]", err, leaderObjId, localObjId)
		log.LogWarn(pkg.ActionMsg(ActionCheckChunkInfo, LocalProcessAddr, pkg.StartT, fmt.Errorf(msg)))
	}

	return
}

func (s *DataNode) handleChunkInfo(pkg *Packet) (err error) {
	if !pkg.IsWriteOperation() {
		return
	}

	if !pkg.isHeadNode() {
		err = s.checkChunkInfo(pkg)
	} else {
		err = s.headNodeSetChunkInfo(pkg)
	}
	if err != nil {
		err = errors.Annotatef(err, "Request[%v] handleChunkInfo Error", pkg.GetUniqueLogId())
		pkg.PackErrorBody(ActionCheckChunkInfo, err.Error())
	}

	return
}

func (s *DataNode) headNodeSetChunkInfo(pkg *Packet) (err error) {
	var (
		chunkId int
	)
	store := pkg.DataPartition.GetTinyStore()
	chunkId, err = store.GetChunkForWrite()
	if err != nil {
		pkg.DataPartition.ChangeStatus(proto.ReadOnly)
		return
	}
	pkg.FileID = uint64(chunkId)
	objectId, _ := store.AllocObjectId(uint32(pkg.FileID))
	pkg.Offset = int64(objectId)

	return
}

func (s *DataNode) headNodePutChunk(pkg *Packet) {
	if pkg == nil || pkg.FileID <= 0 || pkg.IsReturn {
		return
	}
	if pkg.StoreMode != proto.TinyStoreMode || !pkg.isHeadNode() || !pkg.IsWriteOperation() || !pkg.IsTransitPkg() {
		return
	}
	store := pkg.DataPartition.GetTinyStore()
	if pkg.IsErrPack() {
		store.PutUnAvailChunk(int(pkg.FileID))
	} else {
		store.PutAvailChunk(int(pkg.FileID))
	}
	pkg.IsReturn = true
}

func (s *DataNode) addDiskErrs(partitionId uint32, err error, flag uint8) {
	if err == nil {
		return
	}
	dp := s.space.GetPartition(partitionId)
	if dp == nil {
		return
	}
	d := dp.Disk()
	if d == nil {
		return
	}
	if !s.isDiskErr(err.Error()) {
		return
	}
	if flag == WriteFlag {
		d.addWriteErr()
	} else if flag == ReadFlag {
		d.addReadErr()
	}
}

func (s *DataNode) isDiskErr(errMsg string) bool {
	if strings.Contains(errMsg, storage.ErrorParamMismatch.Error()) || strings.Contains(errMsg, storage.ErrorFileNotFound.Error()) ||
		strings.Contains(errMsg, storage.ErrorNoAvaliFile.Error()) || strings.Contains(errMsg, storage.ErrorObjNotFound.Error()) ||
		strings.Contains(errMsg, io.EOF.Error()) || strings.Contains(errMsg, storage.ErrSyscallNoSpace.Error()) ||
		strings.Contains(errMsg, storage.ErrorHasDelete.Error()) || strings.Contains(errMsg, ErrPartitionNotExist.Error()) ||
		strings.Contains(errMsg, storage.ErrObjectSmaller.Error()) ||
		strings.Contains(errMsg, storage.ErrPkgCrcMismatch.Error()) || strings.Contains(errMsg, ErrStoreTypeMismatch.Error()) ||
		strings.Contains(errMsg, storage.ErrorNoUnAvaliFile.Error()) ||
		strings.Contains(errMsg, storage.ErrExtentNameFormat.Error()) || strings.Contains(errMsg, storage.ErrorAgain.Error()) ||
		strings.Contains(errMsg, ErrChunkOffsetMismatch.Error()) ||
		strings.Contains(errMsg, storage.ErrorCompaction.Error()) || strings.Contains(errMsg, storage.ErrorPartitionReadOnly.Error()) {
		return false
	}
	return true
}
