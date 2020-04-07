package checktool

import (
	"sync"
	"github.com/chubaofs/chubaofs/util/config"
	"fmt"
	"time"
	"strconv"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"github.com/chubaofs/chubaofs/util/ump"
	"github.com/chubaofs/chubaofs/util/log"
	"strings"
)

const (
	cfgKeyUsedRatio        = "usedRatio"
	cfgKeyAvailSpaceRatio  = "availSpaceRatio"
	cfgKeyReadWriteDpRatio = "readWriteDpRatio"
	cfgKeyMinRWCnt         = "minRWCnt"
	cfgKeyDomains          = "domains"
	domainSeparator = ","
	UMPKey                 = "checktool"
	TB                     = 1024 * 1024 * 1024 * 1024
	GB                     = 1024 * 1024 * 1024
)

type Server struct {
	usedRatio         float64
	availSpaceRatio   float64
	readWriteDpRatio  float64
	hosts             []string
	minReadWriteCount int64
	lastWarnTime      int64
	wg                sync.WaitGroup
}

func NewServer() *Server {
	return &Server{}
}

func WarnBySpecialUmpKey(umpKey, msg string) {
	log.LogWarn(msg)
	ump.Alarm(umpKey, msg)
}

func (s *Server) Start(cfg *config.Config) (err error) {
	err = s.parseConfig(cfg)
	if err != nil {
		return
	}
	go s.checkVol()
	s.wg.Add(1)
	return
}

func (s *Server) checkVol() {
	go func() {
		s.checkAvailSpaceAndVolsStatus()
		for {
			t := time.NewTimer(time.Minute)
			select {
			case <-t.C:
				s.checkAvailSpaceAndVolsStatus()
			}
		}
	}()
}

func (s *Server) checkAvailSpaceAndVolsStatus() {
	for _, host := range s.hosts {
		log.LogWarnf("check [%v] begin", host)
		s.CheckVolHealth(host)
		log.LogWarnf("check [%v] end", host)
	}
}

func getAllVolStat(host string) (vols map[string]*VolSpaceStat, err error) {
	reqURL := fmt.Sprintf("http://%v/admin/getCluster", host)
	//fmt.Println(reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		fmt.Println(err)
		return
	}
	msg, _ := ioutil.ReadAll(resp.Body)
	cv := &ClusterView{}
	if err = json.Unmarshal(msg, cv); err != nil {
		return
	}
	vols = make(map[string]*VolSpaceStat, 0)
	for _, vss := range cv.VolStat {
		vols[vss.Name] = vss
	}
	return
}

func (s *Server) CheckVolHealth(host string) {
	volStats, err := getAllVolStat(host)
	if err != nil {
		return
	}
	for _, vss := range volStats {
		//fmt.Printf("vol[%v],capacity:%v,used:%v,ratio:%v\n", volName, vss.TotalGB, vss.UsedGB, vss.UsedRatio)
		useRatio, err := strconv.ParseFloat(vss.UsedRatio, 64)
		if err != nil {
			fmt.Printf("check vol[%v] failed,err[%v]\n", vss.Name, err)
			return
		}

		vol, err := getVolSimpleView(vss.Name, host)
		if err != nil {
			fmt.Printf("check vol[%v] failed,err[%v]\n", vss.Name, err)
			return
		}
		if useRatio > s.usedRatio {
			msg := host + fmt.Sprintf(" vol[%v],useRatio larger than [%v],RwCnt[%v],DpCnt[%v],capGB:[%v],useRatio[%v],usedGB[%v],AvailSpaceAllocatedGB[%v]\n",
				vol.Name, s.usedRatio, vol.RwDpCnt, vol.DpCnt, vol.Capacity, useRatio, vss.UsedGB, vol.AvailSpaceAllocated)
			WarnBySpecialUmpKey(UMPKey, msg)
		}
		//fmt.Printf("vol[%v],cap[%v],AvailSpaceAllocated[%v],RwCnt[%v],DpCnt[%v]\n", volName, vol.Capacity, vol.AvailSpaceAllocated, vol.RwDpCnt, vol.DpCnt)
		if vol.RwDpCnt < int(s.minReadWriteCount) || float64(vol.RwDpCnt)/float64(vol.DpCnt) < s.readWriteDpRatio {
			msg := host + fmt.Sprintf(" vol[%v],RwCnt less than [%v],RwCnt[%v],DpCnt[%v],capGB:[%v],useRatio[%v],usedGB[%v],AvailSpaceAllocatedGB[%v]\n",
				vol.Name, s.minReadWriteCount, vol.RwDpCnt, vol.DpCnt, vol.Capacity, useRatio, vss.UsedGB, vol.AvailSpaceAllocated)
			WarnBySpecialUmpKey(UMPKey, msg)
		}
		availableSpaceRatio := float64(vol.AvailSpaceAllocated) / float64(vol.Capacity)
		if availableSpaceRatio < s.availSpaceRatio {
			if vss.UsedGB < vol.AvailSpaceAllocated {
				continue
			}
			if vol.Capacity > 100*1024 && vol.AvailSpaceAllocated > 100*1024 {
				continue
			}
			msg := fmt.Sprintf("vol[%v],availableSpaceRatio less than [%v],RwCnt[%v],DpCnt[%v],capGB:[%v],useRatio[%v],usedGB[%v],AvailSpaceAllocatedGB[%v]\n",
				vol.Name, s.availSpaceRatio, vol.RwDpCnt, vol.DpCnt, vol.Capacity, useRatio, vss.UsedGB, vol.AvailSpaceAllocated)
			leftSpace := vol.Capacity - vol.AvailSpaceAllocated - vss.UsedGB
			count := leftSpace / 120
			msg = host + " " + msg + " " + fmt.Sprintf(generateAllocateDataPartitionURL(host, vss.Name, int(count)))
			WarnBySpecialUmpKey(UMPKey, msg)
		}
		checkMetaPartitions(vss.Name, host)
	}
}

func generateAllocateDataPartitionURL(host, volName string, count int, ) string {
	return fmt.Sprintf("http://%v/dataPartition/create?name=%v&count=%v&type=extent", host, volName, count)
}

func checkMetaPartitions(volName, host string) {
	reqURL := fmt.Sprintf("http://%v/client/metaPartitions?name=%v", host, volName)
	//fmt.Println(reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		fmt.Println(err)
		return
	}
	msg, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	mpvs := make([]*MetaPartitionView, 0)
	if err = json.Unmarshal(msg, &mpvs); err != nil {
		fmt.Println(err)
		return
	}

	for _, mp := range mpvs {
		if mp.LeaderAddr == "" {
			warnMsg := fmt.Sprintf("host[%v],vol[%v],meta partition[%v] no leader", host, volName, mp.PartitionID)
			WarnBySpecialUmpKey(UMPKey, warnMsg)
		}
		checkMetaPartition(host, volName, mp.PartitionID)
	}
	return
}

func checkMetaPartition(host, volName string, id uint64) {
	reqURL := fmt.Sprintf("http://%v/client/metaPartition?name=%v&id=%v", host, volName, id)
	//fmt.Println(reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		fmt.Println(err)
		return
	}
	msg, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	mp := &MetaPartition{}
	if err = json.Unmarshal(msg, mp); err != nil {
		fmt.Println(err)
		return
	}
	if len(mp.Replicas) != int(mp.ReplicaNum) {
		warnMsg := fmt.Sprintf("host[%v],vol[%v],meta partition[%v],miss replica,json[%v]", host, volName, mp.PartitionID, string(msg))
		WarnBySpecialUmpKey(UMPKey, warnMsg)
	}
	return
}

func getVolSimpleView(volName, host string) (vsv *SimpleVolView, err error) {
	reqURL := fmt.Sprintf("http://%v/admin/getVol?name=%v", host, volName)
	//fmt.Println(reqURL)
	resp, err := http.Get(reqURL)
	if err != nil {
		fmt.Println(err)
		return
	}
	msg, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	vsv = &SimpleVolView{}
	if err = json.Unmarshal(msg, vsv); err != nil {
		fmt.Println(err)
		return
	}
	return
}

func (s *Server) parseConfig(cfg *config.Config) (err error) {
	useRatio := cfg.GetFloat(cfgKeyUsedRatio)
	if useRatio <= 0 {
		return fmt.Errorf("parse usedRatio failed")
	}
	s.usedRatio = useRatio
	availSpaceRatio := cfg.GetFloat(cfgKeyAvailSpaceRatio)
	if availSpaceRatio <= 0 {
		return fmt.Errorf("parse availSpaceRatio failed")
	}
	s.availSpaceRatio = availSpaceRatio
	readWriteDpRatio := cfg.GetFloat(cfgKeyReadWriteDpRatio)
	if readWriteDpRatio <= 0 {
		return fmt.Errorf("parse availSpaceRatio failed")
	}
	s.readWriteDpRatio = readWriteDpRatio

	minRWCnt := cfg.GetFloat(cfgKeyMinRWCnt)
	if minRWCnt <= 0 {
		return fmt.Errorf("parse minRWCnt failed")
	}
	s.minReadWriteCount = int64(minRWCnt)
	domains := cfg.GetString(cfgKeyDomains)
	if domains == "" {
		return fmt.Errorf("parse domains failed,domains can't be nil")
	}
	s.hosts = strings.Split(domains, domainSeparator)
	fmt.Printf("usedRatio[%v],availSpaceRatio[%v],readWriteDpRatio[%v],minRWCnt[%v],domains[%v]\n",
		s.usedRatio, s.availSpaceRatio, s.readWriteDpRatio, s.minReadWriteCount,s.hosts)
	return
}

func (s *Server) Shutdown() {
	s.wg.Done()
}

func (s *Server) Sync() {
	s.wg.Wait()
}
