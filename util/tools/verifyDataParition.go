package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/chubaofs/chubaofs/util"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"time"
	//"strconv"
)

var (
	maxPartitionId  = 189451
	verifyPartition chan int
	isOnlyCheckSize = flag.Bool("checkSize", true, "isonly check dataPartitionSize")
)

var (
	aa = "65527 56255 54649 59941 62755 166216 161510 92617 97293 146300 135822 75307 113628 143986 36893 163773 100776 151056 155729 66851 112517 167387 137016 93854 142857 116008 125301 124141 79215 156825 47782 160315 41468 110095 57147 35144 94920 87119 134653 101941 43862 82104 70602 133477 162665 89221 123033 149886 126521 77254 80184 85950 132340 44301 117185 104290 46475 154496 59238 127585 84027 99634 73036 118401 111287 107775 128810 98420 90425 152166 52587 121837 145183 69160 147529 114853 159157 131193 165007 83067 96168 76270 91452 81149 85015 105457 53502 74180 39728 41036 140484 108957 71636 129971 120705 64141 33620 157963 138153 32530 141693 60642 153342 67684 148644 139355 106634 38201 119515 78236 103130"
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
	verifyPartition = make(chan int, 10)
	for i := 1; i <= 10; i++ {
		go verifyWorker()
	}
	// arr:=strings.Split(aa," ")
	for i := 1; i < maxPartitionId; i++ {
		partitionId := i
		verifyPartition <- partitionId
	}
	for {
		time.Sleep(time.Hour)
	}
}

func verifyWorker() {
	for {
		select {
		case partitionId := <-verifyPartition:
			err := verifyDataPartition(partitionId, *isOnlyCheckSize)
			if err != nil {
				fmt.Println(fmt.Sprintf("verify partitionId %v FAILED %v", partitionId, err.Error()))
			} else {
				fmt.Println(fmt.Sprintf("verify partitionId %v SUCCESS", partitionId))
			}

		}
	}
}

type VolName struct {
	VolName string
}

type FileIncore struct {
	FileInCoreMap map[int]*FileMeta
}

type DataPartition struct {
	Replica          []*Replica
	ReplicaNum       int
	PersistenceHosts []string
	VolName          string
	FileInCoreMap    map[int]*FileMeta
	PartitionID      int
}

type Replica struct {
	Addr                    string
	ReportTime              int64
	FileCount               int
	Status                  int
	LoadPartitionIsResponse bool
	TotalSize               int
	UsedSize                int
	NeedCompare             bool
	DiskPath                string
}

type FileMeta struct {
	Metas []*FileCrc
	Name  string
}

type FileCrc struct {
	Crc     uint32
	LocAddr string
	Size    int
}

func verifyDataPartition(partitionId int, isOnlyVerifySize bool) (err error) {
	if isOnlyVerifySize {
		return checkDataPartitionSize(partitionId)
	}
	vname, err := getVolName(partitionId)
	if err != nil {
		return err
	}
	if err = loadDataParititon(partitionId, vname); err != nil {
		return err
	}
	if err = getDataPartitionResult(partitionId); err != nil {
		return err
	}
	return nil
}

func getDataPartitionResult(partitionId int) (err error) {
	time.Sleep(time.Second * 5)
	loadUrl := fmt.Sprintf("http://dbbak.jd.local/dataPartition/get?id=%v", partitionId)
	resp, err := http.Get(loadUrl)
	if err != nil {
		err = fmt.Errorf("cannot get partitonId %v result %v", partitionId, err)
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("cannot get partitonId %v result status code %v", partitionId, resp.StatusCode)
		return
	}
	data, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	dp := new(DataPartition)
	err = json.Unmarshal(data, dp)
	if err != nil {
		err = fmt.Errorf("cannot get partitonId %v result status code %v json unmash %v", partitionId, resp.StatusCode, err.Error())
		return
	}
	maxUsed := 0
	var maxReportTime int64
	for i := 0; i < len(dp.Replica); i++ {
		if maxUsed < dp.Replica[i].UsedSize {
			maxUsed = dp.Replica[i].UsedSize
		}
		if maxReportTime < dp.Replica[i].ReportTime {
			maxReportTime = dp.Replica[i].ReportTime
		}
	}
	for i := 0; i < len(dp.Replica); i++ {
		var (
			err1, err2 error
		)
		if maxUsed-dp.Replica[i].UsedSize > util.MB {
			err1 = fmt.Errorf(fmt.Sprintf("checkPartition %v failed on %v maxUsedSize %v currentUsedSize %v",
				dp.PartitionID, dp.Replica[i].Addr, maxUsed, dp.Replica[i].UsedSize))
		}
		if maxReportTime-dp.Replica[i].ReportTime > 3600 {
			err2 = fmt.Errorf(fmt.Sprintf("checkPartition %v failed on %v maxReportTime %v currenReportTime %v",
				dp.PartitionID, dp.Replica[i].Addr, maxReportTime, dp.Replica[i].ReportTime))
		}
		if err1 != nil && err2 != nil {
			return fmt.Errorf("error1 %v error2 %v", err1.Error(), err2.Error())
		}
		if err1 != nil {
			return err1
		}
		if err2 != nil {
			return err2
		}

	}
	if len(dp.FileInCoreMap) == 0 {
		err = fmt.Errorf("cannot get partitonId %v result status code %v fileInCoremap 0 ", partitionId, resp.StatusCode)
		return err
	}
	for _, file := range dp.FileInCoreMap {
		metas := file.Metas
		if len(metas) == 0 {
			return
		}
		for index := 0; index < len(metas)-1; index++ {
			if metas[index].Size == metas[index+1].Size {
				if metas[index].Crc != metas[index+1].Crc {
					err = fmt.Errorf(fmt.Sprintf("file %v_%v failed verify", partitionId, file.Name))
					return err
				}
			}
		}

		fmt.Println(fmt.Sprintf("file %v_%v success verify", partitionId, file.Name))
	}
	return nil
}

func checkDataPartitionSize(partitionId int) (err error) {
	getPartitionUrl := fmt.Sprintf("http://dbbak.jd.local/dataPartition/get?id=%v", partitionId)
	resp, err := http.Get(getPartitionUrl)
	if err != nil {
		err = fmt.Errorf("cannot get partitonId %v result error %v", partitionId, err.Error())
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("cannot get partitonId %v result status code %v", partitionId, resp.StatusCode)
		return
	}
	data, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	dp := new(DataPartition)
	err = json.Unmarshal(data, dp)
	if err != nil {
		err = fmt.Errorf("cannot get partitonId %v result status code %v json unmash %v", partitionId, resp.StatusCode, err.Error())
		return
	}
	maxUsed := 0
	var maxReportTime int64
	for i := 0; i < len(dp.Replica); i++ {
		if maxUsed < dp.Replica[i].UsedSize {
			maxUsed = dp.Replica[i].UsedSize
		}
		if maxReportTime < dp.Replica[i].ReportTime {
			maxReportTime = dp.Replica[i].ReportTime
		}
	}
	for i := 0; i < len(dp.Replica); i++ {
		if maxUsed-dp.Replica[i].UsedSize > util.MB {
			return fmt.Errorf(fmt.Sprintf("checkPartition %v failed on %v maxUsedSize %v currentUsedSize %v",
				dp.PartitionID, dp.Replica[i].Addr, maxUsed, dp.Replica[i].UsedSize))
		}
		if maxReportTime-dp.Replica[i].ReportTime > 3600 {
			return fmt.Errorf(fmt.Sprintf("checkPartition %v failed on %v maxReportTime %v currenReportTime %v",
				dp.PartitionID, dp.Replica[i].Addr, maxReportTime, dp.Replica[i].ReportTime))
		}
	}

	return nil
}

func loadDataParititon(partitionId int, vname string) (err error) {
	loadUrl := fmt.Sprintf("http://dbbak.jd.local/dataPartition/load?id=%v&name=%v", partitionId, vname)
	resp, err := http.Get(loadUrl)
	if err != nil {
		err = fmt.Errorf("cannot load  partitonId %v", partitionId)
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("cannot load volname partitonId %v status code %v", partitionId, resp.StatusCode)
		return
	}
	data, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if strings.Contains(string(data), "success") {
		return nil
	}
	return fmt.Errorf("load dataPartition %v failed %v", partitionId, string(data))
}

func getVolName(partitionId int) (volName string, err error) {
	loadUrl := fmt.Sprintf("http://dbbak.jd.local/dataPartition/get?id=%v", partitionId)
	resp, err := http.Get(loadUrl)
	if err != nil {
		err = fmt.Errorf("cannot get volname partitonId %v", partitionId)
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("cannot get volname partitonId %v status code %v", partitionId, resp.StatusCode)
		return
	}
	data, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	vn := new(VolName)
	err = json.Unmarshal(data, vn)
	if err != nil {
		err = fmt.Errorf("cannot get volname partitonId %v status code %v json marsh error %v, body %v",
			partitionId, resp.StatusCode, err.Error(), string(data))
		return
	}
	volName = vn.VolName
	return
}
