package main

import (
	"fmt"
	"net/http"
	"io/ioutil"
	"encoding/json"
	"strings"
	"runtime"
	"time"
)

var (
	maxPartitionId  = 68732
	verifyPartition chan int
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	verifyPartition = make(chan int, 10)
	for i := 1; i <= 10; i++ {
		go verifyWorker()
	}
	for partitionId := 1; partitionId <= maxPartitionId; partitionId++ {
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
			err := verifyDataPartition(partitionId)
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

type FileMeta struct {
	Metas []*FileCrc
	Name  string
}

type FileCrc struct {
	Crc     uint32
	LocAddr string
	Size    int
}

func verifyDataPartition(partitionId int) (err error) {
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
		err = fmt.Errorf("cannot get partitonId %v result", partitionId)
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("cannot get partitonId %v result status code %v", partitionId, resp.StatusCode)
		return
	}
	data, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	fi := new(FileIncore)
	err = json.Unmarshal(data, fi)
	if err != nil {
		err = fmt.Errorf("cannot get partitonId %v result status code %v json unmash %v", partitionId, resp.StatusCode, err.Error())
		return
	}
	for _, file := range fi.FileInCoreMap {
		metas := file.Metas
		success := true
		for index := 0; index < len(metas)-1; index++ {
			if metas[index].Size == metas[index+1].Size {
				if metas[index].Crc != metas[index+1].Crc {
					fmt.Println(fmt.Sprintf("file %v_%v failed verify", partitionId, file.Name))
					success = false
				}
			}
		}
		if success {
			fmt.Println(fmt.Sprintf("file %v_%v success verify", partitionId, file.Name))
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
