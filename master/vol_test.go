package master

import (
	"testing"
	"github.com/chubaofs/chubaofs/util"
	"fmt"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	count := commonVol.calculateExpandNum()
	if count == 0 {
		commonVol.Capacity = 100 * util.TB
	}
	volCount := len(commonVol.dataPartitions.dataPartitions)
	commonVol.dataPartitions.readWriteDataPartitions = 0
	server.cluster.DisableAutoAlloc = false
	t.Logf("status[%v],disableAutoAlloc[%v],used[%v],cap[%v]\n",
		commonVol.Status, server.cluster.DisableAutoAlloc, commonVol.UsedSpace, commonVol.Capacity, )
	commonVol.checkNeedAutoCreateDataPartitions(server.cluster)
	newVolCount := len(commonVol.dataPartitions.dataPartitions)
	if volCount == newVolCount {
		t.Errorf("autoCreateDataPartitions failed,expand 0 data partitions")
		return
	}
}

func TestCheckVol(t *testing.T) {
	commonVol.checkStatus(server.cluster)
	commonVol.checkMetaPartitions(server.cluster)
	commonVol.checkDataPartitions(server.cluster)
	fmt.Printf("writable data partitions[%v]", commonVol.dataPartitions.readWriteDataPartitions)
}
