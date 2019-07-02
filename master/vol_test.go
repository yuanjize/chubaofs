package master

import (
	"testing"
	"github.com/chubaofs/chubaofs/util"
	"fmt"
	"github.com/chubaofs/chubaofs/util/log"
)

func TestAutoCreateDataPartitions(t *testing.T) {
	commonVol, err := server.cluster.getVol(commonVolName)
	if err != nil {
		t.Error(err)
	}
	commonVol.Capacity = 300 * util.TB
	dpCount := len(commonVol.dataPartitions.dataPartitions)
	commonVol.dataPartitions.readWriteDataPartitions = 0
	server.cluster.DisableAutoAlloc = false
	t.Logf("status[%v],disableAutoAlloc[%v],used[%v],cap[%v]\n",
		commonVol.Status, server.cluster.DisableAutoAlloc, commonVol.UsedSpace, commonVol.Capacity)
	commonVol.checkNeedAutoCreateDataPartitions(server.cluster)
	newDpCount := len(commonVol.dataPartitions.dataPartitions)
	if dpCount == newDpCount {
		t.Errorf("autoCreateDataPartitions failed,expand 0 data partitions,oldCount[%v],curCount[%v]", dpCount, newDpCount)
		return
	}
}

func TestCheckVol(t *testing.T) {
	commonVol.checkStatus(server.cluster)
	commonVol.checkMetaPartitions(server.cluster)
	commonVol.checkDataPartitions(server.cluster)
	log.LogFlush()
	fmt.Printf("writable data partitions[%v]\n", commonVol.dataPartitions.readWriteDataPartitions)
}
