package master

import (
	"testing"
	"github.com/chubaofs/chubaofs/util"
	"time"
	"fmt"
)

func createDataNodeForTopo(addr, rackName string) (dn *DataNode) {
	dn = NewDataNode(addr, "test")
	dn.RackName = rackName
	dn.Total = 1024 * util.GB
	dn.Used = 10 * util.GB
	dn.Available = 1024 * util.GB
	dn.MaxDiskAvailWeight = 1024 * util.GB
	dn.CreatedVolWeights = 120 * util.GB
	dn.RemainWeightsForCreateVol = 800 * util.GB
	dn.Carry = 0.9
	dn.ReportTime = time.Now()
	dn.isActive = true
	return
}

func TestSingleRack(t *testing.T) {
	//rack name must be DefaultRackName
	racks := server.cluster.t.getAllRacks()
	if racks[0].name != DefaultRackName {
		t.Errorf("rack name should be [%v],but now it's [%v]", DefaultRackName, racks[0].name)
	}
	topo := NewTopology()
	rackName := "test"
	topo.putDataNode(createDataNodeForTopo(mds1Addr, rackName))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, rackName))
	topo.putDataNode(createDataNodeForTopo(mds3Addr, rackName))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, rackName))
	topo.putDataNode(createDataNodeForTopo(mds5Addr, rackName))
	if !topo.isSingleRack() {
		racks := topo.getAllRacks()
		t.Errorf("topo should be single rack,rack num [%v]", len(racks))
		return
	}
	replicaNum := 2
	//single rack exclude,if it is a single rack excludeRacks don't take effect
	excludeRacks := make([]string, 0)
	excludeRacks = append(excludeRacks, rackName)
	racks, err := topo.allocRacks(replicaNum, excludeRacks)
	if err != nil {
		t.Error(err)
		return
	}
	if len(racks) != 1 {
		t.Errorf("expect rack num [%v],len(racks) is %v", 0, len(racks))
		fmt.Println(racks)
		return
	}

	//single rack normal
	racks, err = topo.allocRacks(replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}
	newHosts, err := racks[0].getAvailDataNodeHosts(nil, replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(newHosts)
	racks[0].RemoveDataNode(mds1Addr)
	topo.removeRack(rackName)
}

func TestAllocRacks(t *testing.T) {
	topo := NewTopology()
	rackCount := 3
	//add three racks
	rackName1 := "rack1"
	topo.putDataNode(createDataNodeForTopo(mds1Addr, rackName1))
	topo.putDataNode(createDataNodeForTopo(mds2Addr, rackName1))
	rackName2 := "rack2"
	topo.putDataNode(createDataNodeForTopo(mds3Addr, rackName2))
	topo.putDataNode(createDataNodeForTopo(mds4Addr, rackName2))
	rackName3 := "rack3"
	topo.putDataNode(createDataNodeForTopo(mds5Addr, rackName3))
	racks := topo.getAllRacks()
	if len(racks) != rackCount {
		t.Errorf("expect racks num[%v],len(racks) is %v", rackCount, len(racks))
		return
	}
	//only pass replica num
	replicaNum := 2
	racks, err := topo.allocRacks(replicaNum, nil)
	if err != nil {
		t.Error(err)
		return
	}
	if len(racks) != replicaNum {
		t.Errorf("expect racks num[%v],len(racks) is %v", replicaNum, len(racks))
		return
	}
	cluster := new(Cluster)
	cluster.t = topo
	hosts, err := cluster.ChooseTargetDataHosts(replicaNum)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("ChooseTargetDataHosts in multi racks,hosts[%v]", hosts)
	//test exclude rack
	excludeRacks := make([]string, 0)
	excludeRacks = append(excludeRacks, rackName1)
	racks, err = topo.allocRacks(replicaNum, excludeRacks)
	if err != nil {
		t.Error(err)
		return
	}
	for _, rack := range racks {
		if rack.name == rackName1 {
			t.Errorf("rack [%v] should be exclued", rackName1)
			return
		}
	}
	topo.removeRack(rackName1)
	topo.removeRack(rackName2)
	topo.removeRack(rackName3)
}
