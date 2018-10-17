package master

import (
	"fmt"
	"github.com/tiglabs/containerfs/util"
	"strconv"
)

type DataNodeSpaceStat struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB uint64
	UsedRatio  string
}

type MetaNodeSpaceStat struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB uint64
	UsedRatio  string
}

func (c *Cluster) checkAvailSpace() {
	c.checkDataNodeAvailSpace()
	c.checkVolAvailSpace()
}

func (c *Cluster) checkDataNodeAvailSpace() {
	var (
		total uint64
		used  uint64
	)
	c.dataNodes.Range(func(addr, node interface{}) bool {
		dataNode := node.(*DataNode)
		total = total + dataNode.Total
		used = used + dataNode.Used
		return true
	})
	if total <= 0 {
		return
	}
	useRate := float64(used) / float64(total)
	if useRate > SpaceAvailRate {
		Warn(c.Name, fmt.Sprintf("clusterId[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please add dataNode",
			c.Name, useRate, used, total))
	}
	c.dataNodeSpace.TotalGB = total/util.GB
	newUsed  := used/util.GB
	c.dataNodeSpace.IncreaseGB = newUsed - c.dataNodeSpace.UsedGB
	c.dataNodeSpace.UsedGB = newUsed
	c.dataNodeSpace.UsedRatio = strconv.FormatFloat(useRate, 'f', 3, 32)
}

func (c *Cluster) checkVolAvailSpace() {
	vols := c.copyVols()
	for _, vol := range vols {
		used, total := vol.statSpace()
		if total <= 0 {
			continue
		}
		useRate := float64(used) / float64(total)
		if useRate > SpaceAvailRate {
			Warn(c.Name, fmt.Sprintf("clusterId[%v] vol[%v] space utilization reached [%v],usedSpace[%v],totalSpace[%v] please allocate dataPartition",
				c.Name, vol.Name, useRate, used, total))
		}
	}
}
