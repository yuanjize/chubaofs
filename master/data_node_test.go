package master

import (
	"fmt"
	"testing"
	"time"
)

func TestDataNode(t *testing.T) {
	// /dataNode/add and /dataNode/response processed by mock data server
	addr := "127.0.0.1:9096"
	addDataServer(addr, DefaultRackName)
	server.cluster.checkDataNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getDataNodeInfo(addr, t)
	offlineDataNode(addr, t)
	_, err := server.cluster.getDataNode(addr)
	if err == nil {
		t.Errorf("offline datanode [%v] failed", addr)
	}
	server.cluster.dataNodes.Delete(addr)
}

func getDataNodeInfo(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, GetDataNode, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func offlineDataNode(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, DataNodeOffline, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}
