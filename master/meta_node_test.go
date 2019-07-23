package master

import (
	"fmt"
	"testing"
	"time"
)

func TestMetaNode(t *testing.T) {
	// /metaNode/add and /metaNode/response processed by mock meta server
	addr := mms6Addr
	addMetaServer(addr)
	server.cluster.checkMetaNodeHeartbeat()
	time.Sleep(5 * time.Second)
	getMetaNodeInfo(addr, t)
	offlineMetaNode(addr, t)
}

func getMetaNodeInfo(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, GetMetaNode, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}

func offlineMetaNode(addr string, t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?addr=%v", hostAddr, MetaNodeOffline, addr)
	fmt.Println(reqUrl)
	process(reqUrl, t)
}
