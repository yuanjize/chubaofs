package master

import (
	"fmt"
	"testing"
)

func TestGetAllVols(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v", hostAddr, GetALLVols)
	process(reqUrl, t)
}

func TestGetMetaPartitions(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, ClientMetaPartitions, commonVolName)
	process(reqUrl, t)
}

func TestGetDataPartitions(t *testing.T) {
	reqUrl := fmt.Sprintf("%v%v?name=%v", hostAddr, ClientDataPartitions, commonVolName)
	process(reqUrl, t)
}
