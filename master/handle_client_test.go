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

func TestGetToken(t *testing.T) {
	for _, token := range commonVol.tokens {
		reqUrl := fmt.Sprintf("%v%v?name=%v&token=%v",
			hostAddr, TokenGetURI, commonVol.Name, token.Value)
		fmt.Println(reqUrl)
		process(reqUrl, t)
	}
}
