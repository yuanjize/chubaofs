package master

import (
	"encoding/base64"
	"fmt"
	"github.com/chubaofs/chubaofs/proto"
	"time"
)

func createToken(volName string, tokenType int8) (token *proto.Token, err error) {
	str := fmt.Sprintf("%v_%v_%v", volName, tokenType, time.Now().UnixNano())
	encodeStr := base64.StdEncoding.EncodeToString([]byte(str))
	token = &proto.Token{
		TokenType: tokenType,
		VolName:   volName,
		Value:     encodeStr,
	}
	return

}
