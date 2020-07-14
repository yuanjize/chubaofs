// Copyright 2018 The CFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/chubaofs/chubaofs/third_party/juju/errors"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
)

const (
	masterResponsePath = "/metaNode/response" // Method: 'POST',
	// ContentType: 'application/json'
)

// ReplyToMaster reply operation result to master by sending http request.
func (m *metaManager) respondToMaster(data interface{}) (err error) {
	// Handle panic
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	// Process data and send reply though http specified remote address.
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return
	}
	postToMaster("POST", masterResponsePath, jsonBytes)
	return
}

// ReplyToClient send reply data though tcp connection to client.
func (m *metaManager) respondToClient(conn net.Conn, p *Packet) (err error) {
	// Handle panic
	defer func() {
		if r := recover(); r != nil {
			switch data := r.(type) {
			case error:
				err = data
			default:
				err = errors.New(data.(string))
			}
		}
	}()
	// Process data and send reply though specified tcp connection.
	p.HandleT = time.Now().UnixNano()
	err = p.WriteToConn(conn)
	p.RespT = time.Now().UnixNano()
	if err != nil {
		recieveDuration := time.Unix(0, p.ReadT).Sub(time.Unix(0, p.StartT))
		if p.StartT == 0 {
			recieveDuration = 0
		}
		handleDuration := time.Unix(0, p.HandleT).Sub(time.Unix(0, p.ReadT))
		respDuration := time.Unix(0, p.RespT).Sub(time.Unix(0, p.HandleT))

		umpKey := UMPKey + "_respondToClientFailed"
		umpMsg := fmt.Sprintf("response to client[%s] failed, request[%s], ReqID[%d], response status[%s], time "+
			"duration [recieve: %v, handle: %v, response: %v]",
			err.Error(), p.GetOpMsg(), p.ReqID, p.GetResultMesg(),
			recieveDuration, handleDuration, respDuration)
		ump.Alarm(umpKey, umpMsg)

		log.LogErrorf(umpMsg)
	}
	return
}

func (m *metaManager) responseAckOKToMaster(conn net.Conn, p *Packet) {
	go func() {
		p.PackOkReply()
		if err := p.WriteToConn(conn); err != nil {
			log.LogErrorf("ack master response: %s", err.Error())
		}
	}()
}
