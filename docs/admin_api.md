# API
## Cluster
### overview
curl -v "http://127.0.0.1/admin/getCluster" | python -m json.tool
display the base information of the cluster, such as the detail of metaNode,dataNode,vol and so on.

### freeze
curl -v "http://127.0.0.1/cluster/freeze?enable=true"
if cluster is freezed,the vol never allocates dataPartitions automaticlly
|parameter | type | desc|
|---|---|---|
|enable|bool|if enable is true,the cluster is freezed

## MetaNode


### GET
curl -v "http://127.0.0.1/metaNode/get?addr=127.0.0.1:9021"  | python -m json.tool
show the base information of the metaNode,such as addr,total memory,used memory and so on.
|parameter | type | desc|
|---|---|---|
|addr|string| the addr which communicate with master

response
``` json
{
    "ID": 3,
    "Addr": "10.196.30.200:9021",
    "IsActive": true,
    "Sender": {
        "TaskMap": {}
    },
    "Rack": "",
    "MaxMemAvailWeight": 66556215048,
    "TotalWeight": 67132641280,
    "UsedWeight": 576426232,
    "Ratio": 0.008586377967698518,
    "SelectCount": 0,
    "Carry": 0.6645600532184904,
    "Threshold": 0.75,
    "ReportTime": "2018-12-05T17:26:28.29309577+08:00",
    "MetaPartitionCount": 1
}
```
### Offline
curl -v "http://127.0.0.1/metaNode/offline?addr=127.0.0.1:9021"
remove the metaNode from cluster, meta partitions which locate the metaNode will be migrate other available metaNode asynchronous
|parameter | type | desc|
|---|---|---|
|addr|string| the addr which communicate with master

### Threshold
curl -v "http://127.0.0.1/threshold/set?threshold=0.75"
the used memory percent arrives the threshold,the status of the meta partitions which locate the metaNode will be read only
|parameter | type | desc|
|---|---|---|
|threshold|float64| the max percent of memory which metaNode can use

## DataNode API
### GET
curl -v "http://127.0.0.1/dataNode/get?addr=127.0.0.1:5000"  | python -m json.tool
show the base information of the dataNode,such as addr,disk total size,disk used size and so on.
|parameter | type | desc|
|---|---|---|
|addr|string| the addr which communicate with master

### Offline
curl -v "http://127.0.0.1/dataNode/offline?addr=127.0.0.1:5000"
remove the dataNode from cluster, data partitions which locate the dataNode will be migrate other available dataNode asynchronous
|parameter | type | desc|
|---|---|---|
|addr|string| the addr which communicate with master

## Vol API
### Create
curl -v "http://127.0.0.1/admin/createVol?name=test&replicas=3&type=extent&randomWrite=true&capacity=100"
allocate data partition and meta partition to the user
|parameter | type | desc|
|---|---|---|
|name|string|
|replicas|int|the number replica of data partition and meta partition
|type|string|the type of data partition,now only support extent type
|randomWrite|bool| true is the file in the data partition can be modified
|capacity|int| the quota of vol,unit is GB

### Delete
curl -v "http://127.0.0.1/vol/delete?name=test"
|parameter | type | desc|
|---|---|---|
|name|string|

### Get
curl -v "http://127.0.0.1/client/vol?name=test" | python -m json.tool
|parameter | type | desc|
|---|---|---|
|name|string|
### Stat
curl -v http://127.0.0.1/client/volStat?name=test
|parameter | type | desc|
|---|---|---|
|name|string|

### Update
curl -v "http://127.0.0.1/vol/update?name=test&capacity=100"
|parameter | type | desc|
|---|---|---|
|name|string|
|capacity|int| the quota of vol,unit is GB

## MetaPartition API

### Parameter specification
  - **name**: the name of vol
  - **id**: the id of metaPartition
  - **addr**: the addr of metaNode, format is ip:port

### Get
 http://127.0.0.1/client/metaPartition?name=baudfs&id=1
### Offline one replica
 http://127.0.0.1/metaPartition/offline?name=baudfs&id=13&addr=ip:port

## DataPartition API

### Parameter specification
  - **name**: the name of vol
  - **id**: the id of dataPartition
  - **addr**: the addr of dataNode, format is ip:port
  - **count**： the total num of dataPartitions in the vol
  - **type**: store engine type

### Create
- http://127.0.0.1/dataPartition/create?count=40&name=baudfs&type=extent
### Get
- http://127.0.0.1/dataPartition/get?id=100
### Load
- http://127.0.0.1/dataPartition/load?name=baudfs&id=1
### Offline one replica
- http://127.0.0.1/dataPartition/offline?name=baudfs&id=13&addr=ip:port
### Get all dataPartitions of a vol
- http://127.0.0.1/client/dataPartitions?name=baudfs


## Master manage API

### Parameter specification
  - **addr**: the addr of master server, format is ip:port
  - **id**: the node id of master server

### Add
- http://127.0.0.1/raftNode/add?addr=ip:port&id=3

### Remove

- http://127.0.0.1/raftNode/remove?addr=ip:port&id=3