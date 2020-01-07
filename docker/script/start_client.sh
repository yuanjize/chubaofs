#!/bin/bash

MntPoint=/cfs/mnt
mkdir -p /cfs/bin /cfs/log /cfs/mnt
src_path=/go/src/github.com/chubaofs/cfs

Master1Addr="192.168.0.11:17010"
LeaderAddr=""
VolName="ltptest"
TryTimes=5
Token=""

getToken() {
    echo -n "get token "
    for i in $(seq 1 100) ; do
        Token=$(curl -s "http://$Master1Addr/admin/getVol?name=ltptest" | jq . | grep -A 1 'TokenType": 2' | grep "Value" | sed 's/.*"Value": "\(.*\)".*/\1/g')
        if [ "x$Token" != "x" ] ; then
            break
        fi
        echo -n "."
        sleep 1
    done
    if [ "$Token" == "x" ] ; then
        echo -n "timeout, exit\n"
        exit 1
    fi
    echo "ok"
}

getLeaderAddr() {
    echo -n "check Master "
    for i in $(seq 1 300) ; do
        LeaderAddr=$(curl -s "http://$Master1Addr/admin/getCluster" | jq '.LeaderAddr' | tr -d '"' )
        if [ "x$LeaderAddr" != "x" ] ; then
            break
        fi
        echo -n "."
        sleep 1
    done
    if [ "x$LeaderAddr" == "x" ] ; then
        echo -n "timeout, exit\n"
        exit 1
    fi
    echo "ok"
}

check_status() {
    node="$1"
    up=0
    echo -n "check $node "
    for i in $(seq 1 300) ; do
        clusterInfo=$(curl -s "http://$LeaderAddr/admin/getCluster")
        #NodeUpCount=$( echo "$clusterInfo" | jq ".data.${node}s | .[].Status" | grep "true" | wc -l)
        NodeTotoalGB=$( echo "$clusterInfo" | jq ".${node}Stat.TotalGB" )
        if [[ $NodeTotoalGB -gt 0 ]]  ; then
            up=1
            break
        fi
        echo -n "."
        sleep 1
    done
    if [ $up -eq 0 ] ; then
        echo -n "timeout, exit"
        curl "http://$LeaderAddr/admin/getCluster" | jq
        exit 1
    fi
    echo "ok"
}

create_vol() {
    clusterInfo=$(curl -s "http://$LeaderAddr/admin/getCluster")
    volname=$(echo "$clusterInfo" | jq ".VolStat[0].Name" | tr -d \")
    if [[ "-$volname" == "-$VolName" ]] ; then
        echo "vol ok"
        return
    fi
    echo -n "create vol "
    res=$(curl -s "http://$LeaderAddr/admin/createVol?name=$VolName&capacity=30&owner=ltptest&replicas=3&type=extent")
    code=$(echo "$res" | grep -q "success" ; echo $?)
    if [[ $code -ne 0 ]] ; then
        echo "failed, exit"
        curl -s "http://$LeaderAddr/admin/getCluster" | jq
        exit 1
    fi
    echo "ok"
}

print_error_info() {
    echo "------ err ----"
    cat /cfs/log/cfs.out
    cat /cfs/log/client/client_info.log
    cat /cfs/log/client/client_error.log
    curl -s "http://$LeaderAddr/admin/getCluster" | jq
    mount
    df -h
    stat $MntPoint
    ls -l $MntPoint
    ls -l $LTPTestDir
}

start_client() {
    echo -n "start client "
    # replace client.json token
    cat /cfs/conf/client.json |  sed -e 's/"token": ".*"/"token": "'$Token'"/' > /cfs/conf/client2.json
    for((i=0; i<$TryTimes; i++)) ; do
        nohup /cfs/bin/cfs-client -c /cfs/conf/client2.json >/cfs/log/cfs.out 2>&1 &
        sleep 4
        sta=$(stat $MntPoint 2>/dev/null | tr ":：" " "  | awk '/Inode/{print $4}')
        if [[ "x$sta" == "x1" ]] ; then
            ok=1
	        echo "ok"
            exit 0
        fi
		ps -ef | grep cfs-client | grep -v "grep" | awk '{print $2}' | xargs -n1 kill 
    done
    echo "failed"
    exit 1
}

getLeaderAddr
check_status "MetaNode"
check_status "DataNode"
create_vol
getToken
start_client

