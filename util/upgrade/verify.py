import os
import json
import urllib2
import time
import argparse

rootPath="/export/cfsd1"

domain="10.0.7.15:8868"

def requestUrl(url):
    try:
        response = urllib2.urlopen(url,timeout=10)
        data = response.read()
        return data
    except BaseException,eb:
        mesg='request {} expection {}'.format(url,eb)
        print mesg
        return mesg


def getMaxPartitionId():
    url='http://{}/admin/getCluster'.format(domain)
    response=requestUrl(url)
    if response.find(url)!=-1:
        return 0
    result=json.loads(response)
    maxPartitionId=result['MaxDataPartitionID']
    return maxPartitionId


def checkPartition(partitionId):
    getUrl = "http://{}/dataPartition/get?id={}".format(domain,partitionId)
    response = requestUrl(getUrl)
    if response.find(getUrl)!=-1:
        return
    result = json.loads(response)
    volname = result['VolName']
    loadUrl = "http://{}/dataPartition/load?id={}&name={}".format(domain,partitionId, volname)
    requestUrl(loadUrl)
    while True:
        try:
            time.sleep(1)
            getUrl="http://{}/dataPartition/get?id={}".format(domain,partitionId)
            response=requestUrl(getUrl)
            result=json.loads(response)
            fileIncoreMap=result['FileInCoreMap']
            print "start check datapartition {} file count {}".format(partitionId,len(fileIncoreMap))
            if len(fileIncoreMap)<128:
                time.sleep(5)
                continue
            successCheck=0
            for k,v in fileIncoreMap.iteritems():
                metas=v['Metas']
                crc=metas[0]['Crc']
                size=metas[0]['Size']
                verifyFail=False
                for index, item in enumerate(metas):
                    if index==0:
                        continue
                    if crc!=item['Crc'] or size!=item['Size']:
                        print(item)
                        verifyFail=True
                if not verifyFail:
                       successCheck=successCheck+1
                print('datapartition {} file {} check result {}'.format(partitionId,k,verifyFail==False))
            print "end check datapartition {} file count {} successCheck {} ".format(partitionId,len(fileIncoreMap),successCheck)
            return
        except BaseException,eb:
            print eb
            time.sleep(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument('--domain', type=str, default="dbbak.jd.local")
    args = parser.parse_args()
    domain=args.domain
    print domain
    maxPartitionId=getMaxPartitionId()
    for partitionId in range(maxPartitionId):
        checkPartition(partitionId)