#!/usr/bin/python
# -*- coding: UTF-8 -*- 

import requests
import os
import time
import json
vols=[]
f=open("/home/guowl/2.log","r")
data=f.read()
f.close()

data=data.split('\n')
for d in data:
    if str(d).isdigit():
        vols.append(int(d))
    
url='http://dbbak.jd.local/dataPartition/get?id={}'
for v in vols:
    geturl=url.format(v)
    resp=requests.get(geturl)
    respdict=json.loads(resp.text)
    # if not respdict.has_key('VolName'):
    #     continue
    name=respdict["VolName"]
    if name=="":
        print ('{} failed get'.format(v))
    offlineUrl='http://dbbak.jd.local/dataPartition/offline?id={}&addr={}&name={}'.format(v,"11.3.36.170:6000",name)
    print(offlineUrl)
    resp=requests.get(offlineUrl)
    print (resp.text)
    resp=requests.get(geturl)
    getResp=str(resp.text)
    while True:
        if getResp.find("11.3.36.170:6000")==-1:
            break
        else:
            time.sleep(1)
    




