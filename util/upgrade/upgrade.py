import os
import logging
import subprocess
import commands
import logging
import sys
import urllib
import time
import argparse
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
rootPath="/export/App/chubaoio"
remoteUrl=""





def getPid(process):
    cmd = "ps aux | grep -v grep| grep '%s' " % process
    info = commands.getoutput(cmd)
    infos = info.split()
    logging.info(cmd)
    if len(infos) >1:
        return infos[1]
    else:
        return -1

def restartMetanode():
    metaExec='{}/chubaometa/meta'.format(rootPath)
    while True:
        try:
            if not os.path.exists(metaExec):
                break
            else:
                logging.info("wait rm {} ".format(metaExec))
                time.sleep(1)
                os.remove(metaExec)
        except BaseException,eb:
            logging.warn("check {} has delete failed {}".format(metaExec,eb))
            time.sleep(1)

    wgetMetaShell="wget {} -O {}".format(remoteUrl,metaExec)
    subprocess.call(wgetMetaShell,shell=True)
    logging.info(wgetMetaShell)

    subprocess.call("chmod +x {}".format(metaExec),shell=True)


    metaPid=getPid(metaExec)
    if metaPid==-1:
        logging.warn("not find metanode process")
        # return
    killShell="kill {}".format(metaPid)
    logging.info(killShell)
    subprocess.call(killShell,shell=True)

    while True:
        try:
            if getPid(metaExec)==-1:
                break
            else:
                logging.info("wait metanode has killed")
                subprocess.call(killShell, shell=True)
                time.sleep(1)
        except BaseException,eb:
            logging.warn("check metanode has killed failed {}".format(eb))
            time.sleep(1)

    startShell='bash {}/chubaometa/start.sh'.format(rootPath)
    logging.info(startShell)
    subprocess.call(startShell,shell=True)

    while True:
        try:
            url = "http://127.0.0.1:9092/getAllPartitions"
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                logging.warn("start metanode success")
                return
            else:
                time.sleep(1)
        except BaseException,eb:
            logging.warn("check metanode is failed {}".format(eb))
            time.sleep(1)
            if getPid(metaExec)==-1:
                logging.info(startShell)
                subprocess.call(startShell, shell=True)
                time.sleep(1)




def restartDatanode():
    dataExec='{}/chubaodatanode/datanode'.format(rootPath)
    while True:
        try:
            if not os.path.exists(dataExec):
                break
            else:
                logging.info("wait rm {} ".format(dataExec))
                time.sleep(1)
                os.remove(dataExec)
        except BaseException,eb:
            logging.warn("check {} has delete success {}".format(dataExec,eb))
            time.sleep(1)


    wgetDataNodeShell="wget {} -O {}".format(remoteUrl,dataExec)
    subprocess.call(wgetDataNodeShell,shell=True)
    logging.info(wgetDataNodeShell)

    subprocess.call("chmod +x {}".format(dataExec),shell=True)


    dataNodePid=getPid(dataExec)
    if dataNodePid==-1:
        logging.warn("not find datanode process")
        # return
    killShell="kill {}".format(dataNodePid)
    logging.info(killShell)
    subprocess.call(killShell,shell=True)

    while True:
        try:
            if getPid(dataExec)==-1:
                break
            else:
                logging.info("wait datanode has killed")
                time.sleep(1)
                subprocess.call(killShell, shell=True)
        except BaseException,eb:
            logging.warn("check datanode has killed failed {}".format(eb))
            time.sleep(1)



    startShell='bash {}/chubaodatanode/start.sh'.format(rootPath)
    logging.info(startShell)
    subprocess.call(startShell,shell=True)

    while True:
        try:
            url = "http://127.0.0.1:6001/disks"
            response = urllib.urlopen(url)
            if response.getcode() == 200:
                logging.warn("start datanode success")
                return
        except BaseException,eb:
            logging.warn("check datanode is failed {}".format(eb))
            time.sleep(1)
            if getPid(dataExec)==-1:
                logging.info(startShell)
                subprocess.call(startShell, shell=True)
                time.sleep(1)


if __name__ == '__main__':
    logging.info("nihao")
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument('--role', type=str, default="datanode")
    parser.add_argument('--path', type=str, default="http://storage.jd.local/dpgimage/cfsdbbak/cmd")
    args = parser.parse_args()
    role=args.role
    remoteUrl=args.path
    print remoteUrl
    print role
    if role=='all':
        for name in os.listdir(rootPath):
            if name == "chubaodatanode":
                restartDatanode()
            if name == "chubaometa":
                restartMetanode()
    else:
        for name in os.listdir(rootPath):
            if name == role and role=='chubaodatanode':
                restartDatanode()
            if name == role and role=='chubaometa':
                restartMetanode()





