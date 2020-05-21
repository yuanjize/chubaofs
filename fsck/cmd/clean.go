package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/chubaofs/chubaofs/sdk/meta"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
)

var gMetaWrapper *meta.MetaWrapper

func Clean(opt string) error {
	if MasterAddr == "" || VolName == "" {
		flag.Usage()
		return fmt.Errorf("Lack of parameters: master(%v) vol(%v)", MasterAddr, VolName)
	}

	ump.InitUmp("fsck")

	_, err := log.InitLog("fscklog", "fsck", log.DebugLevel)
	if err != nil {
		return fmt.Errorf("Init log failed: %v", err)
	}

	gMetaWrapper, err = meta.NewMetaWrapper(VolName, MasterAddr)
	if err != nil {
		return fmt.Errorf("NewMetaWrapper failed: %v", err)
	}

	switch opt {
	case "inode":
		err = cleanInodes()
		if err != nil {
			return fmt.Errorf("Clean inodes failed: %v", err)
		}
	case "dentry":
		err = cleanDentries()
		if err != nil {
			return fmt.Errorf("Clean dentries failed: %v", err)
		}
	default:
	}

	return nil
}

func cleanInodes() error {
	filePath := fmt.Sprintf("_export_%s/%s", VolName, obsoleteInodeDumpFileName)

	fp, err := os.Open(filePath)
	if err != nil {
		return err
	}

	err = walkRawFile(fp, func(data []byte) error {
		inode := &Inode{}
		if e := json.Unmarshal(data, inode); e != nil {
			return e
		}
		/*
		 * Do clean inode with the following exceptions:
		 * 1. nlink == 0, might be a temorary inode
		 * 2. size == 0 && ctime is close to current time, might be in the process of file creation
		 */
		if inode.NLink > 0 &&
			(inode.Size != 0 ||
				time.Unix(inode.CreateTime, 0).Add(24*time.Hour).Before(time.Now())) {
			// ignore errors
			doCleanInode(inode)
		}
		return nil
	})

	return nil
}

func doCleanInode(inode *Inode) error {
	err := gMetaWrapper.Unlink_ll(inode.Inode)
	if err != nil {
		if err != syscall.ENOENT {
			return err
		}
		err = nil
	}

	err = gMetaWrapper.Evict(inode.Inode)
	if err != nil {
		if err != syscall.ENOENT {
			return err
		}
	}

	return nil
}

func cleanDentries() error {
	//filePath := fmt.Sprintf("_export_%s/%s", VolName, obsoleteDentryDumpFileName)
	// TODO: send request to meta node directly with pino, name and ino.
	return nil
}
