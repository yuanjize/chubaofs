package cmd

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func Check() (err error) {
	var getRawDataFromRemote bool

	if InodesFile == "" || DensFile == "" {
		getRawDataFromRemote = true
	}

	if VolName == "" || (getRawDataFromRemote && MasterAddr == "") {
		flag.Usage()
		return
	}

	/*
	 * Record all the inodes and dentries retrieved from metanode
	 */
	var (
		ifile *os.File
		dfile *os.File
	)

	dirPath := fmt.Sprintf("_export_%s", VolName)
	if err = os.MkdirAll(dirPath, 0666); err != nil {
		return
	}

	if getRawDataFromRemote {
		ifile, err = os.Create(fmt.Sprintf("%s/dump.inode", dirPath))
	} else {
		ifile, err = os.Open(InodesFile)
	}
	if err != nil {
		return
	}
	defer ifile.Close()

	if getRawDataFromRemote {
		dfile, err = os.Create(fmt.Sprintf("%s/dump.dentry", dirPath))
	} else {
		dfile, err = os.Open(DensFile)
	}
	if err != nil {
		return
	}
	defer dfile.Close()

	if getRawDataFromRemote {
		/*
		 * Get all the meta partitions info
		 */
		vol, e := getVolumes(MasterAddr, VolName)
		if e != nil {
			return e
		}

		/*
		 * Note that if we are about to clean obselete inodes,
		 * we should get all inodes before geting all dentries.
		 */
		for _, mp := range vol.MetaPartitions {
			cmdline := fmt.Sprintf("http://%s:9092/getInodeRange?id=%d", strings.Split(mp.LeaderAddr, ":")[0], mp.PartitionID)
			if e = exportToFile(ifile, cmdline); e != nil {
				return e
			}
		}

		for _, mp := range vol.MetaPartitions {
			cmdline := fmt.Sprintf("http://%s:9092/getDentry?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], mp.PartitionID)
			if e = exportToFile(dfile, cmdline); e != nil {
				return e
			}
		}
	}

	// go back to the beginning of the files
	ifile.Seek(0, 0)
	dfile.Seek(0, 0)

	inodeMap := make(map[uint64]*Inode)
	orphanDentry := make([]*Dentry, 0)

	/*
	 * Walk through all the inodes to establish inode index
	 */
	err = walkRawFile(ifile, func(data []byte) error {
		inode := &Inode{Dentries: make([]*Dentry, 0)}
		if err := json.Unmarshal(data, inode); err != nil {
			return err
		}
		inodeMap[inode.Inode] = inode
		return nil
	})

	if err != nil {
		return
	}

	/*
	 * Walk through all the dentries to establish inode relations.
	 */
	err = walkRawFile(dfile, func(data []byte) error {
		den := &Dentry{}
		if err := json.Unmarshal(data, den); err != nil {
			return err
		}
		inode, ok := inodeMap[den.ParentId]
		if !ok {
			orphanDentry = append(orphanDentry, den)
		} else {
			inode.Dentries = append(inode.Dentries, den)
		}
		return nil
	})

	if err != nil {
		return
	}

	root, ok := inodeMap[1]
	if !ok {
		err = fmt.Errorf("No root inode")
		return
	}

	/*
	 * Iterate all the path, and mark reachable inode and dentry.
	 */
	followPath(inodeMap, root)

	/*
	 * Export obselete inodes and dentries
	 */
	obseleteInodes, err := os.Create(fmt.Sprintf("%s/dump.inode.obselete", dirPath))
	if err != nil {
		return
	}

	/*
	 * Note: if we get all the inodes raw data first, then obselete
	 * dentries are not trustable.
	 */
	obseleteDentries, err := os.Create(fmt.Sprintf("%s/dump.dentry.obselete", dirPath))
	if err != nil {
		return
	}

	var obseleteTotalFileSize uint64
	var totalFileSize uint64

	for _, inode := range inodeMap {
		if !inode.Valid {
			if _, err = obseleteInodes.WriteString(inode.String() + "\n"); err != nil {
				return
			}
			obseleteTotalFileSize += inode.Size
		}
		totalFileSize += inode.Size

		for _, den := range inode.Dentries {
			if !den.Valid {
				if _, err = obseleteDentries.WriteString(den.String() + "\n"); err != nil {
					return
				}
			}
		}
	}

	for _, den := range orphanDentry {
		if _, err = obseleteDentries.WriteString(den.String() + "\n"); err != nil {
			return
		}
	}

	fmt.Printf("Total File Size: %v\nObselete Total File Size: %v\n", totalFileSize, obseleteTotalFileSize)
	return
}

func followPath(inodeMap map[uint64]*Inode, inode *Inode) {
	inode.Valid = true
	// there is no down path for file inode
	if inode.Type == 0 || len(inode.Dentries) == 0 {
		return
	}

	for _, den := range inode.Dentries {
		childInode, ok := inodeMap[den.Inode]
		if !ok {
			continue
		}
		den.Valid = true
		followPath(inodeMap, childInode)
	}
}

func getVolumes(addr, name string) (*VolInfo, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/client/vol?name=%s", addr, name))
	if err != nil {
		return nil, fmt.Errorf("Get volume info failed: %v", err)
	}
	defer resp.Body.Close()

	vol := &VolInfo{}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err = dec.Decode(vol); err != nil {
		return nil, fmt.Errorf("Unmarshal volume info failed: %v", err)
	}
	return vol, nil
}

func exportToFile(fp *os.File, cmdline string) error {
	resp, err := http.Get(cmdline)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("Invalid status code: %v", resp.StatusCode)
	}

	_, err = io.Copy(fp, resp.Body)
	return err
}
