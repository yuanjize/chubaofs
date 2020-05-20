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
	var remote bool

	if InodesFile == "" || DensFile == "" {
		remote = true
	}

	if VolName == "" || (remote && MasterAddr == "") {
		flag.Usage()
		return
	}

	/*
	 * Record all the inodes and dentries retrieved from metanode
	 */
	var (
		ifile       *os.File
		dfile       *os.File
		ifileUpdate *os.File
	)

	dirPath := fmt.Sprintf("_export_%s", VolName)
	if err = os.MkdirAll(dirPath, 0666); err != nil {
		return
	}

	if remote {
		if ifile, err = os.Create(fmt.Sprintf("%s/%s", dirPath, inodeDumpFileName)); err != nil {
			return
		}
		defer ifile.Close()
		if dfile, err = os.Create(fmt.Sprintf("%s/%s", dirPath, dentryDumpFileName)); err != nil {
			return
		}
		defer dfile.Close()
		if ifileUpdate, err = os.Create(fmt.Sprintf("%s/%s", dirPath, inodeUpdateDumpFileName)); err != nil {
			return
		}
		defer ifileUpdate.Close()
		if err = importRawDataFromRemote(ifile, dfile, ifileUpdate); err != nil {
			return
		}
		// go back to the beginning of the files
		ifile.Seek(0, 0)
		dfile.Seek(0, 0)
		ifileUpdate.Seek(0, 0)
	} else {
		if ifile, err = os.Open(InodesFile); err != nil {
			return
		}
		defer ifile.Close()
		if dfile, err = os.Open(DensFile); err != nil {
			return
		}
		defer dfile.Close()
	}

	/*
	 * Perform analysis
	 */
	imap, dlist, err := analyze(ifile, dfile)
	if err != nil {
		return
	}

	/*
	 * Export obsolete inodes
	 */
	if err = dumpObsoleteInode(imap, fmt.Sprintf("%s/%s", dirPath, obsoleteInodeDumpFileName)); err != nil {
		return
	}

	if remote {
		/*
		 * To get safe obsolete dentry list, dentry dump should be
		 * performed ahead of the inode dump.
		 */
		if _, dlist, err = analyze(ifileUpdate, dfile); err != nil {
			return
		}
	}

	if err = dumpObsoleteDentry(dlist, fmt.Sprintf("%s/%s", dirPath, obsoleteDentryDumpFileName)); err != nil {
		return
	}

	return
}

func importRawDataFromRemote(ifile, dfile, ifileUpdate *os.File) error {
	/*
	 * Get all the meta partitions info
	 */
	vol, err := getVolumes(MasterAddr, VolName)
	if err != nil {
		return err
	}

	/*
	 * Note that if we are about to clean obsolete inodes,
	 * we should get all inodes before geting all dentries.
	 */
	for _, mp := range vol.MetaPartitions {
		cmdline := fmt.Sprintf("http://%s:9092/getInodeRange?id=%d", strings.Split(mp.LeaderAddr, ":")[0], mp.PartitionID)
		if err := exportToFile(ifile, cmdline); err != nil {
			return err
		}
	}

	for _, mp := range vol.MetaPartitions {
		cmdline := fmt.Sprintf("http://%s:9092/getDentry?pid=%d", strings.Split(mp.LeaderAddr, ":")[0], mp.PartitionID)
		if err = exportToFile(dfile, cmdline); err != nil {
			return err
		}
	}

	for _, mp := range vol.MetaPartitions {
		cmdline := fmt.Sprintf("http://%s:9092/getInodeRange?id=%d", strings.Split(mp.LeaderAddr, ":")[0], mp.PartitionID)
		if err := exportToFile(ifileUpdate, cmdline); err != nil {
			return err
		}
	}

	return nil
}

func analyze(ifile, dfile *os.File) (imap map[uint64]*Inode, dlist []*Dentry, err error) {
	imap = make(map[uint64]*Inode)
	dlist = make([]*Dentry, 0)

	/*
	 * Walk through all the inodes to establish inode index
	 */
	err = walkRawFile(ifile, func(data []byte) error {
		inode := &Inode{Dentries: make([]*Dentry, 0)}
		if e := json.Unmarshal(data, inode); e != nil {
			return e
		}
		imap[inode.Inode] = inode
		return nil
	})
	ifile.Seek(0, 0)

	if err != nil {
		return
	}

	/*
	 * Walk through all the dentries to establish inode relations.
	 */
	err = walkRawFile(dfile, func(data []byte) error {
		den := &Dentry{}
		if e := json.Unmarshal(data, den); e != nil {
			return e
		}
		inode, ok := imap[den.ParentId]
		if !ok {
			dlist = append(dlist, den)
		} else {
			inode.Dentries = append(inode.Dentries, den)
		}
		return nil
	})
	dfile.Seek(0, 0)

	if err != nil {
		return
	}

	root, ok := imap[1]
	if !ok {
		err = fmt.Errorf("No root inode")
		return
	}

	/*
	 * Iterate all the path, and mark reachable inode and dentry.
	 */
	followPath(imap, root)
	return
}

func followPath(imap map[uint64]*Inode, inode *Inode) {
	inode.Valid = true
	// there is no down path for file inode
	if inode.Type == 0 || len(inode.Dentries) == 0 {
		return
	}

	for _, den := range inode.Dentries {
		childInode, ok := imap[den.Inode]
		if !ok {
			continue
		}
		den.Valid = true
		followPath(imap, childInode)
	}
}

func dumpObsoleteInode(imap map[uint64]*Inode, name string) error {
	var (
		obsoleteTotalFileSize uint64
		totalFileSize         uint64
	)

	fp, err := os.Create(name)
	if err != nil {
		return err
	}
	defer fp.Close()

	for _, inode := range imap {
		if !inode.Valid {
			if _, err = fp.WriteString(inode.String() + "\n"); err != nil {
				return err
			}
			obsoleteTotalFileSize += inode.Size
		}
		totalFileSize += inode.Size
	}

	fmt.Printf("Total File Size: %v\nObselete Total File Size: %v\n", totalFileSize, obsoleteTotalFileSize)
	return nil
}

func dumpObsoleteDentry(dlist []*Dentry, name string) error {
	/*
	 * Note: if we get all the inodes raw data first, then obsolete
	 * dentries are not trustable.
	 */
	fp, err := os.Create(name)
	if err != nil {
		return err
	}
	defer fp.Close()

	for _, den := range dlist {
		if _, err = fp.WriteString(den.String() + "\n"); err != nil {
			return err
		}
	}
	return nil
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
