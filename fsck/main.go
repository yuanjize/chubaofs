package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/chubaofs/chubaofs/fsck/cmd"
)

var (
	checkOpt string
	cleanOpt string
)

func init() {
	flag.StringVar(&checkOpt, "check", "", "check and export obsolete inodes and dentries")
	flag.StringVar(&cleanOpt, "clean", "", "clean inodes or dentries")

	flag.Parse()
}

func main() {
	if checkOpt != "" {
		err := cmd.Check(checkOpt)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	if cleanOpt != "" && (cleanOpt == "inode" || cleanOpt == "dentry") {
		err := cmd.Clean(cleanOpt)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}
