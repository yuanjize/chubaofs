package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/chubaofs/chubaofs/fsck/cmd"
)

var (
	checkOnly bool
	cleanOpt  string
)

func init() {
	flag.BoolVar(&checkOnly, "check", false, "check and export obselete inodes and dentries")
	flag.StringVar(&cleanOpt, "clean", "", "clean inodes or dentries")

	flag.Parse()
}

func main() {
	if checkOnly {
		err := cmd.Check()
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
