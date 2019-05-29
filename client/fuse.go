// Copyright 2018 The CFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

//
// Usage: ./client -c fuse.json &
//
// Default mountpoint is specified in fuse.json, which is "/mnt".
// Therefore operations to "/mnt" are routed to the baudstorage.
//

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strings"

	"github.com/chubaofs/chubaofs/third_party/fuse"
	"github.com/chubaofs/chubaofs/third_party/fuse/fs"

	bdfs "github.com/chubaofs/chubaofs/client/fs"
	"github.com/chubaofs/chubaofs/util/config"
	"github.com/chubaofs/chubaofs/util/log"
	"github.com/chubaofs/chubaofs/util/ump"
	"strconv"
)

const (
	MaxReadAhead = 512 * 1024
)

const (
	LoggerDir    = "client"
	LoggerPrefix = "client"

	UmpModuleName = "fuseclient"
)

var (
	CommitID   string
	BranchName string
	BuildTime  string
)

var (
	configFile    = flag.String("c", "", "FUSE client config file")
	configVersion = flag.Bool("v", false, "show version")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *configVersion {
		fmt.Printf("CFS Client\n")
		fmt.Printf("Branch: %s\n", BranchName)
		fmt.Printf("Commit: %s\n", CommitID)
		fmt.Printf("Build: %s %s %s %s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime)
		os.Exit(0)
	}

	ump.InitUmp(UmpModuleName)

	cfg := config.LoadConfigFile(*configFile)
	if err := Mount(cfg); err != nil {
		fmt.Println("Mount failed: ", err)
		os.Exit(1)
	}
	// Wait for the log to be flushed
	fmt.Println("Done!")
}

func Mount(cfg *config.Config) error {
	mnt := cfg.GetString("mountpoint")
	volname := cfg.GetString("volname")
	master := cfg.GetString("master")
	logpath := cfg.GetString("logpath")
	loglvl := cfg.GetString("loglvl")
	profport := cfg.GetString("profport")
	rdonly := cfg.GetBool("rdonly")
	icacheTimeout := ParseConfigString(cfg, "icacheTimeout")
	lookupValid := ParseConfigString(cfg, "lookupValid")
	attrValid := ParseConfigString(cfg, "attrValid")
	autoInvalData := ParseConfigString(cfg, "autoInvalData")
	level := ParseLogLevel(loglvl)
	_, err := log.InitLog(logpath, LoggerPrefix, level)
	if err != nil {
		return err
	}
	defer log.LogFlush()

	super, err := bdfs.NewSuper(volname, master, icacheTimeout, lookupValid, attrValid)
	if err != nil {
		return err
	}

	options := []fuse.MountOption{
		fuse.AllowOther(),
		fuse.MaxReadahead(MaxReadAhead),
		fuse.AsyncRead(),
		fuse.AutoInvalData(autoInvalData),
		fuse.FSName("cfs-" + volname),
		fuse.LocalVolume(),
		fuse.VolumeName("cfs-" + volname)}

	if rdonly {
		options = append(options, fuse.ReadOnly())
	}

	c, err := fuse.Mount(mnt, options...)
	if err != nil {
		return err
	}
	defer c.Close()
	go func() {
		fmt.Println(http.ListenAndServe(":"+profport, nil))
	}()

	if err = fs.Serve(c, super); err != nil {
		return err
	}

	<-c.Ready
	return c.MountError
}

func ParseConfigString(cfg *config.Config, keyword string) int64 {
	var ret int64 = -1
	rawstr := cfg.GetString(keyword)
	if rawstr != "" {
		val, err := strconv.Atoi(rawstr)
		if err == nil {
			ret = int64(val)
			fmt.Println(fmt.Sprintf("keyword[%v] value[%v]", keyword, ret))
		}
	}
	return ret
}

func ParseLogLevel(loglvl string) log.Level {
	var level log.Level
	switch strings.ToLower(loglvl) {
	case "debug":
		level = log.DebugLevel
	case "info":
		level = log.InfoLevel
	case "warn":
		level = log.WarnLevel
	case "error":
		level = log.ErrorLevel
	default:
		level = log.ErrorLevel
	}
	return level
}
