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
	"path"
	"runtime"
	"strings"

	"github.com/chubaofs/cfs/third_party/fuse"
	"github.com/chubaofs/cfs/third_party/fuse/fs"

	bdfs "github.com/chubaofs/cfs/client/fs"
	"github.com/chubaofs/cfs/util/config"
	"github.com/chubaofs/cfs/util/log"
	"github.com/chubaofs/cfs/util/ump"
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
	Version = "0.01"
)

var (
	configFile    = flag.String("c", "", "FUSE client config file")
	configVersion = flag.Bool("v", false, "show version")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	ump.InitUmp(UmpModuleName)
	flag.Parse()

	if *configVersion {
		fmt.Printf("CFS client verson: %s\n", Version)
		os.Exit(0)
	}

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
	level := ParseLogLevel(loglvl)
	_, err := log.InitLog(path.Join(logpath, LoggerDir), LoggerPrefix, level)
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
