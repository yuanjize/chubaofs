package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	extentPath = flag.String("extent", "/tmp/1", "default extent path")
	offset     = flag.Int64("offset", 0, "default start offset")
	size       = flag.Int64("size", 0, "default size")
)

const (
	BlockSize = 131072
	SEEK_DATA = 3
	SEEK_HOLE = 4
)

func tinyExtentAvaliOffset(fp *os.File, offset int64) (newOffset, newEnd int64, err error) {
	newOffset, err = fp.Seek(int64(offset), SEEK_DATA)
	if err != nil {
		return
	}
	newEnd, err = fp.Seek(int64(newOffset), SEEK_HOLE)
	if err != nil {
		return
	}
	if newOffset-offset > BlockSize {
		newOffset = offset + BlockSize
	}
	if newEnd-newOffset > BlockSize {
		newEnd = newOffset + BlockSize
	}
	if newEnd < newOffset {
		err = fmt.Errorf("unavali TinyExtentAvaliOffset on SEEK_DATA or SEEK_HOLE   (%v) offset(%v) "+
			"newEnd(%v) newOffset(%v)", fp.Name(), offset, newEnd, newOffset)
	}

	return
}

func main() {
	flag.Parse()
	fp, err := os.Open(*extentPath)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	index := 0
	for {
		if *offset >= *size {
			fmt.Println("has read end")
			break
		}
		newOffset, newEnd, err := tinyExtentAvaliOffset(fp, *offset)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		index++
		if newOffset > *offset {
			fmt.Println(fmt.Sprintf("index(%v) newOffset(%v) newEnd(%v) offset(%v) emptyPacket(%v)", index, newOffset, newEnd, *offset, true))
			*offset = *offset + (newOffset - *offset)
			continue
		}
		fmt.Println(fmt.Sprintf("index(%v) newOffset(%v) newEnd(%v) offset(%v)", index, newOffset, newEnd, *offset))
		*offset = *offset + (newEnd - newOffset)
	}

}
