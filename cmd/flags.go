package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/log"

	docopt "github.com/docopt/docopt-go"
)

const (
	ReaderBufferSize = 1024 * 128
	WriterBufferSize = 1024 * 128
)

type Flags struct {
	Source, Target string

	Parallel int

	AofPath string
	TmpFile struct {
		Path string
		Size int64
	}
	ExpireOffset time.Duration
}

var acceptDB = func(db uint64) bool {
	return true
}

func parseFlags(usage string) *Flags {
	return parseFlagsFromArgs(usage, os.Args[1:])
}

func parseFlagsFromArgs(usage string, args []string) *Flags {
	d, err := docopt.Parse(usage, args, true, "", false)
	if err != nil {
		log.PanicErrorf(err, "parse arguments failed")
	}
	switch {
	case d["--version"].(bool):
		fmt.Println("version:", Version)
		fmt.Println("compile:", Compile)
		os.Exit(0)
	}

	if s, ok := d["--ncpu"].(string); ok && s != "" {
		n, err := strconv.Atoi(s)
		if err != nil {
			log.PanicErrorf(err, "parse --ncpu=%q failed", s)
		}
		if n <= 0 || n > 1024 {
			log.Panicf("parse --ncpu=%q failed, invalid", s)
		}
		runtime.GOMAXPROCS(n)
	} else {
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	var ncpu = runtime.GOMAXPROCS(0)

	var flags Flags
	switch {
	case ncpu <= 1:
		flags.Parallel = 8
	case ncpu <= 8:
		flags.Parallel = 4 * ncpu
	default:
		flags.Parallel = 2 * ncpu
	}
	for _, key := range []string{"INPUT", "--input", "MASTER", "--master"} {
		if s, ok := d[key].(string); ok && s != "" {
			flags.Source = s
		}
	}
	for _, key := range []string{"--output", "--target"} {
		if s, ok := d[key].(string); ok && s != "" {
			flags.Target = s
		}
	}

	if s, ok := d["--aof"].(string); ok && s != "" {
		flags.AofPath = s
	}

	if s, ok := d["--unixtime-in-milliseconds"].(string); ok && s != "" {
		switch s[0] {
		case '-', '+':
			d, err := time.ParseDuration(strings.ToLower(s))
			if err != nil {
				log.PanicErrorf(err, "parse --unixtime-in-milliseconds=%q failed", s)
			}
			flags.ExpireOffset = d
		case '@':
			n, err := strconv.ParseInt(s[1:], 10, 64)
			if err != nil {
				log.PanicErrorf(err, "parse --unixtime-in-milliseconds=%q failed", s)
			}
			flags.ExpireOffset = time.Since(time.Unix(n/1000, n%1000))
		default:
			t, err := time.Parse("2006-01-02 15:04:05", s)
			if err != nil {
				log.PanicErrorf(err, "parse --unixtime-in-milliseconds=%q failed", s)
			}
			flags.ExpireOffset = time.Since(t)
		}
	}

	if s, ok := d["--db"].(string); ok && s != "" && s != "*" {
		n, err := strconv.Atoi(s)
		if err != nil {
			log.PanicErrorf(err, "parse --db=%q failed", s)
		}
		if n < 0 {
			log.Panicf("parse --db=%q failed", s)
		}
		acceptDB = func(db uint64) bool {
			return db == uint64(n)
		}
	}

	if s, ok := d["--tmpfile"].(string); ok {
		flags.TmpFile.Path = s
	}
	if s, ok := d["--tmpfile-size"].(string); ok && s != "" {
		n, err := bytesize.Parse(s)
		if err != nil {
			log.PanicErrorf(err, "parse --tmpfile-size=%q failed", s)
		}
		if n <= 0 {
			log.PanicErrorf(err, "parse --tmpfile-size=%q failed", s)
		}
		flags.TmpFile.Size = n
	} else if flags.TmpFile.Path != "" {
		flags.TmpFile.Size = bytesize.GB * 2
	}
	return &flags
}
