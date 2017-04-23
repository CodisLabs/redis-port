// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/docopt/docopt-go"
)

var args struct {
	input    string
	output   string
	parallel int

	from   string
	passwd string
	auth   string
	target string
	extra  bool

	sockfile   string
	keyfile    string
	keypattern string
	filesize   int64

	shift time.Duration
	psync bool
	codis bool
}

const (
	ReaderBufferSize = bytesize.MB * 32
	WriterBufferSize = bytesize.MB * 8
)

func parseInt(s string, min, max int) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	if n >= min && n <= max {
		return n, nil
	}
	return 0, errors.Errorf("out of range [%d,%d], got %d", min, max, n)
}

const (
	MinDB = 0
	MaxDB = 1023
)

var acceptDB = func(db uint32) bool {
	return db >= MinDB && db <= MaxDB
}

func main() {
	usage := `
Usage:
	redis-port decode   [--ncpu=N]  [--parallel=M]  [--input=INPUT]  [--output=OUTPUT]
	redis-port restore  [--ncpu=N]  [--parallel=M]  [--input=INPUT]  [--faketime=FAKETIME] [--extra] [--filterdb=DB] --target=TARGET [--auth=AUTH] [--redis|--codis]
	redis-port sync     [--ncpu=N]  [--parallel=M]   --from=MASTER   [--password=PASSWORD] [--psync] [--filterdb=DB] --target=TARGET [--auth=AUTH] [--redis|--codis] [--sockfile=FILE [--filesize=SIZE]] [--keyfile=FILE] [--keypattern=string]
	redis-port dump     [--ncpu=N]  [--parallel=M]   --from=MASTER   [--password=PASSWORD] [--extra] [--output=OUTPUT]
	redis-port --version

Options:
	-n N, --ncpu=N                    Set runtime.GOMAXPROCS to N.
	-p M, --parallel=M                Set the number of parallel routines to M.
	-i INPUT, --input=INPUT           Set input file, default is stdin ('/dev/stdin').
	-o OUTPUT, --output=OUTPUT        Set output file, default is stdout ('/dev/stdout').
	-f MASTER, --from=MASTER          Set host:port of master redis.
	-t TARGET, --target=TARGET        Set host:port of slave redis.
	-P PASSWORD, --password=PASSWORD  Set redis auth password.
	-A AUTH, --auth=AUTH              Set auth password for target.
	--faketime=FAKETIME               Set current system time to adjust key's expire time.
	--sockfile=FILE                   Use FILE to as socket buffer, default is disabled.
	--keyfile=FILE                    Get the list of keys for the specified migration from the file, the file format is one key per line.
	--keypattern=PATTERN              Key Pattern.
	--filesize=SIZE                   Set FILE size, default value is 1gb.
	-e, --extra                       Set true to send/receive following redis commands, default is false.
	--redis                           Target is normal redis instance, default is false.
	--codis                           Target is codis proxy, default is true.
	--filterdb=DB                     Filter db = DB, default is *.
	--psync                           Use PSYNC command.
`
	d, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		log.PanicError(err, "parse arguments failed")
	}

	switch {
	case d["--version"].(bool):
		fmt.Println("version:", Version)
		fmt.Println("compile:", Compile)
		return
	}

	if s, ok := d["--ncpu"].(string); ok && s != "" {
		n, err := parseInt(s, 1, 1024)
		if err != nil {
			log.PanicErrorf(err, "parse --ncpu failed")
		}
		runtime.GOMAXPROCS(n)
	}
	ncpu := runtime.GOMAXPROCS(0)

	if s, ok := d["--parallel"].(string); ok && s != "" {
		n, err := parseInt(s, 1, 1024)
		if err != nil {
			log.PanicErrorf(err, "parse --parallel failed")
		}
		args.parallel = n
	}
	if ncpu > args.parallel {
		args.parallel = ncpu
	}
	if args.parallel == 0 {
		args.parallel = 4
	}

	args.input, _ = d["--input"].(string)
	args.output, _ = d["--output"].(string)

	args.from, _ = d["--from"].(string)
	args.passwd, _ = d["--password"].(string)
	args.auth, _ = d["--auth"].(string)
	args.target, _ = d["--target"].(string)

	args.sockfile, _ = d["--sockfile"].(string)
	args.keyfile, _ = d["--keyfile"].(string)
	args.keypattern, _ = d["--keypattern"].(string)

	args.extra = d["--extra"].(bool)
	args.psync = d["--psync"].(bool)
	args.codis = d["--codis"].(bool) || !d["--redis"].(bool)

	if s, ok := d["--faketime"].(string); ok && s != "" {
		switch s[0] {
		case '-', '+':
			d, err := time.ParseDuration(strings.ToLower(s))
			if err != nil {
				log.PanicError(err, "parse --faketime failed")
			}
			args.shift = d
		case '@':
			n, err := strconv.ParseInt(s[1:], 10, 64)
			if err != nil {
				log.PanicError(err, "parse --faketime failed")
			}
			args.shift = time.Duration(n*int64(time.Millisecond) - time.Now().UnixNano())
		default:
			t, err := time.Parse("2006-01-02 15:04:05", s)
			if err != nil {
				log.PanicError(err, "parse --faketime failed")
			}
			args.shift = time.Duration(t.UnixNano() - time.Now().UnixNano())
		}
	}

	if s, ok := d["--filterdb"].(string); ok && s != "" && s != "*" {
		n, err := parseInt(s, MinDB, MaxDB)
		if err != nil {
			log.PanicError(err, "parse --filterdb failed")
		}
		u := uint32(n)
		acceptDB = func(db uint32) bool {
			return db == u
		}
	}

	if s, ok := d["--filesize"].(string); ok && s != "" {
		if len(args.sockfile) == 0 {
			log.Panic("please specify --sockfile first")
		}
		n, err := bytesize.Parse(s)
		if err != nil {
			log.PanicError(err, "parse --filesize failed")
		}
		if n <= 0 {
			log.Panicf("parse --filesize = %d, invalid number", n)
		}
		args.filesize = n
	} else {
		args.filesize = bytesize.GB
	}

	log.Infof("set ncpu = %d, parallel = %d\n", ncpu, args.parallel)

	switch {
	case d["decode"].(bool):
		new(cmdDecode).Main()
	case d["restore"].(bool):
		new(cmdRestore).Main()
	case d["dump"].(bool):
		new(cmdDump).Main()
	case d["sync"].(bool):
		new(cmdSync).Main()
	}
}
