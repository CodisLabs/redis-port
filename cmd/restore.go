package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/bufio2"
	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"

	"github.com/CodisLabs/redis-port/pkg/rdb"
)

func main() {
	const usage = `
Usage:
	redis-restore [--ncpu=N] [--input=INPUT|INPUT] [--target=TARGET] [--aof=FILE] [--db=DB] [--unixtime-in-milliseconds=EXPR]
	redis-restore  --version

Options:
	-n N, --ncpu=N                    Set runtime.GOMAXPROCS to N.
	-i INPUT, --input=INPUT           Set input file, default is '/dev/stdin'.
	-t TARGET, --target=TARGET        The target redis instance ([auth@]host:port).
	-a FILE, --aof=FILE               Also restore the replication backlog.
	--faketime=FAKETIME               Set current system time to adjust key's expire time.
	--db=DB                           Accept db = DB, default is *.
	--unixtime-in-milliseconds=EXPR   Update expire time when restoring objects from RDB.

Examples:
	$ redis-restore    dump.rdb -t 127.0.0.1:6379
	$ redis-restore -i dump.rdb -t 127.0.0.1:6379 --aof dump.aof
	$ redis-restore -i dump.rdb -t 127.0.0.1:6379 --db=0
	$ redis-restore -i dump.rdb -t 127.0.0.1:6379 --unixtime-in-milliseconds="@209059200000"       // ttlms += (now - '1976-08-17')
	$ redis-restore -i dump.rdb -t 127.0.0.1:6379 --unixtime-in-milliseconds="+1000"               // ttlms += 1s
	$ redis-restore -i dump.rdb -t 127.0.0.1:6379 --unixtime-in-milliseconds="-1000"               // ttlms -= 1s
	$ redis-restore -i dump.rdb -t 127.0.0.1:6379 --unixtime-in-milliseconds="1976-08-17 00:00:00" // ttlms += (now - '1976-08-17')
`
	var flags = parseFlags(usage)

	var input, aoflog struct {
		Path string
		Size int64
		io.Reader
		rd *bufio2.Reader

		rbytes atomic2.Int64

		forward, skip atomic2.Int64
	}
	if len(flags.Source) != 0 {
		input.Path = flags.Source
	} else {
		input.Path = "/dev/stdin"
	}
	if len(flags.AofPath) != 0 {
		aoflog.Path = flags.AofPath
	}

	var target struct {
		Path       string
		Addr, Auth string
	}
	target.Path = flags.Target
	if len(target.Path) == 0 {
		log.Panicf("invalid target address")
	}
	target.Addr, target.Auth = redisParsePath(target.Path)
	if len(target.Addr) == 0 {
		log.Panicf("invalid master address")
	}
	log.Infof("restore: input = %q, aoflog = %q target = %q\n", input.Path, aoflog.Path, target.Path)

	if input.Path != "/dev/stdin" {
		file, size := openReadFile(input.Path)
		defer file.Close()
		input.Reader, input.Size = file, size
	} else {
		input.Reader = os.Stdin
	}
	input.rd = rBuilder(input.Reader).Must().
		Count(&input.rbytes).Buffer2(ReaderBufferSize).Reader.(*bufio2.Reader)

	if aoflog.Path != "" {
		file, size := openReadFile(aoflog.Path)
		defer file.Close()
		aoflog.Reader, aoflog.Size = file, size
	} else {
		aoflog.Reader = bytes.NewReader(nil)
	}
	aoflog.rd = rBuilder(aoflog.Reader).Must().
		Count(&aoflog.rbytes).Buffer2(ReaderBufferSize).Reader.(*bufio2.Reader)

	var entryChan = newRDBLoader(input.rd, 32)

	var jobs = NewParallelJob(flags.Parallel, func() {
		doRestoreDBEntry(entryChan, target.Addr, target.Auth,
			func(e *rdb.DBEntry) bool {
				if e.Expire != rdb.NoExpire {
					e.Expire += flags.ExpireOffset
				}
				if !acceptDB(e.DB) {
					input.skip.Incr()
					return false
				}
				input.forward.Incr()
				return true
			})
	}).Then(func() {
		if aoflog.Path == "" {
			return
		}
		doRestoreAoflog(aoflog.rd, target.Addr, target.Auth,
			func(db uint64, cmd string) bool {
				if !acceptDB(db) && cmd != "PING" {
					aoflog.skip.Incr()
					return false
				}
				aoflog.forward.Incr()
				return true
			})
	}).Run()

	log.Infof("restore: (r,f,s/a,f,s) = (rdb,rdb.forward,rdb.skip/aof,rdb.forward,rdb.skip)")

	NewJob(func() {
		for stop := false; !stop; {
			select {
			case <-jobs:
				stop = true
			case <-time.After(time.Second):
			}
			stats := &struct {
				input, aoflog int64
			}{
				input.rbytes.Int64(), aoflog.rbytes.Int64(),
			}

			var b bytes.Buffer
			var percent float64
			if input.Size != 0 {
				percent = float64(stats.input) * 100 / float64(input.Size)
			}
			if input.Size >= stats.input {
				fmt.Fprintf(&b, "restore: rdb = %d - [%6.2f%%]", input.Size, percent)
			} else {
				fmt.Fprintf(&b, "restore: rdb = %d", input.Size)
			}
			fmt.Fprintf(&b, "   (r,f,s/a,f,s)=%s",
				formatAlign(4, "(%d,%d,%d/%d,%d,%d)", stats.input, input.forward.Int64(), input.skip.Int64(),
					stats.aoflog, aoflog.forward.Int64(), aoflog.skip.Int64()))
			fmt.Fprintf(&b, "  ~  (%s,-,-/%s,-,-)",
				bytesize.Int64(stats.input).HumanString(), bytesize.Int64(stats.aoflog).HumanString())
			log.Info(b.String())
		}
	}).RunAndWait()

	log.Info("restore: done")
}
