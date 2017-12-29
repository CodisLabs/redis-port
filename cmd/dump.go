package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/bufio2"
	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

func main() {
	const usage = `
Usage:
	redis-dump [--ncpu=N] --master=MASTER|MASTER [--output=OUTPUT] [--backlog]
	redis-dump  --version

Options:
	-n N, --ncpu=N                    Set runtime.GOMAXPROCS to N.
	-m MASTER, --master=MASTER        The master redis instance ([auth@]host:port).
	-o OUTPUT, --output=OUTPUT        Set output file, default is '/dev/stdout'.
	-a, --backlog                     Also dump the replication backlog.

Examples:
	$ redis-dump    127.0.0.1:6379 -o dump.rdb
	$ redis-dump    127.0.0.1:6379 -o dump.rdb -a
	$ redis-dump -m passwd@192.168.0.1:6380 -o dump.rdb --backlog
`
	var flags = parseFlags(usage)

	var master struct {
		Path       string
		Addr, Auth string
		net.Conn
		rd *bufio2.Reader
		wt *bufio2.Writer
	}
	master.Path = flags.Source
	master.Addr, master.Auth = redisParsePath(flags.Source)
	if len(master.Addr) == 0 {
		log.Panicf("invalid master address")
	}

	var output struct {
		Path string
		io.Writer
	}
	if len(flags.Target) != 0 {
		output.Path = flags.Target
	} else {
		output.Path = "/dev/stdout"
	}
	log.Infof("dump: master = %q, output = %q\n", master.Path, output.Path)

	var wbytes atomic2.Int64

	master.Conn = openConn(master.Addr, master.Auth)
	defer master.Close()
	master.rd = rBuilder(master.Conn).Must().
		Buffer2(ReaderBufferSize).Reader.(*bufio2.Reader)
	master.wt = wBuilder(master.Conn).Must().
		Buffer2(WriterBufferSize).Writer.(*bufio2.Writer)

	if output.Path != "/dev/stdout" {
		file := openWriteFile(output.Path)
		defer closeFile(file)
		output.Writer = file
	} else {
		output.Writer = os.Stdout
	}
	var writer = wBuilder(output.Writer).Must().
		Count(&wbytes).Buffer(WriterBufferSize).Writer.(*bufio.Writer)

	var runid, offset, rdbSizeChan = redisSendPsyncFullsync(master.rd, master.wt)
	var rdbSize = func() int64 {
		for {
			select {
			case n := <-rdbSizeChan:
				if n != 0 {
					return n
				}
				log.Info("+")
			case <-time.After(time.Second):
				log.Info("-")
			}
		}
	}()
	log.Infof("dump: runid = %q, offset = %d", runid, offset)
	log.Infof("dump: rdb file = %d (%s)\n", rdbSize,
		bytesize.Int64(rdbSize).HumanString())

	var mu sync.Mutex

	var dumpoff atomic2.Int64
	var reploff = atomic2.Int64(offset)
	var encoder = redis.NewEncoderBuffer(master.wt)

	var jobs = NewJob(func() {
		var (
			rd = rBuilder(master.rd).Count(&dumpoff).Reader
			wt = wBuilder(writer).Mutex(&mu).Writer
		)
		ioCopyN(wt, rd, rdbSize)
	}).Then(func() {
		if !flags.Backlog {
			return
		}
		var (
			rd = rBuilder(master.rd).Count(&reploff).Reader
			wt = wBuilder(writer).Mutex(&mu).Writer
		)
		ioCopyBuffer(wt, rd)
	}).Run()

	var done = NewJob(func() {
		for stop := false; !stop; {
			select {
			case <-jobs:
				stop = true
			case <-time.After(time.Second):
				redisSendReplAck(encoder, reploff.Int64())
			}
			synchronized(&mu, func() {
				flushWriter(writer)
			})
		}
	}).Run()

	log.Infof("dump: (w/b) = (write/backlog)")

	NewJob(func() {
		for stop := false; !stop; {
			select {
			case <-done:
				stop = true
			case <-time.After(time.Second):
			}
			stats := &struct {
				dumpoff, reploff, wbytes int64
			}{
				dumpoff.Int64(),
				reploff.Int64(), wbytes.Int64(),
			}

			var b bytes.Buffer
			var percent float64
			if rdbSize != 0 {
				percent = float64(stats.dumpoff) * 100 / float64(rdbSize)
			}
			if rdbSize >= stats.wbytes {
				fmt.Fprintf(&b, "dump: rdb = %d - [%6.2f%%]", rdbSize, percent)
			} else {
				fmt.Fprintf(&b, "dump: rdb = %d", rdbSize)
			}
			var backlog = stats.reploff - offset
			fmt.Fprintf(&b, "   (w,b)=%s",
				formatAlign(4, "(%d,%d)", stats.wbytes, backlog))
			fmt.Fprintf(&b, "  ~  (%s,%s)",
				bytesize.Int64(stats.wbytes).HumanString(),
				bytesize.Int64(backlog).HumanString())
			log.Info(b.String())
		}
	}).RunAndWait()

	log.Info("dump: done")

}
