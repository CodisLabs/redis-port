package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
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
	redis-dump [--ncpu=N] --master=MASTER|MASTER [--output=OUTPUT] [--aof=FILE]
	redis-dump  --version

Options:
	-n N, --ncpu=N                    Set runtime.GOMAXPROCS to N.
	-m MASTER, --master=MASTER        The master redis instance ([auth@]host:port).
	-o OUTPUT, --output=OUTPUT        Set output file, default is '/dev/stdout'.
	-a FILE, --aof=FILE               Also dump the replication backlog.

Examples:
	$ redis-dump    127.0.0.1:6379 -o dump.rdb
	$ redis-dump    127.0.0.1:6379 -o dump.rdb -a
	$ redis-dump -m passwd@192.168.0.1:6380 -o dump.rdb -a dump.aof
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
	if len(master.Path) == 0 {
		log.Panicf("invalid master address")
	}
	master.Addr, master.Auth = redisParsePath(master.Path)
	if len(master.Addr) == 0 {
		log.Panicf("invalid master address")
	}

	var output, aoflog struct {
		Path string
		io.Writer
		wt *bufio.Writer

		wbytes atomic2.Int64
	}
	if len(flags.Target) != 0 {
		output.Path = flags.Target
	} else {
		output.Path = "/dev/stdout"
	}

	if len(flags.AofPath) != 0 {
		aoflog.Path = flags.AofPath
	}
	log.Infof("dump: master = %q, output = %q, aoflog = %q\n", master.Path, output.Path, aoflog.Path)

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
	output.wt = wBuilder(output.Writer).Must().
		Count(&output.wbytes).Buffer(WriterBufferSize).Writer.(*bufio.Writer)

	if aoflog.Path != "" {
		file := openWriteFile(aoflog.Path)
		defer closeFile(file)
		aoflog.Writer = file
	} else {
		aoflog.Writer = ioutil.Discard
	}
	aoflog.wt = wBuilder(aoflog.Writer).Must().
		Count(&aoflog.wbytes).Buffer(WriterBufferSize).Writer.(*bufio.Writer)

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

	var encoder = redis.NewEncoderBuffer(master.wt)

	var jobs = NewJob(func() {
		var (
			rd = rBuilder(master.rd).Reader
			wt = wBuilder(output.wt).Mutex(&mu).Writer
		)
		ioCopyN(wt, rd, rdbSize)
	}).Then(func() {
		if aoflog.Path == "" {
			return
		}
		var (
			rd = rBuilder(master.rd).Reader
			wt = wBuilder(aoflog.wt).Mutex(&mu).Writer
		)
		ioCopyBuffer(wt, rd)
	}).Run()

	var done = NewJob(func() {
		for stop := false; !stop; {
			select {
			case <-jobs:
				stop = true
			case <-time.After(time.Second):
				redisSendReplAck(encoder, offset+aoflog.wbytes.Int64())
			}
			synchronized(&mu, func() {
				flushWriter(output.wt)
			})
			synchronized(&mu, func() {
				flushWriter(aoflog.wt)
			})
		}
	}).Run()

	log.Infof("dump: (w,a) = (rdb,aof)")

	NewJob(func() {
		for stop := false; !stop; {
			select {
			case <-done:
				stop = true
			case <-time.After(time.Second):
			}
			stats := &struct {
				output, aoflog int64
			}{
				output.wbytes.Int64(),
				aoflog.wbytes.Int64(),
			}

			var b bytes.Buffer
			var percent float64
			if rdbSize != 0 {
				percent = float64(stats.output) * 100 / float64(rdbSize)
			}
			if rdbSize >= stats.output {
				fmt.Fprintf(&b, "dump: rdb = %d - [%6.2f%%]", rdbSize, percent)
			} else {
				fmt.Fprintf(&b, "dump: rdb = %d", rdbSize)
			}
			fmt.Fprintf(&b, "   (w,a)=%s",
				formatAlign(4, "(%d,%d)", stats.output, stats.aoflog))
			fmt.Fprintf(&b, "  ~  (%s,%s)",
				bytesize.Int64(stats.output).HumanString(), bytesize.Int64(stats.aoflog).HumanString())
			log.Info(b.String())
		}
	}).RunAndWait()

	log.Info("dump: done")
}
