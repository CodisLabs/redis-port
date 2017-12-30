package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/bufio2"
	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"

	"github.com/CodisLabs/redis-port/pkg/libs/pipe"
	"github.com/CodisLabs/redis-port/pkg/rdb"
)

func main() {
	const usage = `
Usage:
	redis-sync [--ncpu=N] (--master=MASTER|MASTER) --target=TARGET [--db=DB] [--tmpfile-size=SIZE [--tmpfile=FILE]]
	redis-sync  --version

Options:
	-n N, --ncpu=N                    Set runtime.GOMAXPROCS to N.
	-m MASTER, --master=MASTER        The master redis instance ([auth@]host:port).
	-t TARGET, --target=TARGET        The target redis instance ([auth@]host:port).
	--db=DB                           Accept db = DB, default is *.
	--tmpfile=FILE                    Use FILE to as socket buffer.
	--tmpfile-size=SIZE               Set FILE size. If no --tmpfile is provided, a temporary file under current folder will be created.

Examples:
	$ redis-sync -m 127.0.0.1:6379 -t 127.0.0.1:6380
	$ redis-sync    127.0.0.1:6379 -t passwd@127.0.0.1:6380
	$ redis-sync    127.0.0.1:6379 -t passwd@127.0.0.1:6380 --db=0
	$ redis-sync    127.0.0.1:6379 -t passwd@127.0.0.1:6380 --db=0 --tmpfile-size=10gb
	$ redis-sync    127.0.0.1:6379 -t passwd@127.0.0.1:6380 --db=0 --tmpfile-size=10gb --tmpfile ~/sockfile.tmp
`
	var flags = parseFlags(usage)

	var master struct {
		Path       string
		Addr, Auth string
		net.Conn
		rd *bufio2.Reader
		wt *bufio2.Writer

		rdb, aof struct {
			forward, skip atomic2.Int64
		}
		rbytes atomic2.Int64
	}
	master.Path = flags.Source
	if len(master.Path) == 0 {
		log.Panicf("invalid master address")
	}
	master.Addr, master.Auth = redisParsePath(master.Path)
	if len(master.Addr) == 0 {
		log.Panicf("invalid master address")
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
		log.Panicf("invalid target address")
	}
	log.Infof("sync: master = %q, target = %q\n", master.Path, target.Path)

	var tmpfile *os.File
	if flags.TmpFile.Size != 0 {
		if flags.TmpFile.Path != "" {
			tmpfile = openReadWriteFile(flags.TmpFile.Path)
		} else {
			tmpfile = openTempFile(".", "tmpfile-")
		}
		defer closeFile(tmpfile)
	}

	master.Conn = openConn(master.Addr, master.Auth)
	defer master.Close()
	master.rd = rBuilder(master.Conn).
		Buffer2(ReaderBufferSize).Reader.(*bufio2.Reader)
	master.wt = wBuilder(master.Conn).
		Buffer2(WriterBufferSize).Writer.(*bufio2.Writer)

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
	log.Infof("sync: runid = %q, offset = %d", runid, offset)
	log.Infof("sync: rdb file = %d (%s)\n", rdbSize,
		bytesize.Int64(rdbSize).HumanString())

	var dumpoff atomic2.Int64
	var reploff = atomic2.Int64(offset)

	var pipeReader = func() pipe.Reader {
		var mp = pipe.NewPipe()
		go func() {
			defer mp.Close()
			var psync = &struct {
				net.Conn
				rd *bufio2.Reader
				wt *bufio2.Writer
			}{
				master.Conn,
				master.rd, master.wt,
			}
			ioCopyN(wBuilder(mp.Writer()).Count(&dumpoff).Writer, psync.rd, rdbSize)

			for {
				var fence = NewJob(func() {
					defer psync.Conn.Close()
					io.Copy(wBuilder(mp.Writer()).Count(&reploff).Writer, psync.rd)
				}).Run()

				NewJob(func() {
					defer psync.Conn.Close()
					for {
						if err := redisSendReplAckNoCheck(psync.wt, reploff.Int64()); err != nil {
							log.WarnErrorf(err, "send replconf failed")
							return
						}
						time.Sleep(time.Second)
					}
				}).RunAndWait()

				<-fence

				log.Infof("connect lost %q", master.Addr)

			try_again:
				time.Sleep(time.Second)
				c, err := net.Dial("tcp", master.Addr)
				if err != nil {
					log.WarnErrorf(err, "cannot connect to %q", master.Addr)
					goto try_again
				} else {
					log.Infof("reconnect to %q", master.Addr)
				}
				psync.Conn = authenticate(c, master.Auth)
				psync.rd = rBuilder(psync.Conn).
					Buffer2(ReaderBufferSize).Reader.(*bufio2.Reader)
				psync.wt = wBuilder(psync.Conn).
					Buffer2(WriterBufferSize).Writer.(*bufio2.Writer)
				redisSendPsyncContinue(psync.rd, psync.wt, runid, reploff.Int64())
			}
		}()
		if tmpfile == nil {
			return mp.Reader()
		} else {
			var fp = pipe.NewPipeFile(tmpfile, int(flags.TmpFile.Size))
			go func() {
				defer fp.Close()
				ioCopyBuffer(fp.Writer(), mp.Reader())
			}()
			return fp.Reader()
		}
	}()
	defer pipeReader.Close()

	var reader = rBuilder(pipeReader).Must().Count(&master.rbytes).
		Buffer2(ReaderBufferSize).Reader.(*bufio2.Reader)

	var entryChan = newRDBLoader(io.LimitReader(reader, rdbSize), 32)

	var jobs = NewParallelJob(flags.Parallel, func() {
		doRestoreDBEntry(entryChan, target.Addr, target.Auth,
			func(e *rdb.DBEntry) bool {
				if !acceptDB(e.DB) {
					master.rdb.skip.Incr()
					return false
				}
				master.rdb.forward.Incr()
				return true
			})
	}).Then(func() {
		doRestoreAoflog(reader, target.Addr, target.Auth,
			func(db uint64, cmd string) bool {
				if !acceptDB(db) && cmd != "PING" {
					master.aof.skip.Incr()
					return false
				}
				master.aof.forward.Incr()
				return true
			})
	}).Run()

	log.Infof("sync: (r/f,s/f,s) = (read,rdb.forward,rdb.skip/rdb.forward,rdb.skip)")

	NewJob(func() {
		var last, stats struct {
			rdb, aof struct {
				forward, skip int64
			}
			dumpoff, reploff, rbytes int64
		}
		for stop := false; !stop; {
			select {
			case <-jobs:
				stop = true
			case <-time.After(time.Second):
			}
			stats.dumpoff = dumpoff.Int64()
			stats.reploff = reploff.Int64()
			stats.rbytes = master.rbytes.Int64()
			stats.rdb.forward = master.rdb.forward.Int64()
			stats.rdb.skip = master.rdb.skip.Int64()
			stats.aof.forward = master.aof.forward.Int64()
			stats.aof.skip = master.aof.skip.Int64()

			var b bytes.Buffer
			var percent float64
			if rdbSize != 0 {
				percent = float64(stats.dumpoff) * 100 / float64(rdbSize)
			}
			fmt.Fprintf(&b, "sync: rdb = %d - [%6.2f%%]", rdbSize, percent)
			fmt.Fprintf(&b, "   (r/f,s/f,s)=%s",
				formatAlign(4, "(%d/%d,%d/%d,%d)", stats.rbytes,
					stats.rdb.forward, stats.rdb.skip,
					stats.aof.forward, stats.aof.skip))
			fmt.Fprintf(&b, "  ~  %s",
				formatAlign(4, "(%s/-,-/-,-)",
					bytesize.Int64(stats.rbytes).HumanString()))
			fmt.Fprintf(&b, "  ~  speed=%s",
				formatAlign(4, "(%s/%d,%d/%d,%d)",
					bytesize.Int64(stats.rbytes-last.rbytes).HumanString(),
					stats.rdb.forward-last.rdb.forward, stats.rdb.skip-last.rdb.skip,
					stats.aof.forward-last.aof.forward, stats.aof.skip-last.aof.skip))
			last = stats
			log.Info(b.String())
		}
	}).RunAndWait()

	log.Info("sync: done")
}

// func (cmd *cmdSync) Main() {
// 	var input io.ReadCloser
// 	var nsize int64
// 	if args.psync {
// 		input, nsize = cmd.SendPSyncCmd(from, args.passwd)
// 	}
// 	defer input.Close()
//
// 	log.Infof("rdb file = %d\n", nsize)
//
// 	reader := bufio.NewReaderSize(input, ReaderBufferSize)
//
// 	cmd.SyncRDBFile(reader, target, args.auth, nsize, args.codis)
// 	cmd.SyncCommand(reader, target, args.auth)
// }
//
// func (cmd *cmdSync) SyncRDBFile(reader *bufio.Reader, target, passwd string, nsize int64, codis bool) {
// 	pipe := newRDBLoader(reader, &cmd.rbytes, args.parallel*32)
// 	wait := make(chan struct{})
// 	go func() {
// 		defer close(wait)
// 		group := make(chan int, args.parallel)
// 		for i := 0; i < cap(group); i++ {
// 			go func() {
// 				defer func() {
// 					group <- 0
// 				}()
// 				c := openRedisConn(target, passwd)
// 				defer c.Close()
// 				var lastdb uint32 = 0
// 				for e := range pipe {
// 					if !acceptDB(e.DB) {
// 						cmd.ignore.Incr()
// 					} else {
// 						cmd.nentry.Incr()
// 						if e.DB != lastdb {
// 							lastdb = e.DB
// 							selectDB(c, lastdb)
// 						}
// 						restoreRdbEntry(c, e, codis)
// 					}
// 				}
// 			}()
// 		}
// 		for i := 0; i < cap(group); i++ {
// 			<-group
// 		}
// 	}()
//
// 	for done := false; !done; {
// 		select {
// 		case <-wait:
// 			done = true
// 		case <-time.After(time.Second):
// 		}
// 		stat := cmd.Stat()
// 		var b bytes.Buffer
// 		fmt.Fprintf(&b, "total=%d - %12d [%3d%%]", nsize, stat.rbytes, 100*stat.rbytes/nsize)
// 		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
// 		if stat.ignore != 0 {
// 			fmt.Fprintf(&b, "  ignore=%-12d", stat.ignore)
// 		}
// 		log.Info(b.String())
// 	}
// 	log.Info("sync rdb done")
// }
//
// func (cmd *cmdSync) SyncCommand(reader *bufio.Reader, target, passwd string) {
// 	c := openNetConn(target, passwd)
// 	defer c.Close()
//
// 	writer := bufio.NewWriterSize(stats.NewCountWriter(c, &cmd.wbytes), WriterBufferSize)
// 	defer flushWriter(writer)
//
// 	go func() {
// 		p := make([]byte, ReaderBufferSize)
// 		for {
// 			iocopy(c, ioutil.Discard, p, len(p))
// 		}
// 	}()
//
// 	go func() {
// 		var bypass bool = false
// 		for {
// 			resp := redis.MustDecode(reader)
// 			if scmd, args, err := redis.ParseArgs(resp); err != nil {
// 				log.PanicError(err, "parse command arguments failed")
// 			} else if scmd != "ping" {
// 				if scmd == "select" {
// 					if len(args) != 1 {
// 						log.Panicf("select command len(args) = %d", len(args))
// 					}
// 					s := string(args[0])
// 					n, err := parseInt(s, MinDB, MaxDB)
// 					if err != nil {
// 						log.PanicErrorf(err, "parse db = %s failed", s)
// 					}
// 					bypass = !acceptDB(uint32(n))
// 				}
// 				if bypass {
// 					cmd.nbypass.Incr()
// 					continue
// 				}
// 			}
// 			cmd.forward.Incr()
// 			redis.MustEncode(writer, resp)
// 			flushWriter(writer)
// 		}
// 	}()
//
// 	for lstat := cmd.Stat(); ; {
// 		time.Sleep(time.Second)
// 		nstat := cmd.Stat()
// 		var b bytes.Buffer
// 		fmt.Fprintf(&b, "sync: ")
// 		fmt.Fprintf(&b, " +forward=%-6d", nstat.forward-lstat.forward)
// 		fmt.Fprintf(&b, " +nbypass=%-6d", nstat.nbypass-lstat.nbypass)
// 		fmt.Fprintf(&b, " +nbytes=%d", nstat.wbytes-lstat.wbytes)
// 		log.Info(b.String())
// 		lstat = nstat
// 	}
// }
