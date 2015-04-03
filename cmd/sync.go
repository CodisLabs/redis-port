// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"time"

	"github.com/wandoulabs/redis-port/pkg/libs/atomic2"
	"github.com/wandoulabs/redis-port/pkg/libs/io/pipe"
	"github.com/wandoulabs/redis-port/pkg/libs/log"
	"github.com/wandoulabs/redis-port/pkg/libs/stats"
	"github.com/wandoulabs/redis-port/pkg/redis"
)

type cmdSync struct {
	rbytes, wbytes, nentry, ignore atomic2.Int64

	forward, nbypass atomic2.Int64
}

type cmdSyncStat struct {
	rbytes, wbytes, nentry, ignore int64

	forward, nbypass int64
}

func (cmd *cmdSync) Stat() *cmdSyncStat {
	return &cmdSyncStat{
		rbytes: cmd.rbytes.Get(),
		wbytes: cmd.wbytes.Get(),
		nentry: cmd.nentry.Get(),
		ignore: cmd.ignore.Get(),

		forward: cmd.forward.Get(),
		nbypass: cmd.nbypass.Get(),
	}
}

func (cmd *cmdSync) Main() {
	from, target := args.from, args.target
	if len(from) == 0 {
		log.Panic("invalid argument: from")
	}
	if len(target) == 0 {
		log.Panic("invalid argument: target")
	}

	log.Infof("sync from '%s' to '%s'\n", from, target)

	var sockfile *os.File
	if len(args.sockfile) != 0 {
		sockfile = openReadWriteFile(args.sockfile)
		defer sockfile.Close()
	}

	master, nsize := cmd.SendCmd(from, args.passwd)
	defer master.Close()

	log.Infof("rdb file = %d\n", nsize)

	var input io.Reader
	if sockfile != nil {
		r, w := pipe.NewFilePipe(int(args.filesize), sockfile)
		defer r.Close()
		go func() {
			defer w.Close()
			p := make([]byte, ReaderBufferSize)
			for {
				iocopy(master, w, p, len(p))
			}
		}()
		input = r
	} else {
		input = master
	}

	reader := bufio.NewReaderSize(input, ReaderBufferSize)

	cmd.SyncRDBFile(reader, target, nsize)
	cmd.SyncCommand(reader, target)
}

func (cmd *cmdSync) SendCmd(master, passwd string) (net.Conn, int64) {
	c, wait := openSyncConn(master, passwd)
	for {
		select {
		case nsize := <-wait:
			if nsize == 0 {
				log.Info("+")
			} else {
				return c, nsize
			}
		case <-time.After(time.Second):
			log.Info("-")
		}
	}
}

func (cmd *cmdSync) SyncRDBFile(reader *bufio.Reader, slave string, nsize int64) {
	pipe := newRDBLoader(reader, &cmd.rbytes, args.parallel*32)
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		group := make(chan int, args.parallel)
		for i := 0; i < cap(group); i++ {
			go func() {
				defer func() {
					group <- 0
				}()
				c := openRedisConn(slave)
				defer c.Close()
				var lastdb uint32 = 0
				for e := range pipe {
					if !acceptDB(e.DB) || !acceptKey(e.Key) {
						cmd.ignore.Incr()
					} else {
						cmd.nentry.Incr()
						if e.DB != lastdb {
							lastdb = e.DB
							selectDB(c, lastdb)
						}
						restoreRdbEntry(c, e)
					}
				}
			}()
		}
		for i := 0; i < cap(group); i++ {
			<-group
		}
	}()

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		stat := cmd.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "total=%d - %12d [%3d%%]", nsize, stat.rbytes, 100*stat.rbytes/nsize)
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		if stat.ignore != 0 {
			fmt.Fprintf(&b, "  ignore=%-12d", stat.ignore)
		}
		log.Info(b.String())
	}
	log.Info("sync rdb done")
}

func (cmd *cmdSync) SyncCommand(reader *bufio.Reader, slave string) {
	c := openNetConn(slave)
	defer c.Close()

	writer := bufio.NewWriterSize(stats.NewCountWriter(c, &cmd.wbytes), WriterBufferSize)
	defer flushWriter(writer)

	go func() {
		p := make([]byte, ReaderBufferSize)
		for {
			iocopy(c, ioutil.Discard, p, len(p))
		}
	}()

	go func() {
		var bypass bool = false
		for {
			resp := redis.MustDecode(reader)
			if scmd, args, err := redis.ParseArgs(resp); err != nil {
				log.PanicError(err, "parse command arguments failed")
			} else if scmd != "ping" {
				if scmd == "select" {
					if len(args) != 1 {
						log.Panicf("select command len(args) = %d", len(args))
					}
					s := string(args[0])
					n, err := parseInt(s, MinDB, MaxDB)
					if err != nil {
						log.PanicErrorf(err, "parse db = %s failed", s)
					}
					bypass = !acceptDB(uint32(n))
				}
				// Some commands like MSET may have multi keys, but we only use
				// first for filter
				if bypass || (len(args) > 0 && !acceptKey(args[0])) {
					cmd.nbypass.Incr()
					continue
				}
			}
			cmd.forward.Incr()
			redis.MustEncode(writer, resp)
			flushWriter(writer)
		}
	}()

	for lstat := cmd.Stat(); ; {
		time.Sleep(time.Second)
		nstat := cmd.Stat()
		var b bytes.Buffer
		fmt.Fprintf(&b, "sync: ")
		fmt.Fprintf(&b, " +forward=%-6d", nstat.forward-lstat.forward)
		fmt.Fprintf(&b, " +nbypass=%-6d", nstat.nbypass-lstat.nbypass)
		fmt.Fprintf(&b, " +nbytes=%d", nstat.wbytes-lstat.rbytes)
		log.Info(b.String())
		lstat = nstat
	}
}
