// Copyright 2016 CodisLabs. All Rights Reserved.
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

	"github.com/CodisLabs/redis-port/pkg/libs/atomic2"
	"github.com/CodisLabs/redis-port/pkg/libs/io/pipe"
	"github.com/CodisLabs/redis-port/pkg/libs/log"
	"github.com/CodisLabs/redis-port/pkg/libs/stats"
	"github.com/CodisLabs/redis-port/pkg/redis"
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
	targetdb := args.targetdb
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

	var input io.ReadCloser
	var nsize int64
	if args.psync {
		input, nsize = cmd.SendPSyncCmd(from, args.passwd)
	} else {
		input, nsize = cmd.SendSyncCmd(from, args.passwd)
	}
	defer input.Close()

	log.Infof("rdb file = %d\n", nsize)

	if sockfile != nil {
		r, w := pipe.NewFilePipe(int(args.filesize), sockfile)
		defer r.Close()
		go func(r io.Reader) {
			defer w.Close()
			p := make([]byte, ReaderBufferSize)
			for {
				iocopy(r, w, p, len(p))
			}
		}(input)
		input = r
	}

	reader := bufio.NewReaderSize(input, ReaderBufferSize)

	cmd.SyncRDBFile(reader, target, args.auth, nsize, targetdb)
	cmd.SyncCommand(reader, target, args.auth,targetdb)
}

func (cmd *cmdSync) SendSyncCmd(master, passwd string) (net.Conn, int64) {
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

func (cmd *cmdSync) SendPSyncCmd(master, passwd string) (pipe.Reader, int64) {
	c := openNetConn(master, passwd)
	br := bufio.NewReaderSize(c, ReaderBufferSize)
	bw := bufio.NewWriterSize(c, WriterBufferSize)

	runid, offset, wait := sendPSyncFullsync(br, bw)
	log.Infof("psync runid = %s offset = %d, fullsync", runid, offset)

	var nsize int64
	for nsize == 0 {
		select {
		case nsize = <-wait:
			if nsize == 0 {
				log.Info("+")
			}
		case <-time.After(time.Second):
			log.Info("-")
		}
	}

	piper, pipew := pipe.NewSize(ReaderBufferSize)

	go func() {
		defer pipew.Close()
		p := make([]byte, 8192)
		for rdbsize := int(nsize); rdbsize != 0; {
			rdbsize -= iocopy(br, pipew, p, rdbsize)
		}
		for {
			n, err := cmd.PSyncPipeCopy(c, br, bw, offset, pipew)
			if err != nil {
				log.PanicErrorf(err, "psync runid = %s, offset = %d, pipe is broken", runid, offset)
			}
			offset += n
			for {
				time.Sleep(time.Second)
				c = openNetConnSoft(master, passwd)
				if c != nil {
					log.Infof("psync reopen connection, offset = %d", offset)
					break
				} else {
					log.Infof("psync reopen connection, failed")
				}
			}
			authPassword(c, passwd)
			br = bufio.NewReaderSize(c, ReaderBufferSize)
			bw = bufio.NewWriterSize(c, WriterBufferSize)
			sendPSyncContinue(br, bw, runid, offset)
		}
	}()
	return piper, nsize
}

func (cmd *cmdSync) PSyncPipeCopy(c net.Conn, br *bufio.Reader, bw *bufio.Writer, offset int64, copyto io.Writer) (int64, error) {
	defer c.Close()
	var nread atomic2.Int64
	go func() {
		defer c.Close()
		for {
			time.Sleep(time.Second * 5)
			if err := sendPSyncAck(bw, offset+nread.Get()); err != nil {
				return
			}
		}
	}()

	var p = make([]byte, 8192)
	for {
		n, err := br.Read(p)
		if err != nil {
			return nread.Get(), nil
		}
		if _, err := copyto.Write(p[:n]); err != nil {
			return nread.Get(), err
		}
		nread.Add(int64(n))
	}
}

func (cmd *cmdSync) SyncRDBFile(reader *bufio.Reader, target, passwd string, nsize int64, targetdb uint32) {
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
				c := openRedisConn(target, passwd)
				defer c.Close()
				var lastdb uint32 = 0
				for e := range pipe {
					if !acceptDB(e.DB) {
						cmd.ignore.Incr()
					} else {
						cmd.nentry.Incr()
						if !targetDB(e.DB) {
							e.DB = targetdb
							//lastdb = targetdb
						}
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

func (cmd *cmdSync) SyncCommand(reader *bufio.Reader, target, passwd string, targetdb uint32) {
	c := openNetConn(target, passwd)
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
					if !bypass {
						if !targetDB(uint32(n)){
							resp = redis.NewCommand(scmd,targetdb);

						}
					}
				}
				if bypass {
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
		fmt.Fprintf(&b, " +nbytes=%d", nstat.wbytes-lstat.wbytes)
		log.Info(b.String())
		lstat = nstat
	}
}
