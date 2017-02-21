// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
	"github.com/CodisLabs/redis-port/pkg/redis"
)

type cmdRestore struct {
	rbytes, ebytes, nentry, ignore atomic2.Int64

	forward, nbypass atomic2.Int64
}

type cmdRestoreStat struct {
	rbytes, ebytes, nentry, ignore int64

	forward, nbypass int64
}

func (cmd *cmdRestore) Stat() *cmdRestoreStat {
	return &cmdRestoreStat{
		rbytes: cmd.rbytes.Get(),
		ebytes: cmd.ebytes.Get(),
		nentry: cmd.nentry.Get(),
		ignore: cmd.ignore.Get(),

		forward: cmd.forward.Get(),
		nbypass: cmd.nbypass.Get(),
	}
}

func (cmd *cmdRestore) Main() {
	input, target := args.input, args.target
	if len(target) == 0 {
		log.Panic("invalid argument: target")
	}
	if len(input) == 0 {
		input = "/dev/stdin"
	}

	log.Infof("restore from '%s' to '%s'\n", input, target)

	var readin io.ReadCloser
	var nsize int64
	if input != "/dev/stdin" {
		readin, nsize = openReadFile(input)
		defer readin.Close()
	} else {
		readin, nsize = os.Stdin, 0
	}

	reader := bufio.NewReaderSize(readin, ReaderBufferSize)

	cmd.RestoreRDBFile(reader, target, args.auth, nsize)

	if !args.extra {
		return
	}

	if nsize != 0 && nsize == cmd.rbytes.Get() {
		return
	}

	cmd.RestoreCommand(reader, target, args.auth)
}

func (cmd *cmdRestore) RestoreRDBFile(reader *bufio.Reader, target, passwd string, nsize int64) {
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
		if nsize != 0 {
			fmt.Fprintf(&b, "total = %d - %12d [%3d%%]", nsize, stat.rbytes, 100*stat.rbytes/nsize)
		} else {
			fmt.Fprintf(&b, "total = %12d", stat.rbytes)
		}
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		if stat.ignore != 0 {
			fmt.Fprintf(&b, "  ignore=%-12d", stat.ignore)
		}
		log.Info(b.String())
	}
	log.Info("restore: rdb done")
}

func (cmd *cmdRestore) RestoreCommand(reader *bufio.Reader, target, passwd string) {
	c := openNetConn(target, passwd)
	defer c.Close()

	writer := bufio.NewWriterSize(c, WriterBufferSize)
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
		fmt.Fprintf(&b, "restore: ")
		fmt.Fprintf(&b, " +forward=%-6d", nstat.forward-lstat.forward)
		fmt.Fprintf(&b, " +nbypass=%-6d", nstat.nbypass-lstat.nbypass)
		log.Info(b.String())
		lstat = nstat
	}
}
