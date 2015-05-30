// Copyright 2014 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"time"

	"github.com/wandoulabs/redis-port/pkg/libs/atomic2"
	"github.com/wandoulabs/redis-port/pkg/libs/log"
)

type cmdDump struct {
}

func (cmd *cmdDump) Main() {
	from, output := args.from, args.output
	if len(from) == 0 {
		log.Panic("invalid argument: from")
	}
	if len(output) == 0 {
		output = "/dev/stdout"
	}

	log.Infof("dump from '%s' to '%s'\n", from, output)

	var dumpto io.WriteCloser
	if output != "/dev/stdout" {
		dumpto = openWriteFile(output)
		defer dumpto.Close()
	} else {
		dumpto = os.Stdout
	}

	master, nsize := cmd.SendCmd(from, args.passwd)
	defer master.Close()

	log.Infof("rdb file = %d\n", nsize)

	reader := bufio.NewReaderSize(master, ReaderBufferSize)
	writer := bufio.NewWriterSize(dumpto, WriterBufferSize)

	cmd.DumpRDBFile(reader, writer, nsize)

	if !args.extra {
		return
	}

	cmd.DumpCommand(reader, writer, nsize)
}

func (cmd *cmdDump) SendCmd(master, passwd string) (net.Conn, int64) {
	c, wait := openSyncConn(master, passwd)
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
	return c, nsize
}

func (cmd *cmdDump) DumpRDBFile(reader *bufio.Reader, writer *bufio.Writer, nsize int64) {
	var nread atomic2.Int64
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		p := make([]byte, WriterBufferSize)
		for nsize != nread.Get() {
			nstep := int(nsize - nread.Get())
			ncopy := int64(iocopy(reader, writer, p, nstep))
			nread.Add(ncopy)
			flushWriter(writer)
		}
	}()

	for done := false; !done; {
		select {
		case <-wait:
			done = true
		case <-time.After(time.Second):
		}
		n := nread.Get()
		p := 100 * n / nsize
		log.Infof("total = %d - %12d [%3d%%]\n", nsize, n, p)
	}
	log.Info("dump: rdb done")
}

func (cmd *cmdDump) DumpCommand(reader *bufio.Reader, writer *bufio.Writer, nsize int64) {
	var nread atomic2.Int64
	go func() {
		p := make([]byte, ReaderBufferSize)
		for {
			ncopy := int64(iocopy(reader, writer, p, len(p)))
			nread.Add(ncopy)
			flushWriter(writer)
		}
	}()

	for {
		time.Sleep(time.Second)
		log.Infof("dump: total = %d\n", nsize+nread.Get())
	}
}
