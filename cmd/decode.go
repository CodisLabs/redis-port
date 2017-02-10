// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/CodisLabs/redis-port/pkg/libs/atomic2"
	"github.com/CodisLabs/redis-port/pkg/libs/log"
	"github.com/CodisLabs/redis-port/pkg/rdb"
)

type cmdDecode struct {
	rbytes, wbytes, nentry atomic2.Int64
}

type cmdDecodeStat struct {
	rbytes, wbytes, nentry int64
}

func (cmd *cmdDecode) Stat() *cmdDecodeStat {
	return &cmdDecodeStat{
		rbytes: cmd.rbytes.Get(),
		wbytes: cmd.wbytes.Get(),
		nentry: cmd.nentry.Get(),
	}
}

func (cmd *cmdDecode) Main() {
	input, output := args.input, args.output
	if len(input) == 0 {
		input = "/dev/stdin"
	}
	if len(output) == 0 {
		output = "/dev/stdout"
	}

	log.Infof("decode from '%s' to '%s'\n", input, output)

	var readin io.ReadCloser
	var nsize int64
	if input != "/dev/stdin" {
		readin, nsize = openReadFile(input)
		defer readin.Close()
	} else {
		readin, nsize = os.Stdin, 0
	}

	var saveto io.WriteCloser
	if output != "/dev/stdout" {
		saveto = openWriteFile(output)
		defer saveto.Close()
	} else {
		saveto = os.Stdout
	}

	reader := bufio.NewReaderSize(readin, ReaderBufferSize)
	writer := bufio.NewWriterSize(saveto, WriterBufferSize)

	ipipe := newRDBLoader(reader, &cmd.rbytes, args.parallel*32)
	opipe := make(chan string, cap(ipipe))

	go func() {
		defer close(opipe)
		group := make(chan int, args.parallel)
		for i := 0; i < cap(group); i++ {
			go func() {
				defer func() {
					group <- 0
				}()
				cmd.decoderMain(ipipe, opipe)
			}()
		}
		for i := 0; i < cap(group); i++ {
			<-group
		}
	}()

	wait := make(chan struct{})
	go func() {
		defer close(wait)
		for s := range opipe {
			cmd.wbytes.Add(int64(len(s)))
			if _, err := writer.WriteString(s); err != nil {
				log.PanicError(err, "write string failed")
			}
			flushWriter(writer)
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
		fmt.Fprintf(&b, "decode: ")
		if nsize != 0 {
			fmt.Fprintf(&b, "total = %d - %12d [%3d%%]", nsize, stat.rbytes, 100*stat.rbytes/nsize)
		} else {
			fmt.Fprintf(&b, "total = %12d", stat.rbytes)
		}
		fmt.Fprintf(&b, "  write=%-12d", stat.wbytes)
		fmt.Fprintf(&b, "  entry=%-12d", stat.nentry)
		log.Info(b.String())
	}
	log.Info("decode: done")
}

func (cmd *cmdDecode) decoderMain(ipipe <-chan *rdb.BinEntry, opipe chan<- string) {
	encodeJson := func(w *bytes.Buffer, o interface{}) {
		b, err := json.Marshal(o)
		if err != nil {
			log.PanicError(err, "encode to json failed")
		}
		if _, err := w.Write(b); err != nil {
			log.PanicError(err, "encode to json failed")
		}
		if _, err := w.WriteString("\n"); err != nil {
			log.PanicError(err, "encode to json failed")
		}
	}
	for e := range ipipe {
		o, err := rdb.DecodeDump(e.Value)
		if err != nil {
			log.PanicError(err, "decode failed")
		}
		var b = &bytes.Buffer{}
		switch obj := o.(type) {
		default:
			log.Panicf("unknown object %v", o)
		case rdb.String:
			encodeJson(b, &struct {
				DB    uint32 `json:"db"`
				Type  string `json:"type"`
				Key   string `json:"key"`
				Value string `json:"value"`
			}{
				e.DB, "string", string(e.Key), string(obj),
			})
		case rdb.List:
			for i, ele := range obj {
				encodeJson(b, &struct {
					DB    uint32 `json:"db"`
					Type  string `json:"type"`
					Key   string `json:"key"`
					Index int    `json:"index"`
					Value string `json:"value"`
				}{
					e.DB, "list", string(e.Key), i, string(ele),
				})
			}
		case rdb.Hash:
			for _, ele := range obj {
				encodeJson(b, &struct {
					DB    uint32 `json:"db"`
					Type  string `json:"type"`
					Key   string `json:"key"`
					Field string `json:"field"`
					Value string `json:"value"`
				}{
					e.DB, "hash", string(e.Key), string(ele.Field), string(ele.Value),
				})
			}
		case rdb.Set:
			for _, mem := range obj {
				encodeJson(b, &struct {
					DB     uint32 `json:"db"`
					Type   string `json:"type"`
					Key    string `json:"key"`
					Member string `json:"member"`
				}{
					e.DB, "dict", string(e.Key), string(mem),
				})
			}
		case rdb.ZSet:
			for _, ele := range obj {
				encodeJson(b, &struct {
					DB     uint32  `json:"db"`
					Type   string  `json:"type"`
					Key    string  `json:"key"`
					Member string  `json:"member"`
					Score  float64 `json:"score"`
				}{
					e.DB, "zset", string(e.Key), string(ele.Member), ele.Score,
				})
			}
		}
		if e.ExpireAt != 0 {
			encodeJson(b, &struct {
				DB       uint32 `json:"db"`
				Type     string `json:"type"`
				Key      string `json:"key"`
				ExpireAt uint64 `json:"expireat"`
			}{
				e.DB, "expire", string(e.Key), e.ExpireAt,
			})
		}
		cmd.nentry.Incr()
		opipe <- b.String()
	}
}
