// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"bufio"
	"io"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
	"github.com/CodisLabs/redis-port/pkg/libs/stats"
	"github.com/CodisLabs/redis-port/pkg/rdb"
	"github.com/CodisLabs/redis-port/pkg/redis"

	redigo "github.com/garyburd/redigo/redis"
	"io/ioutil"
)

func openRedisConn(target, passwd string) redigo.Conn {
	return redigo.NewConn(openNetConn(target, passwd), 0, 0)
}

func openNetConn(target, passwd string) net.Conn {
	c, err := net.Dial("tcp", target)
	if err != nil {
		log.PanicErrorf(err, "cannot connect to '%s'", target)
	}
	authPassword(c, passwd)
	return c
}

func openNetConnSoft(target, passwd string) net.Conn {
	c, err := net.Dial("tcp", target)
	if err != nil {
		return nil
	}
	authPassword(c, passwd)
	return c
}

func openReadFile(name string) (*os.File, int64) {
	f, err := os.Open(name)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-reader '%s'", name)
	}
	s, err := f.Stat()
	if err != nil {
		log.PanicErrorf(err, "cannot stat file-reader '%s'", name)
	}
	return f, s.Size()
}

func openWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-writer '%s'", name)
	}
	return f
}

func openReadWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		log.PanicErrorf(err, "cannot open file-readwriter '%s'", name)
	}
	return f
}

func authPassword(c net.Conn, passwd string) {
	if passwd == "" {
		return
	}
	_, err := c.Write(redis.MustEncodeToBytes(redis.NewCommand("auth", passwd)))
	if err != nil {
		log.PanicError(errors.Trace(err), "write auth command failed")
	}
	var b = make([]byte, 5)
	if _, err := io.ReadFull(c, b); err != nil {
		log.PanicError(errors.Trace(err), "read auth response failed")
	}
	if strings.ToUpper(string(b)) != "+OK\r\n" {
		log.Panic("auth failed")
	}
}

func openSyncConn(target string, passwd string) (net.Conn, <-chan int64) {
	c := openNetConn(target, passwd)
	if _, err := c.Write(redis.MustEncodeToBytes(redis.NewCommand("sync"))); err != nil {
		log.PanicError(errors.Trace(err), "write sync command failed")
	}
	return c, waitRdbDump(c)
}

func waitRdbDump(r io.Reader) <-chan int64 {
	size := make(chan int64)
	go func() {
		var rsp string
		for {
			b := []byte{0}
			if _, err := r.Read(b); err != nil {
				log.PanicErrorf(err, "read sync response = '%s'", rsp)
			}
			if len(rsp) == 0 && b[0] == '\n' {
				size <- 0
				continue
			}
			rsp += string(b)
			if strings.HasSuffix(rsp, "\r\n") {
				break
			}
		}
		if rsp[0] != '$' {
			log.Panicf("invalid sync response, rsp = '%s'", rsp)
		}
		n, err := strconv.Atoi(rsp[1 : len(rsp)-2])
		if err != nil || n <= 0 {
			log.PanicErrorf(err, "invalid sync response = '%s', n = %d", rsp, n)
		}
		size <- int64(n)
	}()
	return size
}

func sendPSyncFullsync(br *bufio.Reader, bw *bufio.Writer) (string, int64, <-chan int64) {
	cmd := redis.NewCommand("psync", "?", -1)
	if err := redis.Encode(bw, cmd, true); err != nil {
		log.PanicError(err, "write psync command failed, fullsync")
	}
	r, err := redis.Decode(br)
	if err != nil {
		log.PanicError(err, "invalid psync response, fullsync")
	}
	if e, ok := r.(*redis.Error); ok {
		log.Panicf("invalid psync response, fullsync, %s", e.Value)
	}
	x, err := redis.AsString(r, nil)
	if err != nil {
		log.PanicError(err, "invalid psync response, fullsync")
	}
	xx := strings.Split(x, " ")
	if len(xx) != 3 || strings.ToLower(xx[0]) != "fullresync" {
		log.Panicf("invalid psync response = '%s', should be fullsync", x)
	}
	v, err := strconv.ParseInt(xx[2], 10, 64)
	if err != nil {
		log.PanicError(err, "parse psync offset failed")
	}
	runid, offset := xx[1], v-1
	return runid, offset, waitRdbDump(br)
}

func sendPSyncContinue(br *bufio.Reader, bw *bufio.Writer, runid string, offset int64) {
	cmd := redis.NewCommand("psync", runid, offset+2)
	if err := redis.Encode(bw, cmd, true); err != nil {
		log.PanicError(err, "write psync command failed, continue")
	}
	r, err := redis.Decode(br)
	if err != nil {
		log.PanicError(err, "invalid psync response, continue")
	}
	if e, ok := r.(*redis.Error); ok {
		log.Panicf("invalid psync response, continue, %s", e.Value)
	}
	x, err := redis.AsString(r, nil)
	if err != nil {
		log.PanicError(err, "invalid psync response, continue")
	}
	xx := strings.Split(x, " ")
	if len(xx) != 1 || strings.ToLower(xx[0]) != "continue" {
		log.Panicf("invalid psync response = '%s', should be continue", x)
	}
}

func sendPSyncAck(bw *bufio.Writer, offset int64) error {
	cmd := redis.NewCommand("REPLCONF", "ack", offset)
	return redis.Encode(bw, cmd, true)
}

func selectDB(c redigo.Conn, db uint32) {
	_, err := redigo.String(c.Do("SELECT", db))
	if err != nil {
		log.PanicError(err, "SELECT command error")
	}
}

func restoreRdbEntry(c redigo.Conn, e *rdb.BinEntry, codis bool) {
	var ttlms uint64
	if e.ExpireAt != 0 {
		now := uint64(time.Now().Add(args.shift).UnixNano())
		now /= uint64(time.Millisecond)
		if now >= e.ExpireAt {
			ttlms = 1
		} else {
			ttlms = e.ExpireAt - now
		}
	}
	const MaxValueSize = bytesize.MB * 128

	if len(e.Value) < MaxValueSize {
		if codis {
			_, err := redigo.String(c.Do("SLOTSRESTORE", e.Key, ttlms, e.Value))
			if err != nil {
				log.PanicError(err, "SLOTSRESTORE command error")
			}
		} else {
			_, err := redigo.String(c.Do("RESTORE", e.Key, ttlms, e.Value, "REPLACE"))
			if err != nil {
				log.PanicError(err, "RESTORE command error")
			}
		}
	} else {
		o, err := e.ObjEntry()
		if err != nil {
			log.PanicErrorf(err, "decode object failed")
		}
		const MaxPipeline = 128

		var (
			wait = &sync.WaitGroup{}
			send = make(chan []interface{}, MaxPipeline)
			recv = make(chan struct{}, MaxPipeline)
		)
		go func() {
			for _ = range recv {
				r, err := c.Receive()
				if err != nil {
					log.PanicErrorf(err, "receive error")
				}
				if err, ok := r.(redigo.Error); ok {
					log.PanicErrorf(err, "receive error")
				}
				wait.Done()
			}
		}()
		go func() {
			defer close(recv)
			for args := range send {
				if err := c.Send(args[0].(string), args[1:]...); err != nil {
					log.PanicErrorf(err, "send error")
				}
				if len(send) == 0 || len(recv) == cap(recv) {
					if err := c.Flush(); err != nil {
						log.PanicErrorf(err, "flush error")
					}
				}
				recv <- struct{}{}
			}
		}()
		defer func() {
			close(send)
			wait.Wait()
		}()

		sendCommand := func(args ...interface{}) {
			wait.Add(1)
			send <- args
		}

		switch o.Value.(type) {
		default:
			log.Panicf("unknown object %v", e)
		case rdb.String:
			sendCommand("SET", o.Key, []byte(o.Value.(rdb.String)))
		case rdb.List:
			sendCommand("DEL", o.Key)
			var list = o.Value.(rdb.List)
			for len(list) != 0 {
				var args = []interface{}{
					"RPUSH", o.Key,
				}
				for i := 0; i < 30 && len(list) != 0; i++ {
					args = append(args, list[0])
					list = list[1:]
				}
				sendCommand(args...)
			}
		case rdb.Hash:
			sendCommand("DEL", o.Key)
			var hash = o.Value.(rdb.Hash)
			for len(hash) != 0 {
				var args = []interface{}{
					"HMSET", o.Key,
				}
				for i := 0; i < 30 && len(hash) != 0; i++ {
					args = append(args, hash[0].Field, hash[0].Value)
					hash = hash[1:]
				}
				sendCommand(args...)
			}
		case rdb.ZSet:
			sendCommand("DEL", o.Key)
			var zset = o.Value.(rdb.ZSet)
			for len(zset) != 0 {
				var args = []interface{}{
					"ZADD", o.Key,
				}
				for i := 0; i < 30 && len(zset) != 0; i++ {
					args = append(args, zset[0].Score, zset[0].Member)
					zset = zset[1:]
				}
				sendCommand(args...)
			}
		case rdb.Set:
			sendCommand("DEL", o.Key)
			var dict = o.Value.(rdb.Set)
			for len(dict) != 0 {
				var args = []interface{}{
					"SADD", o.Key,
				}
				for i := 0; i < 30 && len(dict) != 0; i++ {
					args = append(args, dict[0])
					dict = dict[1:]
				}
				sendCommand(args...)
			}
		}
		if ttlms != 0 {
			sendCommand("PEXPIRE", o.Key, ttlms)
		}
	}
}

func iocopy(r io.Reader, w io.Writer, p []byte, max int) int {
	if max <= 0 || len(p) == 0 {
		log.Panicf("invalid max = %d, len(p) = %d", max, len(p))
	}
	if len(p) > max {
		p = p[:max]
	}
	if n, err := r.Read(p); err != nil {
		log.PanicError(err, "read error")
	} else {
		p = p[:n]
	}
	if _, err := w.Write(p); err != nil {
		log.PanicError(err, "write error")
	}
	return len(p)
}

func flushWriter(w *bufio.Writer) {
	if err := w.Flush(); err != nil {
		log.PanicError(err, "flush error")
	}
}

func newRDBLoader(reader *bufio.Reader, rbytes *atomic2.Int64, size int) chan *rdb.BinEntry {
	pipe := make(chan *rdb.BinEntry, size)
	go func() {
		defer close(pipe)
		l := rdb.NewLoader(stats.NewCountReader(reader, rbytes))
		if err := l.Header(); err != nil {
			log.PanicError(err, "parse rdb header error")
		}
		for {
			if entry, err := l.NextBinEntry(); err != nil {
				log.PanicError(err, "parse rdb entry error")
			} else {
				if entry != nil {
					pipe <- entry
				} else {
					if err := l.Footer(); err != nil {
						log.PanicError(err, "parse rdb checksum error")
					}
					return
				}
			}
		}
	}()
	return pipe
}

func GetSpecifiedKeys(name string) map[string]bool {
	buf, err := ioutil.ReadFile(name)
	if err != nil {
		log.PanicError(err, "read specified key list file error")
		return nil
	}
	keys := strings.Split(string(buf), "\n")
	dict := make(map[string]bool, len(keys))
	for i, key := range keys {
		log.Infof("line %d:specified key is %s\n", i, key)
		dict[key] = true
	}
	return dict
}

func IsSpecifiedKey(key, pattern string, keys map[string]bool) bool {
	var matched bool
	var err error
	if nil != keys && keys[key] == true {
		matched = true
		return matched
	}
	if pattern != "" {
		matched, err = regexp.MatchString(pattern, key)
		if err != nil {
			log.Panicf("regex match key(%s) pattern(%s) error.\n", key, pattern)
		}
	}
	if !matched {
		log.Infof("the key %s is not specified", key)
	}
	return matched
}
