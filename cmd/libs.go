package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/bufio2"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"

	"github.com/CodisLabs/redis-port/pkg/rdb"

	redigo "github.com/garyburd/redigo/redis"
)

func redisParsePath(path string) (addr, auth string) {
	var abspath = func() string {
		if strings.HasPrefix(path, "//") {
			return path
		} else {
			return "//" + path
		}
	}()
	u, err := url.Parse(abspath)
	if err != nil {
		log.PanicErrorf(err, "invalid redis address %q", path)
	}
	if u.User != nil {
		return u.Host, u.User.String()
	} else {
		return u.Host, ""
	}
}

func redisNewCommand(cmd string, args ...interface{}) *redis.Resp {
	var multi = make([]*redis.Resp, 0, len(args)+1)
	multi = append(multi, redis.NewBulkBytes([]byte(cmd)))
	for i := range args {
		switch v := args[i].(type) {
		case string:
			multi = append(multi, redis.NewBulkBytes([]byte(v)))
		case []byte:
			multi = append(multi, redis.NewBulkBytes(v))
		default:
			var b bytes.Buffer
			fmt.Fprint(&b, v)
			multi = append(multi, redis.NewBulkBytes(b.Bytes()))
		}
	}
	return redis.NewArray(multi)
}

func authenticate(c net.Conn, auth string) net.Conn {
	if auth == "" {
		return c
	}
	if b, err := redis.EncodeToBytes(redisNewCommand("AUTH", auth)); err != nil {
		log.PanicErrorf(err, "authenticate failed")
	} else if _, err := c.Write(b); err != nil {
		log.PanicErrorf(err, "authenticate failed")
	}
	var b = make([]byte, 5)
	if _, err := io.ReadFull(c, b); err != nil {
		log.PanicErrorf(err, "authenticate failed")
	}
	if strings.ToUpper(string(b)) != "+OK\r\n" {
		log.Panicf("authenticate failed, reply = %q", b)
	}
	return c
}

func redisSendPsyncFullsync(r *bufio2.Reader, w *bufio2.Writer) (string, int64, <-chan int64) {
	var enc = redis.NewEncoderBuffer(w)
	var dec = redis.NewDecoderBuffer(r)
	var cmd = redisNewCommand("PSYNC", "?", -1)
	redisSendCommand(enc, cmd, true)
	reply := redisRespAsString(redisGetResponse(dec))
	split := strings.Split(reply, " ")
	if len(split) != 3 || strings.ToLower(split[0]) != "fullresync" {
		log.Panicf("psync response = %q", reply)
	}
	n, err := strconv.ParseInt(split[2], 10, 64)
	if err != nil {
		log.PanicErrorf(err, "psync response = %q", reply)
	}
	runid, offset, rdbSize := split[1], n, make(chan int64)
	go func() {
		var rsp string
		for {
			b := []byte{0}
			if _, err := r.Read(b); err != nil {
				log.PanicErrorf(err, "psync response = %q", rsp)
			}
			if len(rsp) == 0 && b[0] == '\n' {
				rdbSize <- 0
				continue
			}
			rsp += string(b)
			if strings.HasSuffix(rsp, "\r\n") {
				break
			}
		}
		if rsp[0] != '$' {
			log.Panicf("psync response = %q", rsp)
		}
		n, err := strconv.Atoi(rsp[1 : len(rsp)-2])
		if err != nil || n <= 0 {
			log.PanicErrorf(err, "psync response = %q", rsp)
		}
		rdbSize <- int64(n)
	}()
	return runid, offset, rdbSize
}

func redisSendPsyncContinue(r *bufio2.Reader, w *bufio2.Writer, runid string, offset int64) {
	var enc = redis.NewEncoderBuffer(w)
	var dec = redis.NewDecoderBuffer(r)
	var cmd = redisNewCommand("PSYNC", runid, offset+1)
	redisSendCommand(enc, cmd, true)
	reply := redisRespAsString(redisGetResponse(dec))
	split := strings.Split(reply, " ")
	if len(split) != 1 || strings.ToLower(split[0]) != "continue" {
		log.Panicf("psync response = %q", reply)
	}
}

func redisSendReplAck(w *bufio2.Writer, offset int64) {
	var enc = redis.NewEncoderBuffer(w)
	var cmd = redisNewCommand("REPLCONF", "ack", offset)
	redisSendCommand(enc, cmd, true)
}

func redisSendReplAckNoCheck(w *bufio2.Writer, offset int64) error {
	var enc = redis.NewEncoderBuffer(w)
	var cmd = redisNewCommand("REPLCONF", "ack", offset)
	return enc.Encode(cmd, true)
}

func redisSendCommand(enc *redis.Encoder, cmd *redis.Resp, flush bool) {
	if err := enc.Encode(cmd, flush); err != nil {
		log.PanicErrorf(err, "encode resp failed")
	}
}

func redisGetResponse(dec *redis.Decoder) *redis.Resp {
	r, err := dec.Decode()
	if err != nil {
		log.PanicErrorf(err, "decode resp failed")
	}
	if r.IsError() {
		log.Panicf("error response = %q", string(r.Value))
	}
	return r
}

func redisRespAsString(r *redis.Resp) string {
	if !r.IsString() {
		log.Panicf("not string type(%s)", r.Type)
	}
	return string(r.Value)
}

func redisFlushEncoder(enc *redis.Encoder) {
	if err := enc.Flush(); err != nil {
		log.PanicErrorf(err, "flush encoder failed")
	}
}

func openConn(addr, auth string) net.Conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		log.PanicErrorf(err, "cannot connect to %q", addr)
	}
	return authenticate(c, auth)
}

func openReadFile(name string) (*os.File, int64) {
	f, err := os.Open(name)
	if err != nil {
		log.PanicErrorf(err, "can't open file %q", name)
	}
	s, err := f.Stat()
	if err != nil {
		log.PanicErrorf(err, "can't stat file %q", name)
	}
	return f, s.Size()
}

func openTempFile(dir, prefix string) *os.File {
	f, err := ioutil.TempFile(dir, prefix)
	if err != nil {
		log.PanicErrorf(err, "can't open temp file dir=%q prefix=%q", dir, prefix)
	}
	return f
}

func openWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
	if err != nil {
		log.PanicErrorf(err, "can't open file %q", name)
	}
	return f
}

func openReadWriteFile(name string) *os.File {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		log.PanicErrorf(err, "can't open file %q", name)
	}
	return f
}

func flushWriter(w *bufio.Writer) {
	if err := w.Flush(); err != nil {
		log.PanicErrorf(err, "flush writer failed")
	}
}

func closeFile(file *os.File) {
	if err := file.Close(); err != nil {
		log.PanicErrorf(err, "close file failed")
	}
}

func newRDBLoader(r io.Reader, size int) <-chan *rdb.DBEntry {
	var entryChan = make(chan *rdb.DBEntry, size)
	go func() {
		defer close(entryChan)
		loader := rdb.NewLoader(r)
		loader.Header()
		loader.ForEach(func(e *rdb.DBEntry) bool {
			entryChan <- e.IncrRefCount()
			return true
		})
		loader.Footer()
	}()
	return entryChan
}

func synchronized(l sync.Locker, fn func()) {
	l.Lock()
	fn()
	l.Unlock()
}

func formatAlign(align int, format string, args ...interface{}) string {
	var b bytes.Buffer
	fmt.Fprintf(&b, format, args...)
	for b.Len()%align != 0 {
		b.WriteByte(' ')
	}
	return b.String()
}

func ioCopyN(w io.Writer, r io.Reader, limit int64) {
	n, err := io.CopyN(w, r, limit)
	if err != nil {
		log.PanicErrorf(err, "copy bytes failed, n = %d, expected = %d", n, limit)
	}
}

func ioCopyBuffer(w io.Writer, r io.Reader) {
	_, err := io.CopyBuffer(w, r, make([]byte, 8192))
	if err != nil {
		log.PanicErrorf(err, "copy bytes failed")
	} else {
		log.Panicf("copy bytes failed, EOF")
	}
}

type Job struct {
	main func()
}

func NewJob(main func()) *Job {
	return &Job{main}
}

func NewParallelJob(parallel int, main func()) *Job {
	return &Job{func() {
		wg := &sync.WaitGroup{}
		wg.Add(parallel)
		for i := 0; i < parallel; i++ {
			go func() {
				defer wg.Done()
				main()
			}()
		}
		wg.Wait()
	}}
}

func (j *Job) Then(main func()) *Job {
	var last = j.main
	return &Job{func() { last(); main() }}
}

func (j *Job) Run() <-chan struct{} {
	var done = make(chan struct{})
	go func() {
		defer close(done)
		j.main()
	}()
	return done
}

func (j *Job) RunAndWait() {
	<-j.Run()
}

func toJson(v interface{}) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		log.PanicErrorf(err, "marshal to json failed")
	}
	return string(b)
}

func toJsonDBEntry(e *rdb.DBEntry, w *bufio.Writer) {
	encodeJson := func(o interface{}) {
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
	switch e.Value.Type() {
	default:
		log.Panicf("unknown object type=%s db=%d key=%q", e.Value.Type(), e.DB, e.Key.String())
	case rdb.OBJ_STRING:
		encodeJson(&struct {
			DB    uint64 `json:"db"`
			Type  string `json:"type"`
			Key   string `json:"key"`
			Value string `json:"value"`
		}{
			e.DB, "string", e.Key.StringUnsafe(), e.Value.AsString().StringUnsafe(),
		})
	case rdb.OBJ_LIST:
		e.Value.AsList().ForEach(func(iter *rdb.RedisListIterator, index int) bool {
			var field = iter.Next()
			if field == nil {
				return false
			}
			encodeJson(&struct {
				DB    uint64 `json:"db"`
				Type  string `json:"type"`
				Key   string `json:"key"`
				Index int    `json:"index"`
				Value string `json:"value"`
			}{
				e.DB, "list", e.Key.StringUnsafe(), index, field.StringUnsafe(),
			})
			return true
		})
	case rdb.OBJ_HASH:
		e.Value.AsHash().ForEach(func(iter *rdb.RedisHashIterator, index int) bool {
			var field, value = iter.Next()
			if field == nil {
				return false
			}
			encodeJson(&struct {
				DB    uint64 `json:"db"`
				Type  string `json:"type"`
				Key   string `json:"key"`
				Field string `json:"field"`
				Value string `json:"value"`
			}{
				e.DB, "hash", e.Key.StringUnsafe(), field.StringUnsafe(), value.StringUnsafe(),
			})
			return true
		})
	case rdb.OBJ_SET:
		e.Value.AsSet().ForEach(func(iter *rdb.RedisSetIterator, index int) bool {
			var member = iter.Next()
			if member == nil {
				return false
			}
			encodeJson(&struct {
				DB     uint64 `json:"db"`
				Type   string `json:"type"`
				Key    string `json:"key"`
				Member string `json:"member"`
			}{
				e.DB, "dict", e.Key.StringUnsafe(), member.StringUnsafe(),
			})
			return true
		})
	case rdb.OBJ_ZSET:
		e.Value.AsZset().ForEach(func(iter *rdb.RedisZsetIterator, index int) bool {
			var member = iter.Next()
			if member == nil {
				return false
			}
			encodeJson(&struct {
				DB     uint64  `json:"db"`
				Type   string  `json:"type"`
				Key    string  `json:"key"`
				Index  int     `json:"index"`
				Member string  `json:"member"`
				Score  float64 `json:"score"`
			}{
				e.DB, "zset", e.Key.StringUnsafe(), index, member.StringUnsafe(), member.Score,
			})
			return true
		})
	}
	if e.Expire != rdb.NoExpire {
		encodeJson(&struct {
			DB       uint64 `json:"db"`
			Type     string `json:"type"`
			Key      string `json:"key"`
			ExpireAt uint64 `json:"expireat"`
		}{
			e.DB, "expire", e.Key.StringUnsafe(), uint64(e.Expire / time.Millisecond),
		})
	}
}

func redigoOpenConn(addr, auth string) redigo.Conn {
	return redigo.NewConn(openConn(addr, auth), 0, 0)
}

func redigoSendCommand(c redigo.Conn, cmd string, args ...interface{}) {
	if err := c.Send(cmd, args...); err != nil {
		log.PanicErrorf(err, "send redigo request failed")
	}
}

func redigoFlushConnIf(c redigo.Conn, on func() bool) {
	if on() {
		redigoFlushConn(c)
	}
}

func redigoFlushConn(c redigo.Conn) {
	if err := c.Flush(); err != nil {
		log.PanicErrorf(err, "flush redigo connection failed")
	}
}

func redigoGetResponse(c redigo.Conn) {
	_, err := c.Receive()
	if err != nil {
		log.PanicErrorf(err, "fetch redigo reply failed")
	}
}

func genRestoreCommands(e *rdb.DBEntry, db uint64, on func(cmd string, args ...interface{})) {
	if db != e.DB {
		on("SELECT", e.DB)
	}
	var key = e.Key.BytesUnsafe()
	on("DEL", key)

	const MaxArgsNum = 511
	var args []interface{}
	var pushArgs = func(cmd string, added ...interface{}) {
		if len(args) == 0 {
			args = append(args, key)
		}
		args = append(args, added...)
		if len(args) < MaxArgsNum {
			return
		}
		on(cmd, args...)
		args = make([]interface{}, 0, MaxArgsNum+1)
	}
	var flushCommand = func(cmd string) {
		if len(args) == 0 {
			return
		}
		on(cmd, args...)
	}
	switch e.Value.Type() {
	default:
		log.Panicf("unknown object type db=%d key=%s", e.DB, e.Key.String())
	case rdb.OBJ_STRING:
		on("SET", key, e.Value.AsString().BytesUnsafe())
	case rdb.OBJ_LIST:
		e.Value.AsList().ForEach(func(iter *rdb.RedisListIterator, index int) bool {
			var field = iter.Next()
			if field == nil {
				return false
			}
			pushArgs("RPUSH", field.BytesUnsafe())
			return true
		})
		flushCommand("RPUSH")
	case rdb.OBJ_HASH:
		e.Value.AsHash().ForEach(func(iter *rdb.RedisHashIterator, index int) bool {
			var field, value = iter.Next()
			if field == nil {
				return false
			}
			pushArgs("HMSET", field.BytesUnsafe(), value.BytesUnsafe())
			return true
		})
		flushCommand("HMSET")
	case rdb.OBJ_SET:
		e.Value.AsSet().ForEach(func(iter *rdb.RedisSetIterator, index int) bool {
			var member = iter.Next()
			if member == nil {
				return false
			}
			pushArgs("SADD", member.BytesUnsafe())
			return true
		})
		flushCommand("SADD")
	case rdb.OBJ_ZSET:
		e.Value.AsZset().ForEach(func(iter *rdb.RedisZsetIterator, index int) bool {
			var member = iter.Next()
			if member == nil {
				return false
			}
			pushArgs("ZADD", member.Score, member.BytesUnsafe())
			return true
		})
		flushCommand("SADD")
	}
	if e.Expire != rdb.NoExpire {
		on("PEXPIREAT", key, int64(e.Expire/time.Millisecond))
	}
}

func doRestoreDBEntry(entryChan <-chan *rdb.DBEntry, addr, auth string, on func(e *rdb.DBEntry) bool) {
	var ticker = time.NewTicker(time.Millisecond * 250)
	defer ticker.Stop()

	var tick atomic2.Int64
	go func() {
		for range ticker.C {
			tick.Incr()
		}
	}()

	var c = redigoOpenConn(addr, auth)
	defer c.Close()

	var replyChan = make(chan *rdb.DBEntry, 128)

	NewJob(func() {
		defer close(replyChan)
		var db uint64
		for e := range entryChan {
			if on(e) {
				genRestoreCommands(e, db, func(cmd string, args ...interface{}) {
					redigoSendCommand(c, cmd, args...)
					redigoFlushConnIf(c, func() bool {
						switch {
						case tick.Swap(0) != 0:
							return true
						case len(replyChan) == cap(replyChan):
							return true
						default:
							return len(entryChan) == 0
						}
					})
					replyChan <- e.IncrRefCount()
				})
				db = e.DB
			}
			e.DecrRefCount()
		}
		redigoFlushConn(c)
	}).Run()

	NewJob(func() {
		for e := range replyChan {
			e.DecrRefCount()
			redigoGetResponse(c)
		}
	}).RunAndWait()
}

func doRestoreAoflog(reader *bufio2.Reader, addr, auth string, on func(db uint64, cmd string) bool) {
	var ticker = time.NewTicker(time.Millisecond * 100)
	defer ticker.Stop()

	var tick atomic2.Int64
	go func() {
		for range ticker.C {
			tick.Incr()
		}
	}()

	var c = openConn(addr, auth)
	defer c.Close()

	go ioCopyBuffer(ioutil.Discard, c)

	var encoder = redis.NewEncoder(c)
	var decoder = redis.NewDecoderBuffer(reader)
	var db uint64
	for {
		r, err := decoder.Decode()
		if err != nil {
			redisFlushEncoder(encoder)
			log.PanicErrorf(err, "decode command failed")
		}
		if r.Type != redis.TypeArray || len(r.Array) == 0 {
			log.Panicf("invalid command %+v", r)
		}
		var cmd = strings.ToUpper(string(r.Array[0].Value))
		if cmd == "SELECT" {
			if len(r.Array) != 2 {
				log.Panicf("bad select command %+v", r)
			}
			n, err := strconv.ParseInt(string(r.Array[1].Value), 10, 64)
			if err != nil {
				log.PanicErrorf(err, "bad select command %+v", r)
			}
			db = uint64(n)
		}
		if !on(db, cmd) {
			continue
		}
		redisSendCommand(encoder, r, tick.Swap(0) != 0)
	}
}
