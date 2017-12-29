package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

	"github.com/CodisLabs/redis-port/pkg/rdb"
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

func authenticate(c net.Conn, auth string) {
	if auth == "" {
		return
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

func redisSendPsyncContinue(enc *redis.Encoder, dec *redis.Decoder, runid string, offset int64) {
	var cmd = redisNewCommand("PSYNC", runid, offset+1)
	redisSendCommand(enc, cmd, true)
	reply := redisRespAsString(redisGetResponse(dec))
	split := strings.Split(reply, " ")
	if len(split) != 1 || strings.ToLower(split[0]) != "continue" {
		log.Panicf("psync response = %q", reply)
	}
}

func redisSendReplAck(enc *redis.Encoder, offset int64) {
	var cmd = redisNewCommand("REPLCONF", "ack", offset)
	redisSendCommand(enc, cmd, true)
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

func openConn(addr, auth string) net.Conn {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		log.PanicErrorf(err, "cannot connect to %q", addr)
	}
	authenticate(c, auth)
	return c
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
