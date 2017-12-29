package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"

	"github.com/CodisLabs/redis-port/pkg/rdb"
)

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
