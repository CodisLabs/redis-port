package rdb

import (
	"encoding/binary"
	"io"
	"strconv"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

type Loader struct {
	r io.Reader

	header struct {
		version int64 // rdb version
	}
	cursor struct {
		db uint64 // current database
	}
	footer struct {
		checksum uint64 // expected checksum
	}
	rio redisRio
}

func NewLoader(r io.Reader) *Loader {
	if r == nil {
		log.Panicf("Create loader with nil reader.")
	}
	l := &Loader{r: r}
	l.rio.init()
	return l
}

func (l *Loader) onRead(b []byte) int {
	_, err := io.ReadFull(l.r, b)
	if err != nil {
		log.PanicErrorf(err, "Read bytes failed.")
	}
	return 1
}

func (l *Loader) Header() {
	header := make([]byte, 9)
	if err := l.rio.Read(header); err != nil {
		log.PanicErrorf(err, "Read RDB header failed.")
	}
	if format := string(header[:5]); format != "REDIS" {
		log.Panicf("Verify magic string, invalid format = '%s'.", format)
	}
	n, err := strconv.ParseInt(string(header[5:]), 10, 64)
	if err != nil {
		log.PanicErrorf(err, "Try to parse version = '%s'.", header[5:])
	}
	switch {
	case n < 1 || n > RDB_VERSION:
		log.Panicf("Can't handle RDB format version = %d.", n)
	default:
		l.header.version = n
	}
}

func (l *Loader) Footer() {
	if l.header.version >= 5 {
		var expected = l.rio.Checksum()
		footer := make([]byte, 8)
		if err := l.rio.Read(footer); err != nil {
			log.PanicErrorf(err, "Read RDB footer failed.")
		}
		l.footer.checksum = binary.LittleEndian.Uint64(footer)
		switch {
		case l.footer.checksum == 0:
			log.Debugf("RDB file was saved with checksum disabled.")
		case l.footer.checksum != expected:
			log.Panicf("Wrong checksum, expected = %#16x, footer = %#16x.", expected, l.footer.checksum)
		}
	}
}

const NoExpireTime = time.Duration(-1)

type DBEntry struct {
	DB     uint64
	Expire time.Duration
	Key    *RedisStringObject
	Value  *RedisObject
}

func (e *DBEntry) Release() {
	if obj := e.Key; obj != nil {
		obj.DecrRefCount()
	}
	if obj := e.Value; obj != nil {
		obj.DecrRefCount()
	}
}

func (l *Loader) Next() *DBEntry {
	for {
		expire := NoExpireTime
		opcode := l.rio.LoadType()
		switch opcode {
		case RDB_OPCODE_EXPIRETIME:
			expire = l.rio.LoadTime()
			opcode = l.rio.LoadType()
		case RDB_OPCODE_EXPIRETIME_MS:
			expire = l.rio.LoadTimeMillisecond()
			opcode = l.rio.LoadType()
		case RDB_OPCODE_EOF:
			return nil
		case RDB_OPCODE_SELECTDB:
			l.cursor.db = l.rio.LoadLen()
			continue
		case RDB_OPCODE_RESIZEDB:
			l.rio.LoadLen()
			l.rio.LoadLen()
			continue
		case RDB_OPCODE_AUX:
			l.rio.LoadStringObject().DecrRefCount()
			l.rio.LoadStringObject().DecrRefCount()
			continue
		}

		switch opcode {
		case RDB_TYPE_MODULE, RDB_TYPE_MODULE_2:
			log.Panicf("Don't support module object yet.")
		case RDB_TYPE_STREAM_LISTPACKS:
			log.Panicf("Don't support stream object yet.")
		}

		return &DBEntry{
			DB:     l.cursor.db,
			Expire: expire,
			Key:    l.rio.LoadStringObject(),
			Value:  l.rio.LoadObject(opcode),
		}
	}
}

func (l *Loader) Scan(onEntry func(e *DBEntry) bool) bool {
	for {
		switch e := l.Next(); {
		case e == nil:
			return false
		case !onEntry(e):
			return true
		}
	}
}
