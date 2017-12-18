package rdb

// #cgo         CFLAGS: -I.
// #cgo         CFLAGS: -I../../third_party/
// #cgo         CFLAGS: -I../../third_party/redis/deps/lua/src/
// #cgo         CFLAGS: -std=c99 -pedantic -O2
// #cgo         CFLAGS: -Wall -W -Wno-missing-field-initializers
// #cgo         CFLAGS: -D_REENTRANT
// #cgo linux   CFLAGS: -D_POSIX_C_SOURCE=199309L
// #cgo        LDFLAGS: -lm
// #cgo linux   CFLAGS: -I../../third_party/jemalloc/include/
// #cgo linux   CFLAGS: -DUSE_JEMALLOC
// #cgo linux  LDFLAGS: -lrt
// #cgo linux  LDFLAGS: -L../../third_party/jemalloc/lib/ -ljemalloc_pic
//
// #include "cgo_redis.h"
//
import "C"

import (
	"io"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

const redisServerConfig = `
hash-max-ziplist-entries 512
hash-max-ziplist-value 64
list-compress-depth 0
list-max-ziplist-size -2
set-max-intset-entries 512
zset-max-ziplist-entries 128
zset-max-ziplist-value 64
rdbchecksum yes
rdbcompression yes
`

func init() {
	var buf = strings.TrimSpace(redisServerConfig)
	var hdr = (*reflect.StringHeader)(unsafe.Pointer(&buf))
	C.initRedisServer(unsafe.Pointer(hdr.Data), C.size_t(hdr.Len))
}

func unsafeCastToLoader(rdb *C.rio) *Loader {
	var l *Loader
	var ptr = uintptr(unsafe.Pointer(rdb)) -
		(unsafe.Offsetof(l.rio) + unsafe.Offsetof(l.rio.rdb))
	return (*Loader)(unsafe.Pointer(ptr))
}

func unsafeCastToSlice(buf unsafe.Pointer, len C.size_t) []byte {
	var hdr = &reflect.SliceHeader{
		Data: uintptr(buf), Len: int(len), Cap: int(len),
	}
	return *(*[]byte)(unsafe.Pointer(hdr))
}

//export cgoRedisRioRead
func cgoRedisRioRead(rdb *C.rio, buf unsafe.Pointer, len C.size_t) C.size_t {
	loader, buffer := unsafeCastToLoader(rdb), unsafeCastToSlice(buf, len)
	return C.size_t(loader.onRead(buffer))
}

//export cgoRedisRioWrite
func cgoRedisRioWrite(rdb *C.rio, buf unsafe.Pointer, len C.size_t) C.size_t {
	loader, buffer := unsafeCastToLoader(rdb), unsafeCastToSlice(buf, len)
	return C.size_t(loader.onWrite(buffer))
}

//export cgoRedisRioTell
func cgoRedisRioTell(rdb *C.rio) C.off_t {
	loader := unsafeCastToLoader(rdb)
	return C.off_t(loader.onTell())
}

//export cgoRedisRioFlush
func cgoRedisRioFlush(rdb *C.rio) C.int {
	loader := unsafeCastToLoader(rdb)
	return C.int(loader.onFlush())
}

//export cgoRedisRioUpdateChecksum
func cgoRedisRioUpdateChecksum(rdb *C.rio, checksum C.uint64_t) {
	loader := unsafeCastToLoader(rdb)
	loader.onUpdateChecksum(uint64(checksum))
}

type redisRio struct {
	rdb C.rio
}

func (r *redisRio) init() {
	C.redisRioInit(&r.rdb)
}

func (r *redisRio) Read(b []byte) error {
	var hdr = (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var ret = C.redisRioRead(&r.rdb, unsafe.Pointer(hdr.Data), C.size_t(hdr.Cap))
	if ret != 0 {
		return errors.Trace(io.ErrUnexpectedEOF)
	}
	return nil
}

func (r *redisRio) LoadLen() uint64 {
	var len C.uint64_t
	var ret = C.redisRioLoadLen(&r.rdb, &len)
	if ret != 0 {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadLen() failed")
	}
	return uint64(len)
}

func (r *redisRio) LoadType() int {
	var typ C.int
	var ret = C.redisRioLoadType(&r.rdb, &typ)
	if ret != 0 {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadType() failed.")
	}
	return int(typ)
}

func (r *redisRio) LoadTime() time.Duration {
	var val C.time_t
	var ret = C.redisRioLoadTime(&r.rdb, &val)
	if ret != 0 {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadTime() failed.")
	}
	return time.Duration(val) * time.Second
}

func (r *redisRio) LoadTimeMillisecond() time.Duration {
	var val C.longlong
	var ret = C.redisRioLoadTimeMillisecond(&r.rdb, &val)
	if ret != 0 {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadTimeMillisecond() failed.")
	}
	return time.Duration(val) * time.Millisecond
}

func (r *redisRio) LoadObject(typ int) *RedisObject {
	panic("TODO")
}

func (r *redisRio) LoadStringObject() *RedisStringObject {
	panic("TODO")
}

const (
	RDB_VERSION = int64(C.RDB_VERSION)
)

const (
	RDB_OPCODE_AUX           = int(C.RDB_OPCODE_AUX)
	RDB_OPCODE_EOF           = int(C.RDB_OPCODE_EOF)
	RDB_OPCODE_EXPIRETIME    = int(C.RDB_OPCODE_EXPIRETIME)
	RDB_OPCODE_EXPIRETIME_MS = int(C.RDB_OPCODE_EXPIRETIME_MS)
	RDB_OPCODE_RESIZEDB      = int(C.RDB_OPCODE_RESIZEDB)
	RDB_OPCODE_SELECTDB      = int(C.RDB_OPCODE_SELECTDB)

	RDB_TYPE_STRING           = int(C.RDB_TYPE_STRING)
	RDB_TYPE_LIST             = int(C.RDB_TYPE_LIST)
	RDB_TYPE_SET              = int(C.RDB_TYPE_SET)
	RDB_TYPE_ZSET             = int(C.RDB_TYPE_ZSET)
	RDB_TYPE_HASH             = int(C.RDB_TYPE_HASH)
	RDB_TYPE_ZSET_2           = int(C.RDB_TYPE_ZSET_2)
	RDB_TYPE_MODULE           = int(C.RDB_TYPE_MODULE)
	RDB_TYPE_MODULE_2         = int(C.RDB_TYPE_MODULE_2)
	RDB_TYPE_HASH_ZIPMAP      = int(C.RDB_TYPE_HASH_ZIPMAP)
	RDB_TYPE_LIST_ZIPLIST     = int(C.RDB_TYPE_LIST_ZIPLIST)
	RDB_TYPE_SET_INTSET       = int(C.RDB_TYPE_SET_INTSET)
	RDB_TYPE_ZSET_ZIPLIST     = int(C.RDB_TYPE_ZSET_ZIPLIST)
	RDB_TYPE_HASH_ZIPLIST     = int(C.RDB_TYPE_HASH_ZIPLIST)
	RDB_TYPE_LIST_QUICKLIST   = int(C.RDB_TYPE_LIST_QUICKLIST)
	RDB_TYPE_STREAM_LISTPACKS = int(C.RDB_TYPE_STREAM_LISTPACKS)
)

type RedisObject struct {
}

func (o *RedisObject) DecrRefCount() {
	panic("todo")
}

type RedisStringObject struct {
	*RedisObject
}
