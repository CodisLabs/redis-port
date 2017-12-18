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
	"fmt"
	"io"
	"reflect"
	"strconv"
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

func unsafeCastToString(buf unsafe.Pointer, len C.size_t) string {
	var hdr = &reflect.StringHeader{
		Data: uintptr(buf), Len: int(len),
	}
	return *(*string)(unsafe.Pointer(hdr))
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
	var obj = C.redisRioLoadObject(&r.rdb, C.int(typ))
	if obj == nil {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadObject() failed.")
	}
	return &RedisObject{obj}
}

func (r *redisRio) LoadStringObject() *RedisStringObject {
	var obj = C.redisRioLoadStringObject(&r.rdb)
	if obj == nil {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadStringObject() failed.")
	}
	return &RedisStringObject{&RedisObject{obj}}
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

const (
	OBJ_STRING = RedisType(C.OBJ_STRING)
	OBJ_LIST   = RedisType(C.OBJ_LIST)
	OBJ_SET    = RedisType(C.OBJ_SET)
	OBJ_ZSET   = RedisType(C.OBJ_ZSET)
	OBJ_HASH   = RedisType(C.OBJ_HASH)
	OBJ_MODULE = RedisType(C.OBJ_MODULE)
	OBJ_STREAM = RedisType(C.OBJ_STREAM)
)

type RedisType int

func (t RedisType) String() string {
	switch t {
	case OBJ_STRING:
		return "OBJ_STRING"
	case OBJ_LIST:
		return "OBJ_LIST"
	case OBJ_SET:
		return "OBJ_SET"
	case OBJ_ZSET:
		return "OBJ_ZSET"
	case OBJ_HASH:
		return "OBJ_HASH"
	case OBJ_MODULE:
		return "OBJ_MODULE"
	case OBJ_STREAM:
		return "OBJ_STREAM"
	}
	return fmt.Sprintf("OBJ_UNKNOWN[%d]", t)
}

const (
	OBJ_ENCODING_RAW        = RedisEncoding(C.OBJ_ENCODING_RAW)
	OBJ_ENCODING_INT        = RedisEncoding(C.OBJ_ENCODING_INT)
	OBJ_ENCODING_HT         = RedisEncoding(C.OBJ_ENCODING_HT)
	OBJ_ENCODING_ZIPMAP     = RedisEncoding(C.OBJ_ENCODING_ZIPMAP)
	OBJ_ENCODING_LINKEDLIST = RedisEncoding(C.OBJ_ENCODING_LINKEDLIST)
	OBJ_ENCODING_ZIPLIST    = RedisEncoding(C.OBJ_ENCODING_ZIPLIST)
	OBJ_ENCODING_INTSET     = RedisEncoding(C.OBJ_ENCODING_INTSET)
	OBJ_ENCODING_SKIPLIST   = RedisEncoding(C.OBJ_ENCODING_SKIPLIST)
	OBJ_ENCODING_EMBSTR     = RedisEncoding(C.OBJ_ENCODING_EMBSTR)
	OBJ_ENCODING_QUICKLIST  = RedisEncoding(C.OBJ_ENCODING_QUICKLIST)
	OBJ_ENCODING_STREAM     = RedisEncoding(C.OBJ_ENCODING_STREAM)
)

type RedisEncoding int

func (t RedisEncoding) String() string {
	switch t {
	case OBJ_ENCODING_RAW:
		return "ENCODING_RAW"
	case OBJ_ENCODING_INT:
		return "ENCODING_INT"
	case OBJ_ENCODING_HT:
		return "ENCODING_HT"
	case OBJ_ENCODING_ZIPMAP:
		return "ENCODING_ZIPMAP"
	case OBJ_ENCODING_LINKEDLIST:
		return "ENCODING_LINKEDLIST"
	case OBJ_ENCODING_ZIPLIST:
		return "ENCODING_ZIPLIST"
	case OBJ_ENCODING_INTSET:
		return "ENCODING_INTSET"
	case OBJ_ENCODING_SKIPLIST:
		return "ENCODING_SKIPLIST"
	case OBJ_ENCODING_EMBSTR:
		return "ENCODING_EMBSTR"
	case OBJ_ENCODING_QUICKLIST:
		return "ENCODING_QUICKLIST"
	case OBJ_ENCODING_STREAM:
		return "ENCODING_STREAM"
	}
	return fmt.Sprintf("ENCODING_UNKNOWN[%d]", t)
}

type RedisUnsafeSds struct {
	Ptr unsafe.Pointer
	Len int

	LongValue int64
}

func (p *RedisUnsafeSds) Release() {
	if p.Ptr != nil {
		C.redisSdsFree(p.Ptr)
	}
}

func (p *RedisUnsafeSds) String() string {
	if p.Ptr != nil {
		return string(unsafeCastToSlice(p.Ptr, C.size_t(p.Len)))
	}
	return strconv.FormatInt(p.LongValue, 10)
}

func (p *RedisUnsafeSds) UnsafeString() string {
	if p.Ptr != nil {
		return unsafeCastToString(p.Ptr, C.size_t(p.Len))
	}
	return strconv.FormatInt(p.LongValue, 10)
}

type RedisObject struct {
	obj unsafe.Pointer
}

func (o *RedisObject) Type() RedisType {
	return RedisType(C.redisObjectType(o.obj))
}

func (o *RedisObject) Encoding() RedisEncoding {
	return RedisEncoding(C.redisObjectEncoding(o.obj))
}

func (o *RedisObject) RefCount() int {
	return int(C.redisObjectRefCount(o.obj))
}

func (o *RedisObject) IncrRefCount() {
	C.redisObjectIncrRefCount(o.obj)
}

func (o *RedisObject) DecrRefCount() {
	C.redisObjectDecrRefCount(o.obj)
}

func (o *RedisObject) CreateDumpPayload() string {
	var sds = o.CreateDumpPayloadUnsafe()
	var str = sds.String()
	sds.Release()
	return str
}

func (o *RedisObject) CreateDumpPayloadUnsafe() *RedisUnsafeSds {
	var len C.size_t
	var ptr = C.redisObjectCreateDumpPayload(o.obj, &len)
	return &RedisUnsafeSds{ptr, int(len), 0}
}

func DecodeFromPayload(buf []byte) *RedisObject {
	var hdr = (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	var obj = C.redisObjectDecodeFromPayload(unsafe.Pointer(hdr.Data), C.size_t(hdr.Len))
	if obj == nil {
		log.Panicf("Decode From Payload failed.")
	}
	return &RedisObject{obj}
}

type RedisStringObject struct {
	*RedisObject
}
