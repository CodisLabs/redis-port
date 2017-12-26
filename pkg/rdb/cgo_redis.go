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
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

var DefaultLazyfree = false

func init() {
	var buf bytes.Buffer
	for k, v := range map[string]interface{}{
		"hash-max-ziplist-entries": 0,
		"hash-max-ziplist-value":   0,
		"list-compress-depth":      0,
		"list-max-ziplist-size":    0,
		"set-max-intset-entries":   0,
		"zset-max-ziplist-entries": 0,
		"zset-max-ziplist-value":   0,
		"rdbchecksum":              "yes",
		"rdbcompression":           "no",
	} {
		fmt.Fprintf(&buf, "%s %v\n", k, v)
	}
	var cfg = append(buf.Bytes(), byte(0))
	var hdr = (*reflect.StringHeader)(unsafe.Pointer(&cfg))
	C.initRedisServer((*C.char)(unsafe.Pointer(hdr.Data)))
}

type ZmallocMemStats struct {
	MemoryUsed bytesize.Int64
	Rss        bytesize.Int64
	MemorySize bytesize.Int64
}

func ReadZmallocMemStats() *ZmallocMemStats {
	return &ZmallocMemStats{
		MemoryUsed: bytesize.Int64(C.zmalloc_used_memory()),
		Rss:        bytesize.Int64(C.zmalloc_get_rss()),
		MemorySize: bytesize.Int64(C.zmalloc_get_memory_size()),
	}
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

func unsafeCastToLoader(rio *C.redisRio) *Loader {
	var l *Loader
	var ptr = uintptr(unsafe.Pointer(rio)) -
		(unsafe.Offsetof(l.rio) + unsafe.Offsetof(l.rio.rio))
	return (*Loader)(unsafe.Pointer(ptr))
}

//export onRedisRioRead
func onRedisRioRead(rio *C.redisRio, buf unsafe.Pointer, len C.size_t) C.size_t {
	loader, buffer := unsafeCastToLoader(rio), unsafeCastToSlice(buf, len)
	n, err := loader.r.Read(buffer)
	if err != nil {
		log.PanicErrorf(err, "Read bytes failed.")
	}
	return C.size_t(n)
}

type redisRio struct {
	rio C.redisRio
}

func (r *redisRio) init() {
	C.redisRioInit(&r.rio)
}

func (r *redisRio) Read(b []byte) error {
	var hdr = (*reflect.SliceHeader)(unsafe.Pointer(&b))
	var ret = C.redisRioRead(&r.rio, unsafe.Pointer(hdr.Data), C.size_t(hdr.Cap))
	if ret != 0 {
		return errors.Trace(io.ErrUnexpectedEOF)
	}
	return nil
}

func (r *redisRio) Checksum() uint64 {
	return uint64(C.redisRioChecksum(&r.rio))
}

func (r *redisRio) LoadLen() uint64 {
	var len C.uint64_t
	var ret = C.redisRioLoadLen(&r.rio, &len)
	if ret != 0 {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadLen() failed")
	}
	return uint64(len)
}

func (r *redisRio) LoadType() int {
	var typ C.int
	var ret = C.redisRioLoadType(&r.rio, &typ)
	if ret != 0 {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadType() failed.")
	}
	return int(typ)
}

func (r *redisRio) LoadTime() time.Duration {
	var val C.time_t
	var ret = C.redisRioLoadTime(&r.rio, &val)
	if ret != 0 {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadTime() failed.")
	}
	return time.Duration(val) * time.Second
}

func (r *redisRio) LoadTimeMillisecond() time.Duration {
	var val C.longlong
	var ret = C.redisRioLoadTimeMillisecond(&r.rio, &val)
	if ret != 0 {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadTimeMillisecond() failed.")
	}
	return time.Duration(val) * time.Millisecond
}

func (r *redisRio) LoadObject(typ int) *RedisObject {
	var obj = C.redisRioLoadObject(&r.rio, C.int(typ))
	if obj == nil {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadObject() failed.")
	}
	return newRedisObject(obj)
}

func (r *redisRio) LoadStringObject() *RedisStringObject {
	var obj = C.redisRioLoadStringObject(&r.rio)
	if obj == nil {
		log.PanicErrorf(io.ErrUnexpectedEOF, "Read RDB LoadStringObject() failed.")
	}
	return &RedisStringObject{newRedisObject(obj)}
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

type RedisSds struct {
	Ptr   unsafe.Pointer
	Len   int
	Value int64
	Score float64

	IsLeak bool
}

func (p *RedisSds) Release() {
	if p.IsLeak && p.Ptr != nil {
		C.redisSdsFreePtr(p.Ptr)
	}
}

func (p *RedisSds) IsPointer() bool {
	return p.Ptr != nil
}

func (p *RedisSds) IsInteger() bool {
	return p.Ptr == nil
}

func (p *RedisSds) String() string {
	if p.IsInteger() {
		return strconv.FormatInt(p.Value, 10)
	}
	var slice = unsafeCastToSlice(p.Ptr, C.size_t(p.Len))
	return string(slice)
}

func (p *RedisSds) StringUnsafe() string {
	if p.IsInteger() {
		return strconv.FormatInt(p.Value, 10)
	}
	return unsafeCastToString(p.Ptr, C.size_t(p.Len))
}

type RedisObject struct {
	obj unsafe.Pointer

	lazyfree C.int
	refcount atomic2.Int64
}

func newRedisObject(obj unsafe.Pointer) *RedisObject {
	o := &RedisObject{obj: obj}
	o.refcount.Set(1)
	return o.SetLazyfree(DefaultLazyfree)
}

func (o *RedisObject) Type() RedisType {
	return RedisType(C.redisObjectType(o.obj))
}

func (o *RedisObject) Encoding() RedisEncoding {
	return RedisEncoding(C.redisObjectEncoding(o.obj))
}

func (o *RedisObject) IsEncodedObject() bool {
	switch o.Encoding() {
	default:
		return true
	case OBJ_ENCODING_QUICKLIST, OBJ_ENCODING_HT, OBJ_ENCODING_SKIPLIST:
		return false
	}
}

func (o *RedisObject) SetLazyfree(enable bool) *RedisObject {
	if enable {
		o.lazyfree = C.int(1)
	} else {
		o.lazyfree = C.int(0)
	}
	return o
}

func (o *RedisObject) RefCount() int {
	return o.refcount.AsInt()
}

func (o *RedisObject) IncrRefCount() *RedisObject {
	switch after := o.refcount.Incr(); {
	case after <= 1:
		fallthrough
	case after > 1024:
		log.Panicf("Invalid IncrRefCount - [%d]", after-1)
	}
	return o
}

func (o *RedisObject) DecrRefCount() {
	switch after := o.refcount.Decr(); {
	case after == 0:
		C.redisObjectDecrRefCount(o.obj, o.lazyfree)
	case after < 0:
		log.Panicf("Invalid DecrRefCount - [%d]", after+1)
	}
}

func (o *RedisObject) CreateDumpPayload() string {
	var sds = o.CreateDumpPayloadUnsafe()
	var str = sds.String()
	sds.Release()
	return str
}

func (o *RedisObject) CreateDumpPayloadUnsafe() *RedisSds {
	var sds C.redisSds
	C.redisObjectCreateDumpPayload(o.obj, &sds)
	return &RedisSds{Ptr: sds.ptr, Len: int(sds.len), IsLeak: true}
}

func DecodeFromPayload(buf []byte) *RedisObject {
	var hdr = (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	var obj = C.redisObjectDecodeFromPayload(unsafe.Pointer(hdr.Data), C.size_t(hdr.Len))
	if obj == nil {
		log.Panicf("Decode From Payload failed.")
	}
	return newRedisObject(obj)
}

func (o *RedisObject) IsString() bool {
	return o.Type() == OBJ_STRING
}

func (o *RedisObject) AsString() *RedisStringObject {
	return &RedisStringObject{o}
}

func (o *RedisObject) IsList() bool {
	return o.Type() == OBJ_LIST
}

func (o *RedisObject) AsList() *RedisListObject {
	return &RedisListObject{o}
}

func (o *RedisObject) IsHash() bool {
	return o.Type() == OBJ_HASH
}

func (o *RedisObject) AsHash() *RedisHashObject {
	return &RedisHashObject{o}
}

func (o *RedisObject) IsZset() bool {
	return o.Type() == OBJ_ZSET
}

func (o *RedisObject) AsZset() *RedisZsetObject {
	return &RedisZsetObject{o}
}

func (o *RedisObject) IsSet() bool {
	return o.Type() == OBJ_SET
}

func (o *RedisObject) AsSet() *RedisSetObject {
	return &RedisSetObject{o}
}

type RedisStringObject struct {
	*RedisObject
}

func (o *RedisStringObject) Len() int {
	return int(C.redisStringObjectLen(o.obj))
}

func (o *RedisStringObject) loadRedisSds() *RedisSds {
	var sds C.redisSds
	C.redisStringObjectLoad(o.obj, &sds)
	return &RedisSds{Ptr: sds.ptr, Len: int(sds.len), Value: int64(sds.val)}
}

func (o *RedisStringObject) String() string {
	return o.loadRedisSds().String()
}

func (o *RedisStringObject) StringUnsafe() string {
	return o.loadRedisSds().StringUnsafe()
}

type RedisListObject struct {
	*RedisObject
}

func (o *RedisListObject) Len() int {
	return int(C.redisListObjectLen(o.obj))
}

func (o *RedisListObject) NewIterator() *RedisListIterator {
	return newRedisListIterator(o)
}

func (o *RedisListObject) ForEach(on func(iter *RedisListIterator) bool) int {
	var step int
	var iter = o.NewIterator()
	for on(iter) {
		step++
	}
	iter.Release()
	return step
}

func (o *RedisListObject) Strings() []string {
	var list []string
	o.ForEach(func(iter *RedisListIterator) bool {
		var key = iter.Next()
		if key == nil {
			return false
		}
		list = append(list, key.String())
		return true
	})
	return list
}

func (o *RedisListObject) StringsUnsafe() []string {
	var list []string
	o.ForEach(func(iter *RedisListIterator) bool {
		var key = iter.Next()
		if key == nil {
			return false
		}
		list = append(list, key.StringUnsafe())
		return true
	})
	return list
}

func redisTypeIteratorHasNext(p *C.redisTypeIterator) bool {
	var empty = func() bool { return p.index == p.slice.len }
	if !empty() {
		return true
	} else {
		C.redisTypeIteratorLoad(p)
		return !empty()
	}
}

func redisTypeIteratorNext(p *C.redisTypeIterator) *RedisSds {
	if !redisTypeIteratorHasNext(p) {
		return nil
	}
	var first *C.redisSds
	first, p.index = &p.slice.buf[p.index], p.index+1
	return &RedisSds{Ptr: first.ptr, Len: int(first.len), Value: int64(first.val), Score: float64(first.score)}
}

type RedisListIterator struct {
	iter *C.redisTypeIterator
	robj *RedisObject
}

func newRedisListIterator(o *RedisListObject) *RedisListIterator {
	return &RedisListIterator{
		iter: C.redisListObjectNewIterator(o.obj),
		robj: o.IncrRefCount(),
	}
}

func (p *RedisListIterator) Release() {
	C.redisListIteratorRelease(p.iter)
	p.robj.DecrRefCount()
}

func (p *RedisListIterator) Next() *RedisSds {
	return redisTypeIteratorNext(p.iter)
}

type RedisHashObject struct {
	*RedisObject
}

func (o *RedisHashObject) Len() int {
	return int(C.redisHashObjectLen(o.obj))
}

func (o *RedisHashObject) NewIterator() *RedisHashIterator {
	return newRedisHashIterator(o)
}

func (o *RedisHashObject) ForEach(on func(iter *RedisHashIterator) bool) int {
	var step int
	var iter = o.NewIterator()
	for on(iter) {
		step++
	}
	iter.Release()
	return step
}

func (o *RedisHashObject) Map() map[string]string {
	var hash = make(map[string]string)
	o.ForEach(func(iter *RedisHashIterator) bool {
		var key, value = iter.Next()
		if key == nil {
			return false
		}
		hash[key.String()] = value.String()
		return true
	})
	return hash
}

func (o *RedisHashObject) MapUnsafe() map[string]string {
	var hash = make(map[string]string)
	o.ForEach(func(iter *RedisHashIterator) bool {
		var key, value = iter.Next()
		if key == nil {
			return false
		}
		hash[key.StringUnsafe()] = value.StringUnsafe()
		return true
	})
	return hash
}

type RedisHashIterator struct {
	iter *C.redisTypeIterator
	robj *RedisObject
}

func newRedisHashIterator(o *RedisHashObject) *RedisHashIterator {
	return &RedisHashIterator{
		iter: C.redisHashObjectNewIterator(o.obj),
		robj: o.IncrRefCount(),
	}
}

func (p *RedisHashIterator) Release() {
	C.redisHashIteratorRelease(p.iter)
	p.robj.DecrRefCount()
}

func (p *RedisHashIterator) Next() (*RedisSds, *RedisSds) {
	var key = redisTypeIteratorNext(p.iter)
	if key != nil {
		return key, redisTypeIteratorNext(p.iter)
	}
	return nil, nil
}

type RedisZsetObject struct {
	*RedisObject
}

func (o *RedisZsetObject) Len() int {
	return int(C.redisZsetObjectLen(o.obj))
}

func (o *RedisZsetObject) NewIterator() *RedisZsetIterator {
	return newRedisZsetIterator(o)
}

func (o *RedisZsetObject) ForEach(on func(iter *RedisZsetIterator) bool) int {
	var step int
	var iter = o.NewIterator()
	for on(iter) {
		step++
	}
	iter.Release()
	return step
}

func (o *RedisZsetObject) Map() map[string]float64 {
	var zset = make(map[string]float64)
	o.ForEach(func(iter *RedisZsetIterator) bool {
		var key = iter.Next()
		if key == nil {
			return false
		}
		zset[key.String()] = key.Score
		return true
	})
	return zset
}

func (o *RedisZsetObject) MapUnsafe() map[string]float64 {
	var zset = make(map[string]float64)
	o.ForEach(func(iter *RedisZsetIterator) bool {
		var key = iter.Next()
		if key == nil {
			return false
		}
		zset[key.StringUnsafe()] = key.Score
		return true
	})
	return zset
}

type RedisZsetIterator struct {
	iter *C.redisTypeIterator
	robj *RedisObject
}

func newRedisZsetIterator(o *RedisZsetObject) *RedisZsetIterator {
	return &RedisZsetIterator{
		iter: C.redisZsetObjectNewIterator(o.obj),
		robj: o.IncrRefCount(),
	}
}

func (p *RedisZsetIterator) Release() {
	C.redisZsetIteratorRelease(p.iter)
	p.robj.DecrRefCount()
}

func (p *RedisZsetIterator) Next() *RedisSds {
	return redisTypeIteratorNext(p.iter)
}

type RedisSetObject struct {
	*RedisObject
}

func (o *RedisSetObject) Len() int {
	return int(C.redisSetObjectLen(o.obj))
}

func (o *RedisSetObject) ForEach(on func(iter *RedisSetIterator) bool) int {
	var step int
	var iter = o.NewIterator()
	for on(iter) {
		step++
	}
	iter.Release()
	return step
}

func (o *RedisSetObject) Map() map[string]bool {
	var set = make(map[string]bool)
	o.ForEach(func(iter *RedisSetIterator) bool {
		var key = iter.Next()
		if key == nil {
			return false
		}
		set[key.String()] = true
		return true
	})
	return set
}

func (o *RedisSetObject) MapUnsafe() map[string]bool {
	var set = make(map[string]bool)
	o.ForEach(func(iter *RedisSetIterator) bool {
		var key = iter.Next()
		if key == nil {
			return false
		}
		set[key.StringUnsafe()] = true
		return true
	})
	return set
}

func (o *RedisSetObject) NewIterator() *RedisSetIterator {
	return newRedisSetIterator(o)
}

type RedisSetIterator struct {
	iter *C.redisTypeIterator
	robj *RedisObject
}

func newRedisSetIterator(o *RedisSetObject) *RedisSetIterator {
	return &RedisSetIterator{
		iter: C.redisSetObjectNewIterator(o.obj),
		robj: o.IncrRefCount(),
	}
}

func (p *RedisSetIterator) Release() {
	C.redisSetIteratorRelease(p.iter)
	p.robj.DecrRefCount()
}

func (p *RedisSetIterator) Next() *RedisSds {
	return redisTypeIteratorNext(p.iter)
}
