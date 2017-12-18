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
	"unsafe"

	"github.com/CodisLabs/codis/pkg/utils/errors"
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

const (
	RDB_VERSION = int64(C.RDB_VERSION)
)
