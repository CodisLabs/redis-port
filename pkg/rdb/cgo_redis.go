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
import "unsafe"

type redisRio struct {
	rdb C.rio
}

func (r *redisRio) init() {
	C.redisRioInit(&r.rdb)
}

//export cgoRedisRioRead
func cgoRedisRioRead(rdb *C.rio, buf unsafe.Pointer, len C.size_t) C.size_t {
	panic("TODO")
}

//export cgoRedisRioWrite
func cgoRedisRioWrite(rdb *C.rio, buf unsafe.Pointer, len C.size_t) C.size_t {
	panic("TODO")
}

//export cgoRedisRioTell
func cgoRedisRioTell(rdb *C.rio) C.off_t {
	panic("TODO")
}

//export cgoRedisRioFlush
func cgoRedisRioFlush(rdb *C.rio) C.int {
	panic("TODO")
}

//export cgoRedisRioUpdateChecksum
func cgoRedisRioUpdateChecksum(rdb *C.rio, checksum C.uint64_t) {
	panic("TODO")
}

const (
	C_OK = C.C_OK
)

const (
	RDB_VERSION = int64(C.RDB_VERSION)
)
