package rdb

import (
	"C"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

//export cgoRedisLogPanic
func cgoRedisLogPanic(buf unsafe.Pointer, len C.size_t) {
	log.Panicf("In Redis CGo:\n%s", unsafeCastToSlice(buf, len))
}

//export cgoRedisLogLevel
func cgoRedisLogLevel(buf unsafe.Pointer, len C.size_t, level C.int) {
	func() func(format string, args ...interface{}) {
		switch level {
		case 0:
			return log.Debugf
		case 3:
			return log.Warnf
		default:
			return log.Infof
		}
	}()("In Redis CGo: %s", unsafeCastToSlice(buf, len))
}
