package rdb

import (
	"bytes"
	"reflect"
	"testing"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/utils/assert"
)

func TestRedisSds(t *testing.T) {
	var msg = []byte("hello world!!")
	var hdr = (*reflect.SliceHeader)(unsafe.Pointer(&msg))
	var sds = &RedisSds{
		Ptr: unsafe.Pointer(hdr.Data),
		Len: hdr.Len,
	}
	var s1 = sds.String()
	var p1 = (*reflect.StringHeader)(unsafe.Pointer(&s1))
	assert.Must(s1 == string(msg))
	assert.Must(p1.Data != hdr.Data)

	var s2 = sds.StringUnsafe()
	var p2 = (*reflect.StringHeader)(unsafe.Pointer(&s2))
	assert.Must(s2 == string(msg))
	assert.Must(p2.Data == hdr.Data)

	var s3 = sds.Bytes()
	var p3 = (*reflect.SliceHeader)(unsafe.Pointer(&s3))
	assert.Must(bytes.Equal(s3, msg))
	assert.Must(p3.Data != hdr.Data)

	var s4 = sds.BytesUnsafe()
	var p4 = (*reflect.SliceHeader)(unsafe.Pointer(&s4))
	assert.Must(bytes.Equal(s4, msg))
	assert.Must(p4.Data == hdr.Data)
}
