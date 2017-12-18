package rdb

import "io"

type Loader struct {
	r io.Reader

	rio redisRio

	header struct {
		version int64 // rdb version
	}
	cursor struct {
		db       uint64 // current database
		checksum uint64 // current checksum
		offset   int64  // current offset of the underlying reader
	}
	footer struct {
		checksum uint64 // expected checksum
	}
}
