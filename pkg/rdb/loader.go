package rdb

import (
	"io"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

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

func NewLoader(r io.Reader) *Loader {
	if r == nil {
		log.Panicf("Create loader with nil reader.")
	}
	l := &Loader{r: r}
	l.rio.init()
	return l
}

func (l *Loader) onRead(b []byte) int {
	panic("TODO")
}

func (l *Loader) onWrite(b []byte) int {
	panic("TODO")
}

func (l *Loader) onTell() int64 {
	panic("TODO")
}

func (l *Loader) onFlush() int {
	panic("TODO")
}

func (l *Loader) onUpdateChecksum(checksum uint64) {
	panic("TODO")
}
