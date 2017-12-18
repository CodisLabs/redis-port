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
	n, err := l.r.Read(b)
	if err != nil {
		log.PanicErrorf(err, "Read bytes failed.")
	}
	l.cursor.offset += int64(n)
	return n
}

func (l *Loader) onWrite(b []byte) int {
	log.Panicf("Doesn't support write operation.")
	return 0
}

func (l *Loader) onTell() int64 {
	return l.cursor.offset
}

func (l *Loader) onFlush() int {
	log.Panicf("Doesn't support flush operation.")
	return 0
}

func (l *Loader) onUpdateChecksum(checksum uint64) {
	l.cursor.checksum = checksum
}

func (l *Loader) Header() {
	panic("TODO")
}

func (l *Loader) Footer() {
	panic("TODO")
}

func (l *Loader) Next() interface{} {
	panic("TODO")
}
