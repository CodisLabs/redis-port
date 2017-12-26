package pipe

import (
	"io"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

const (
	memBufferPageSize    = 1024 * 4
	defaultMemBufferSize = 1024 * 1024 * 8
)

type memBuffer struct {
	buf []byte

	rpos uint64
	wpos uint64
}

func newMemBuffer() *memBuffer {
	return newMemBufferSize(defaultMemBufferSize)
}

func newMemBufferSize(size int) *memBuffer {
	if size < defaultMemBufferSize {
		size = defaultMemBufferSize
	} else {
		size = align(size, memBufferPageSize)
	}
	return &memBuffer{buf: make([]byte, size)}
}

func (p *memBuffer) ReadSome(b []byte) (int, error) {
	if len(p.buf) == 0 {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	n, offset := roffset(len(b), len(p.buf), p.rpos, p.wpos)
	copy(b[:n], p.buf[offset:])

	p.rpos += uint64(n)
	if p.rpos == p.wpos {
		p.rpos = 0
		p.wpos = 0
	}
	return n, nil
}

func (p *memBuffer) Buffered() int {
	if len(p.buf) == 0 {
		return 0
	}
	return int(p.wpos - p.rpos)
}

func (p *memBuffer) CloseReader() error {
	p.buf = nil
	return nil
}

func (p *memBuffer) WriteSome(b []byte) (int, error) {
	if len(p.buf) == 0 {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	n, offset := woffset(len(b), len(p.buf), p.rpos, p.wpos)
	copy(p.buf[offset:], b[:n])

	p.wpos += uint64(n)
	return n, nil
}

func (p *memBuffer) Available() int {
	if len(p.buf) == 0 {
		return 0
	}
	return len(p.buf) - int(p.wpos-p.rpos)
}

func (p *memBuffer) CloseWriter() error {
	return nil
}
