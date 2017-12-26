package pipe

import (
	"io"
	"os"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

const (
	fileBufferPageSize    = 1024 * 1024 * 4
	defaultFileBufferSize = 1024 * 1024 * 256
)

type fileBuffer struct {
	file *os.File
	size int
	rpos uint64
	wpos uint64
}

func newFileBuffer(file *os.File) *fileBuffer {
	return newFileBufferSize(file, defaultFileBufferSize)
}

func newFileBufferSize(file *os.File, size int) *fileBuffer {
	if size < defaultFileBufferSize {
		size = defaultFileBufferSize
	} else {
		size = align(size, fileBufferPageSize)
	}
	return &fileBuffer{file: file, size: size}
}

func (p *fileBuffer) ReadSome(b []byte) (int, error) {
	if p.file == nil {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	n, offset := roffset(len(b), p.size, p.rpos, p.wpos)
	nn, err := p.file.ReadAt(b[:n], int64(offset))

	p.rpos += uint64(nn)
	if p.rpos == p.wpos {
		p.rpos = 0
		p.wpos = 0
		if err == nil {
			err = p.file.Truncate(0)
		}
	}
	return nn, errors.Trace(err)
}

func (p *fileBuffer) Buffered() int {
	if p.file == nil {
		return 0
	}
	return int(p.wpos - p.rpos)
}

func (p *fileBuffer) CloseReader() error {
	if f := p.file; f != nil {
		p.file = nil
		return errors.Trace(f.Truncate(0))
	}
	return nil
}

func (p *fileBuffer) WriteSome(b []byte) (int, error) {
	if p.file == nil {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	n, offset := woffset(len(b), p.size, p.rpos, p.wpos)
	nn, err := p.file.WriteAt(b[:n], int64(offset))

	p.wpos += uint64(nn)
	return nn, errors.Trace(err)
}

func (p *fileBuffer) Available() int {
	if p.file == nil {
		return 0
	}
	return p.size - int(p.wpos-p.rpos)
}

func (p *fileBuffer) CloseWriter() error {
	return nil
}
