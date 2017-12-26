package pipe

import (
	"io"
	"os"
)

type Reader interface {
	io.ReadCloser
	Buffered() (int, error)
	CloseWithError(err error) error
}

type Writer interface {
	io.WriteCloser
	Available() (int, error)
	CloseWithError(err error) error
}

type PipeReader struct {
	*Pipe
}

func (p *PipeReader) Close() error {
	return p.CloseReader(nil)
}

func (p *PipeReader) CloseWithError(err error) error {
	return p.CloseReader(err)
}

type PipeWriter struct {
	*Pipe
}

func (p *PipeWriter) Close() error {
	return p.CloseWriter(nil)
}

func (p *PipeWriter) CloseWithError(err error) error {
	return p.CloseWriter(err)
}

func New() (Reader, Writer) {
	return NewSize(defaultMemBufferSize)
}

func NewSize(size int) (Reader, Writer) {
	var p = NewPipeSize(size)
	return p.Reader(), p.Writer()
}

func NewFile(file *os.File, size int) (Reader, Writer) {
	var p = NewPipeFile(file, size)
	return p.Reader(), p.Writer()
}
