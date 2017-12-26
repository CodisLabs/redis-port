package pipe

import "io"

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
