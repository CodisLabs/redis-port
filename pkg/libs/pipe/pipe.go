package pipe

import "os"

type Pipe struct {
}

func NewPipe() *Pipe {
	return NewPipeSize(defaultMemBufferSize)
}

func NewPipeSize(size int) *Pipe {
	return newPipe(newMemBufferSize(size))
}

func NewPipeFile(file *os.File, size int) *Pipe {
	return newPipe(newFileBufferSize(file, size))
}

func newPipe(store Buffer) *Pipe {
	panic("TODO")
}

func (p *Pipe) Close() {
	p.CloseReader(nil)
	p.CloseWriter(nil)
}

func (p *Pipe) Reader() Reader {
	return &PipeReader{p}
}

func (p *Pipe) Read(b []byte) (int, error) {
	panic("TODO")
}

func (p *Pipe) Buffered() (int, error) {
	panic("TODO")
}

func (p *Pipe) CloseReader(err error) error {
	panic("TODO")
}

func (p *Pipe) Writer() Writer {
	return &PipeWriter{p}
}

func (p *Pipe) Write(b []byte) (int, error) {
	panic("TODO")
}

func (p *Pipe) Available() (int, error) {
	panic("TODO")
}

func (p *Pipe) CloseWriter(err error) error {
	panic("TODO")
}
