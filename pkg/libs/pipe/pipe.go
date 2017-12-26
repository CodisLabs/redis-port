package pipe

import (
	"io"
	"os"
	"sync"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

type Pipe struct {
	r, w struct {
		sync.Mutex
		cond *sync.Cond
	}
	mu sync.Mutex

	rerr, werr error

	store Buffer
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
	p := &Pipe{store: store}
	p.r.cond = sync.NewCond(&p.mu)
	p.w.cond = sync.NewCond(&p.mu)
	return p
}

func (p *Pipe) Close() {
	p.CloseReader(nil)
	p.CloseWriter(nil)
}

func (p *Pipe) Reader() Reader {
	return &PipeReader{p}
}

func (p *Pipe) Read(b []byte) (int, error) {
	p.r.Lock()
	defer p.r.Unlock()
	for {
		n, err := p.readSome(b)
		if err != nil || n != 0 {
			return n, err
		}
		if len(b) == 0 {
			return 0, nil
		}
	}
}

func (p *Pipe) readSome(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rerr != nil {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	if len(b) == 0 {
		if p.store.Buffered() != 0 {
			return 0, nil
		}
		return 0, p.werr
	}
	n, err := p.store.ReadSome(b)
	if err != nil || n != 0 {
		p.w.cond.Signal()
		return n, err
	}
	if p.werr != nil {
		return 0, p.werr
	} else {
		p.r.cond.Wait()
		return 0, nil
	}
}

func (p *Pipe) Buffered() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rerr != nil {
		return 0, p.rerr
	}
	if n := p.store.Buffered(); n != 0 {
		return n, nil
	} else {
		return 0, p.werr
	}
}

func (p *Pipe) CloseReader(err error) error {
	if err == nil {
		err = errors.Trace(io.ErrClosedPipe)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rerr == nil {
		p.rerr = err
	}
	p.r.cond.Broadcast()
	p.w.cond.Broadcast()
	return p.store.CloseReader()
}

func (p *Pipe) Writer() Writer {
	return &PipeWriter{p}
}

func (p *Pipe) Write(b []byte) (int, error) {
	p.w.Lock()
	defer p.w.Unlock()
	var nn int
	for {
		n, err := p.writeSome(b)
		if err != nil || n == len(b) {
			return nn + n, err
		}
		nn, b = nn+n, b[n:]
	}
}

func (p *Pipe) writeSome(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.werr != nil {
		return 0, errors.Trace(io.ErrClosedPipe)
	}
	if p.rerr != nil {
		return 0, p.rerr
	}
	if len(b) == 0 {
		return 0, nil
	}
	n, err := p.store.WriteSome(b)
	if err != nil || n != 0 {
		p.r.cond.Signal()
		return n, err
	} else {
		p.w.cond.Wait()
		return 0, nil
	}
}

func (p *Pipe) Available() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.werr != nil {
		return 0, p.werr
	}
	if p.rerr != nil {
		return 0, p.rerr
	}
	return p.store.Available(), nil
}

func (p *Pipe) CloseWriter(err error) error {
	if err == nil {
		err = errors.Trace(io.EOF)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.werr == nil {
		p.werr = err
	}
	p.r.cond.Broadcast()
	p.w.cond.Broadcast()
	return p.store.CloseWriter()
}
