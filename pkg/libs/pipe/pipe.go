package pipe

import (
	"io"
	"os"
	"sync"

	"github.com/CodisLabs/codis/pkg/utils/errors"
)

type Pipe struct {
	rd, wt struct {
		sync.Mutex
		cond *sync.Cond
		err  error
	}
	mu sync.Mutex

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
	p.rd.cond = sync.NewCond(&p.mu)
	p.wt.cond = sync.NewCond(&p.mu)
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
	p.rd.Lock()
	defer p.rd.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		if p.rd.err != nil {
			return 0, errors.Trace(io.ErrClosedPipe)
		}
		if len(b) == 0 {
			if p.store.Buffered() != 0 {
				return 0, nil
			}
			return 0, p.wt.err
		}
		n, err := p.store.ReadSome(b)
		if err != nil || n != 0 {
			p.wt.cond.Signal()
			return n, err
		}
		if p.wt.err != nil {
			return 0, p.wt.err
		}
		p.rd.cond.Wait()
	}
}

func (p *Pipe) Buffered() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rd.err != nil {
		return 0, p.rd.err
	}
	if n := p.store.Buffered(); n != 0 {
		return n, nil
	} else {
		return 0, p.wt.err
	}
}

func (p *Pipe) CloseReader(err error) error {
	if err == nil {
		err = errors.Trace(io.ErrClosedPipe)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.rd.err == nil {
		p.rd.err = err
	}
	p.rd.cond.Broadcast()
	p.wt.cond.Broadcast()
	return p.store.CloseReader()
}

func (p *Pipe) Writer() Writer {
	return &PipeWriter{p}
}

func (p *Pipe) Write(b []byte) (int, error) {
	p.wt.Lock()
	defer p.wt.Unlock()
	p.mu.Lock()
	defer p.mu.Unlock()
	var nn int
	for {
		if p.wt.err != nil {
			return nn, errors.Trace(io.ErrClosedPipe)
		}
		if p.rd.err != nil {
			return nn, p.rd.err
		}
	again:
		if len(b) == 0 {
			return nn, nil
		}
		n, err := p.store.WriteSome(b)
		if err != nil || n != 0 {
			p.rd.cond.Signal()
			nn, b = nn+n, b[n:]
			if err == nil {
				goto again
			}
			return nn, err
		}
		p.wt.cond.Wait()
	}
}

func (p *Pipe) Available() (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wt.err != nil {
		return 0, p.wt.err
	}
	if p.rd.err != nil {
		return 0, p.rd.err
	}
	return p.store.Available(), nil
}

func (p *Pipe) CloseWriter(err error) error {
	if err == nil {
		err = errors.Trace(io.EOF)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.wt.err == nil {
		p.wt.err = err
	}
	p.rd.cond.Broadcast()
	p.wt.cond.Broadcast()
	return p.store.CloseWriter()
}
