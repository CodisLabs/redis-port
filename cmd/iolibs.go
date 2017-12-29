package main

import (
	"bufio"
	"io"
	"sync"

	"github.com/CodisLabs/codis/pkg/utils/bufio2"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type ReaderBuilder struct {
	io.Reader
}

func rBuilder(r io.Reader) *ReaderBuilder {
	return &ReaderBuilder{r}
}

func (b *ReaderBuilder) Must() *ReaderBuilder {
	b.Reader = &MustReader{b.Reader}
	return b
}

func (b *ReaderBuilder) Count(p *atomic2.Int64) *ReaderBuilder {
	b.Reader = &CountReader{b.Reader, p}
	return b
}

func (b *ReaderBuilder) Limit(n int64) *ReaderBuilder {
	b.Reader = io.LimitReader(b.Reader, n)
	return b
}

func (b *ReaderBuilder) Buffer(size int) *ReaderBuilder {
	b.Reader = bufio.NewReaderSize(b.Reader, size)
	return b
}

func (b *ReaderBuilder) Buffer2(size int) *ReaderBuilder {
	b.Reader = bufio2.NewReaderSize(b.Reader, size)
	return b
}

type MustReader struct {
	io.Reader
}

func (r *MustReader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	if err != nil {
		log.PanicErrorf(err, "read bytes failed")
	}
	return n, nil
}

type CountReader struct {
	io.Reader
	N *atomic2.Int64
}

func (r *CountReader) Read(b []byte) (int, error) {
	n, err := r.Reader.Read(b)
	r.N.Add(int64(n))
	return n, err
}

type WriterBuilder struct {
	io.Writer
}

func wBuilder(w io.Writer) *WriterBuilder {
	return &WriterBuilder{w}
}

func (b *WriterBuilder) Must() *WriterBuilder {
	b.Writer = &MustWriter{b.Writer}
	return b
}

func (b *WriterBuilder) Count(p *atomic2.Int64) *WriterBuilder {
	b.Writer = &CountWriter{b.Writer, p}
	return b
}

func (b *WriterBuilder) Mutex(l sync.Locker) *WriterBuilder {
	b.Writer = &MutexWriter{b.Writer, l}
	return b
}

func (b *WriterBuilder) Buffer(size int) *WriterBuilder {
	b.Writer = bufio.NewWriterSize(b.Writer, size)
	return b
}

func (b *WriterBuilder) Buffer2(size int) *WriterBuilder {
	b.Writer = bufio2.NewWriterSize(b.Writer, size)
	return b
}

type MustWriter struct {
	io.Writer
}

func (w *MustWriter) Write(b []byte) (int, error) {
	n, err := w.Writer.Write(b)
	if err != nil {
		log.PanicErrorf(err, "write bytes failed")
	}
	return n, nil
}

type CountWriter struct {
	io.Writer
	N *atomic2.Int64
}

func (w *CountWriter) Write(b []byte) (int, error) {
	n, err := w.Writer.Write(b)
	w.N.Add(int64(n))
	return n, err
}

type MutexWriter struct {
	io.Writer
	L sync.Locker
}

func (w *MutexWriter) Write(b []byte) (int, error) {
	w.L.Lock()
	n, err := w.Writer.Write(b)
	w.L.Unlock()
	return n, err
}
