package pipe

import (
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/CodisLabs/codis/pkg/utils/assert"
)

func testBufferReadWriteSome(p Buffer) {
	var makeSlice = func(n int) []byte {
		return make([]byte, rand.Intn(n)+1)
	}
	var rpos uint64
	var readSome = func() int {
		buffered := p.Buffered()
		var b = makeSlice(512)
		n, err := p.ReadSome(b)
		assert.MustNoError(err)
		if buffered != 0 {
			assert.Must(n != 0)
		} else {
			assert.Must(n == 0)
		}
		for i := 0; i < n; i++ {
			assert.Must(b[i] == byte(rpos+uint64(i)))
		}
		rpos += uint64(n)
		return n
	}
	var wpos uint64
	var writeSome = func() int {
		available := p.Available()
		var b = makeSlice(512)
		for i := 0; i < len(b); i++ {
			b[i] = byte(wpos + uint64(i))
		}
		n, err := p.WriteSome(b)
		assert.MustNoError(err)
		if available != 0 {
			assert.Must(n != 0)
		} else {
			assert.Must(n == 0)
		}
		wpos += uint64(n)
		return n
	}
	var nread, nwrite int
	for nread < 1024*1024*128 {
		nread += readSome()
		nwrite += writeSome()
	}
	for nread != nwrite {
		nread += readSome()
	}
}

func TestMemBuffer(t *testing.T) {
	testBufferReadWriteSome(&memBuffer{buf: make([]byte, 32749)})
}

func TestFileBuffer(t *testing.T) {
	f, err := ioutil.TempFile("", "pipe_buffer_test_")
	assert.MustNoError(err)
	defer func() {
		assert.MustNoError(f.Close())
	}()
	testBufferReadWriteSome(&fileBuffer{file: f, size: 32749})
	assert.MustNoError(os.Remove(f.Name()))
}
