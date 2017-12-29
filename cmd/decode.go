package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/bytesize"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

func main() {
	const usage = `
Usage:
	redis-decode [--ncpu=N] [--input=INPUT|INPUT] [--output=OUTPUT]
	redis-decode  --version

Options:
	-n N, --ncpu=N                    Set runtime.GOMAXPROCS to N.
	-i INPUT, --input=INPUT           Set input file, default is '/dev/stdin'.
	-o OUTPUT, --output=OUTPUT        Set output file, default is '/dev/stdout'.

Examples:
	$ redis-decode -i dump.rdb -o dump.log
	$ redis-decode    dump.rdb -o dump.log
	$ cat dump.rdb | redis-decode --ncpu=8 > dump.log
`
	var flags = parseFlags(usage)

	var input struct {
		Path string
		Size int64
		io.Reader
	}
	if len(flags.Source) != 0 {
		input.Path = flags.Source
	} else {
		input.Path = "/dev/stdin"
	}

	var output struct {
		Path string
		io.Writer
	}
	if len(flags.Target) != 0 {
		output.Path = flags.Target
	} else {
		output.Path = "/dev/stdout"
	}
	log.Infof("decode: input = %q, output = %q\n", input.Path, output.Path)

	var rbytes, wbytes atomic2.Int64
	var objects atomic2.Int64

	if input.Path != "/dev/stdin" {
		file, size := openReadFile(input.Path)
		defer file.Close()
		input.Reader, input.Size = file, size
	} else {
		input.Reader = os.Stdin
	}
	var reader = rBuilder(input.Reader).Must().
		Buffer(ReaderBufferSize).Count(&rbytes).Reader

	if output.Path != "/dev/stdout" {
		file := openWriteFile(output.Path)
		defer closeFile(file)
		output.Writer = file
	} else {
		output.Writer = os.Stdout
	}
	var writer = wBuilder(output.Writer).Must().
		Count(&wbytes).Buffer(WriterBufferSize).Writer.(*bufio.Writer)

	var mu sync.Mutex

	var entryChan = newRDBLoader(reader, 32)

	var jobs = NewParallelJob(flags.Parallel, func() {
		for e := range entryChan {
			synchronized(&mu, func() {
				objects.Incr()
				toJsonDBEntry(e, writer)
			})
			e.DecrRefCount()
		}
	}).Run()

	var done = NewJob(func() {
		for stop := false; !stop; {
			select {
			case <-jobs:
				stop = true
			case <-time.After(time.Second):
			}
			synchronized(&mu, func() {
				flushWriter(writer)
			})
		}
	}).Run()

	log.Infof("decode: (r/w/o) = (read/write/objects)")

	NewJob(func() {
		for stop := false; !stop; {
			select {
			case <-done:
				stop = true
			case <-time.After(time.Second):
			}
			stats := &struct {
				rbytes, wbytes, objects int64
			}{
				rbytes.Int64(),
				wbytes.Int64(), objects.Int64(),
			}

			var b bytes.Buffer
			var percent float64
			if input.Size != 0 {
				percent = float64(stats.rbytes) * 100 / float64(input.Size)
			}
			fmt.Fprintf(&b, "decode: file = %d - [%6.2f%%]", input.Size, percent)
			fmt.Fprintf(&b, "   (r,w,o)=%s",
				formatAlign(4, "(%d,%d,%d)", stats.rbytes, stats.wbytes, stats.objects))
			fmt.Fprintf(&b, "  -  (%s,%s,-)",
				bytesize.Int64(stats.rbytes).HumanString(),
				bytesize.Int64(stats.wbytes).HumanString())
			log.Info(b.String())
		}
	}).RunAndWait()

	log.Info("decode: done")
}
