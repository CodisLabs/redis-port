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
`
	var flags = &struct {
		*Flags
		mu sync.Mutex
	}{
		Flags: parseFlags(usage),
	}

	if len(flags.Input) == 0 {
		flags.Input = "/dev/stdin"
	}
	if len(flags.Output) == 0 {
		flags.Output = "/dev/stdout"
	}
	log.Infof("decode: input=%q output=%q\n", flags.Input, flags.Output)

	var rbytes, wbytes atomic2.Int64
	var objects atomic2.Int64

	var input struct {
		io.Reader
		size int64
	}
	if flags.Input != "/dev/stdin" {
		file, size := openReadFile(flags.Input)
		defer file.Close()
		input.Reader, input.size = file, size
	} else {
		input.Reader = os.Stdin
	}
	var reader = rBuilder(input.Reader).Must().
		Buffer(ReaderBufferSize).Count(&rbytes).Reader

	var output struct {
		io.Writer
	}
	if flags.Output != "/dev/stdout" {
		file := openWriteFile(flags.Output)
		defer closeFile(file)
		output.Writer = file
	} else {
		output.Writer = os.Stdout
	}
	var writer = wBuilder(output.Writer).Must().
		Count(&wbytes).Buffer(WriterBufferSize).Writer.(*bufio.Writer)

	var entryChan = newRDBLoader(reader, 32)

	var jobs = NewParallelJob(flags.Parallel, func() {
		for e := range entryChan {
			synchronized(&flags.mu, func() {
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
			synchronized(&flags.mu, func() {
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
			if input.size != 0 {
				percent = float64(stats.rbytes) * 100 / float64(input.size)
			}
			fmt.Fprintf(&b, "decode: file = %d - [%6.2f%%]", input.size, percent)
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
