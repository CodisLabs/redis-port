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
		rd io.Reader

		rbytes atomic2.Int64
	}
	if len(flags.Source) != 0 {
		input.Path = flags.Source
	} else {
		input.Path = "/dev/stdin"
	}

	var output struct {
		Path string
		io.Writer
		wt *bufio.Writer

		wbytes atomic2.Int64
	}
	if len(flags.Target) != 0 {
		output.Path = flags.Target
	} else {
		output.Path = "/dev/stdout"
	}
	log.Infof("decode: input = %q, output = %q\n", input.Path, output.Path)

	var objects atomic2.Int64

	if input.Path != "/dev/stdin" {
		file, size := openReadFile(input.Path)
		defer file.Close()
		input.Reader, input.Size = file, size
	} else {
		input.Reader = os.Stdin
	}
	input.rd = rBuilder(input.Reader).Must().
		Buffer(ReaderBufferSize).Count(&input.rbytes).Reader

	if output.Path != "/dev/stdout" {
		file := openWriteFile(output.Path)
		defer closeFile(file)
		output.Writer = file
	} else {
		output.Writer = os.Stdout
	}
	output.wt = wBuilder(output.Writer).Must().
		Count(&output.wbytes).Buffer(WriterBufferSize).Writer.(*bufio.Writer)

	var mu sync.Mutex

	var entryChan = newRDBLoader(input.rd, 32)

	var jobs = NewParallelJob(flags.Parallel, func() {
		for e := range entryChan {
			synchronized(&mu, func() {
				objects.Incr()
				toJsonDBEntry(e, output.wt)
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
				flushWriter(output.wt)
			})
		}
	}).Run()

	log.Infof("decode: (r,w,o) = (read,write,objects)")

	NewJob(func() {
		for stop := false; !stop; {
			select {
			case <-done:
				stop = true
			case <-time.After(time.Second):
			}
			stats := &struct {
				input, output, objects int64
			}{
				input.rbytes.Int64(), output.wbytes.Int64(), objects.Int64(),
			}

			var b bytes.Buffer
			var percent float64
			if input.Size != 0 {
				percent = float64(stats.input) * 100 / float64(input.Size)
			}
			fmt.Fprintf(&b, "decode: file = %d - [%6.2f%%]", input.Size, percent)
			fmt.Fprintf(&b, "   (r,w,o)=%s",
				formatAlign(4, "(%d,%d,%d)", stats.input, stats.output, stats.objects))
			fmt.Fprintf(&b, "  ~  (%s,%s,-)",
				bytesize.Int64(stats.input).HumanString(), bytesize.Int64(stats.output).HumanString())
			log.Info(b.String())
		}
	}).RunAndWait()

	log.Info("decode: done")
}
