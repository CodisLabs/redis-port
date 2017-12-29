package main

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/assert"
)

func parseFlagsFromString(line string) *Flags {
	var array = []string{}
	for _, s := range strings.Split(line, " ") {
		if t := strings.TrimSpace(s); t != "" {
			array = append(array, t)
		}
	}
	const usage = `
Usage:
	test [--input=INPUT|INPUT] [--output=OUTPUT]
	test [--tmpfile=FILE --tmpfile-size=SIZE]
	test [--unixtime-in-milliseconds=EXPR]
	test  --version

Options:
	-i INPUT, --input=INPUT           Set input file, default is '/dev/stdin'.
	-o OUTPUT, --output=OUTPUT        Set output file, default is '/dev/stdout'.
`
	return parseFlagsFromArgs(usage, array)
}

func TestParseFlagsInputOutput(t *testing.T) {
	var testcase = func(line string, input, output string) {
		var flags = parseFlagsFromString(line)
		assert.Must(flags.Input == input)
		assert.Must(flags.Output == output)
	}
	testcase("", "", "")
	testcase("abc", "abc", "")
	testcase("-i abc", "abc", "")
	testcase("--input abc", "abc", "")
	testcase("a/b/c", "a/b/c", "")
	testcase("/a/b/c", "/a/b/c", "")
	testcase("//a/b/c", "//a/b/c", "")

	testcase("-o abc", "", "abc")
	testcase("--output abc", "", "abc")

	testcase("abc -o xyz", "abc", "xyz")
	testcase("-i abc -o xyz", "abc", "xyz")
}

func TestParseFlagsFileSize(t *testing.T) {
	var testcase = func(line string, path string, size int64) {
		var flags = parseFlagsFromString(line)
		assert.Must(flags.TmpFile.Path == path)
		assert.Must(flags.TmpFile.Size == size)
	}
	testcase("", "", 0)
	testcase("--tmpfile-size=1gb", "", 1<<30)
	testcase("--tmpfile=/a/b/c --tmpfile-size=1gb", "/a/b/c", 1<<30)
}

func TestParseFlagsUnixtime(t *testing.T) {
	var testcase = func(line string, offset, delta time.Duration) {
		var flags = parseFlagsFromString(line)
		fmt.Println(flags.ExpireOffset)
		assert.Must(math.Abs(float64(flags.ExpireOffset-offset)) <= math.Abs(float64(delta)))
	}
	var now = time.Duration(time.Now().UnixNano())
	testcase("", 0, 0)
	testcase("--unixtime-in-milliseconds=@0", -now, time.Second)
	testcase("--unixtime-in-milliseconds=+1000ms", time.Second, 0)
	testcase("--unixtime-in-milliseconds=-1000ms", -time.Second, 0)
}
