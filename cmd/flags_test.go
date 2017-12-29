package main

import (
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
	test [--master=MASTER|MASTER] [--target=TARGET]
	test [--tmpfile=FILE --tmpfile-size=SIZE]
	test [--unixtime-in-milliseconds=EXPR]
	test  --version

Options:
	-m MASTER, --master=MASTER
	-t TARGET, --target=TARGET
`
	return parseFlagsFromArgs(usage, array)
}

func TestParseFlagsMasterTarget(t *testing.T) {
	var testcase = func(line string, source, target string) {
		var flags = parseFlagsFromString(line)
		assert.Must(flags.Source == source)
		assert.Must(flags.Target == target)
	}
	testcase("", "", "")
	testcase("abc", "abc", "")
	testcase("-m abc", "abc", "")
	testcase("--master abc", "abc", "")
	testcase("a/b/c", "a/b/c", "")
	testcase("/a/b/c", "/a/b/c", "")
	testcase("//a/b/c", "//a/b/c", "")

	testcase("-t abc", "", "abc")
	testcase("--target abc", "", "abc")

	testcase("abc -t xyz", "abc", "xyz")
	testcase("-m abc -t xyz", "abc", "xyz")
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
		assert.Must(math.Abs(float64(flags.ExpireOffset-offset)) <= math.Abs(float64(delta)))
	}
	var now = time.Duration(time.Now().UnixNano())
	testcase("", 0, 0)
	testcase("--unixtime-in-milliseconds=@0", -now, time.Second)
	testcase("--unixtime-in-milliseconds=+1000ms", time.Second, 0)
	testcase("--unixtime-in-milliseconds=-1000ms", -time.Second, 0)
}
