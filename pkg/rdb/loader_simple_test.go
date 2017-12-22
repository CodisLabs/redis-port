package rdb_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/CodisLabs/redis-port/pkg/rdb"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

func newLoaderFromHexString(text string) *rdb.Loader {
	b, err := hex.DecodeString(strings.NewReplacer("\t", "", "\r", "", "\n", "", " ", "").Replace(text))
	if err != nil {
		log.PanicErrorf(err, "Read hex string failed.")
	}
	return rdb.NewLoader(bytes.NewReader(b))
}

func loadFromHexString(text string) DatabaseSet {
	return loadFromLoader(newLoaderFromHexString(text))
}

func TestSimpleLoadIntString(t *testing.T) {
	databases := loadFromHexString(`
		524544495330303036fe00000a737472696e675f323535c1ff00000873747269
		6e675f31c0010011737472696e675f343239343936373239360a343239343936
		373239360011737472696e675f343239343936373239350a3432393439363732
		39350012737472696e675f2d32313437343833363438c200000080000c737472
		696e675f3635353335c2ffff00000011737472696e675f323134373438333634
		380a32313437343833363438000c737472696e675f3635353336c20000010000
		0a737472696e675f323536c100010011737472696e675f323134373438333634
		37c2ffffff7fffe49d9f131fb5c3b5
	`)
	defer release(databases)
	expected := []int{1, 255, 256, 65535, 65536, 2147483647, 2147483648, 4294967295, 4294967296, -2147483648}
	databases.ValidateSize(map[uint64]int{0: len(expected)})
	for _, value := range expected {
		databases[0].ValidateStringObject(fmt.Sprintf("string_%d", value), strconv.Itoa(value))
	}
}
