package rdb_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/CodisLabs/redis-port/pkg/rdb"

	"github.com/CodisLabs/codis/pkg/utils/assert"
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

func TestSimpleIntString(t *testing.T) {
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
	var values = []int{1, 255, 256, 65535, 65536, 2147483647, 2147483648, 4294967295, 4294967296, -2147483648}
	databases.ValidateSize(map[uint64]int{0: len(values)})
	for _, value := range values {
		databases[0].ValidateStringObject(fmt.Sprintf("string_%d", value), strconv.Itoa(value))
	}
}

func TestSimpleStringTTL(t *testing.T) {
	databases := loadFromHexString(`
		524544495330303036fe00fc0098f73e5d010000000c737472696e675f74746c
		6d730c737472696e675f74746c6d73fc0098f73e5d010000000b737472696e67
		5f74746c730b737472696e675f74746c73ffd15acd935a3fe949
	`)
	defer release(databases)
	var keys = []string{"string_ttls", "string_ttlms"}
	databases.ValidateSize(map[uint64]int{0: len(keys)})
	for _, key := range keys {
		var entry = databases[0][key]
		assert.Must(entry != nil)
		assert.Must(entry.Value.AsString().String() == key)
		assert.Must(entry.Expire == time.Duration(1500000000000)*time.Millisecond)
	}
}

func TestSimpleLongString(t *testing.T) {
	databases := loadFromHexString(`
		524544495330303036fe00000b737472696e675f6c6f6e67c342f28000010000
		02303130e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0
		ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01
		e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff
		01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0
		ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01
		e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff
		01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0
		ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01
		e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff
		01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0
		ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01
		e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff
		01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0
		ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01
		e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff
		01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0
		ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01
		e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff
		01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0
		ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01
		e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff
		01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0
		ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01e0ff01
		e0ff01e0ff01e0ff01e0ff01e03201013031ffdfdb02bd6d5da5e6
	`)
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var entry = databases[0]["string_long"]
	assert.Must(entry != nil)
	var value = entry.Value.AsString().String()
	fmt.Println(len(value), 1<<16)
	for i := 0; i < len(value); i++ {
		assert.Must(value[i] == ('0' + uint8(i%2)))
	}
}

func TestSimpleListZipmap(t *testing.T) {
	databases := loadFromHexString(`
		524544495330303036fe000a086c6973745f6c7a66c31f440b040b0400000820
		0306000200f102f202e0ff03e1ff07e1ff07e1d90701f2ffff6a1c2d51c02301
		16
	`)
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var list = databases[0].ValidateListObject("list_lzf", 512)
	for i, value := range list {
		assert.Must(value == strconv.Itoa(i%2))
	}
}

func TestSimpleListRegular(t *testing.T) {
	databases := loadFromHexString(`
		524544495330303036fe0001046c69737420c000c001c002c003c004c005c006
		c007c008c009c00ac00bc00cc00dc00ec00fc010c011c012c013c014c015c016
		c017c018c019c01ac01bc01cc01dc01ec01fff756ea1fa90adefe3
	`)
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var list = databases[0].ValidateListObject("list", 32)
	for i, value := range list {
		assert.Must(value == strconv.Itoa(i))
	}
}

func TestSimpleHashAndHashZiplist(t *testing.T) {
	databases := loadFromHexString(`
		524544495330303036fe000405686173683220c00dc00dc0fcc0fcc0ffc0ffc0
		04c004c002c002c0fbc0fbc0f0c0f0c0f9c0f9c008c008c0fac0fac006c006c0
		00c000c001c001c0fec0fec007c007c0f6c0f6c00fc00fc009c009c0f7c0f7c0
		fdc0fdc0f1c0f1c0f2c0f2c0f3c0f3c00ec00ec003c003c00ac00ac00bc00bc0
		f8c0f8c00cc00cc0f5c0f5c0f4c0f4c005c0050d056861736831405151000000
		4d000000200000f102f102f202f202f302f302f402f402f502f502f602f602f7
		02f702f802f802f902f902fa02fa02fb02fb02fc02fc02fd02fd02fe0d03fe0d
		03fe0e03fe0e03fe0f03fe0fffffa423d3036c15e534
	`)
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 2})
	var hash1 = databases[0].ValidateHashObject("hash1", 16)
	for i := 0; i < len(hash1); i++ {
		s := strconv.Itoa(i)
		assert.Must(hash1[s] == s)
	}
	var hash2 = databases[0].ValidateHashObject("hash2", 32)
	for i := 0; i < len(hash2); i++ {
		s := strconv.Itoa(i - 16)
		assert.Must(hash2[s] == s)
	}
}

func TestSimpleZsetAndZsetZiplist(t *testing.T) {
	databases := loadFromHexString(`
		524544495330303036fe0003057a7365743220c016032d3232c00d032d3133c0
		1b032d3237c012032d3138c01a032d3236c004022d34c014032d3230c002022d
		32c017032d3233c01d032d3239c01c032d3238c013032d3139c019032d3235c0
		1e032d3330c008022d38c006022d36c000022d30c001022d31c007022d37c009
		022d39c00f032d3135c01f032d3331c00e032d3134c003022d33c00a032d3130
		c015032d3231c010032d3136c00b032d3131c018032d3234c011032d3137c00c
		032d3132c005022d350c057a736574314051510000004d000000200000f102f1
		02f202f202f302f302f402f402f502f502f602f602f702f702f802f802f902f9
		02fa02fa02fb02fb02fc02fc02fd02fd02fe0d03fe0d03fe0e03fe0e03fe0f03
		fe0fffff2addedbf4f5a8f93
	`)
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 2})
	var zset1 = databases[0].ValidateZsetObject("zset1", 16)
	for i := 0; i < len(zset1); i++ {
		s := strconv.Itoa(i)
		assert.Must(floatEqual(zset1[s], float64(i)))
	}
	var zset2 = databases[0].ValidateZsetObject("zset2", 32)
	for i := 0; i < len(zset2); i++ {
		s := strconv.Itoa(i)
		assert.Must(floatEqual(zset2[s], -float64(i)))
	}
}

func TestSimpleSetAndSetZiplist(t *testing.T) {
	databases := loadFromHexString(`
		524544495330303036fe0002047365743220c016c00dc01bc012c01ac004c014
		c002c017c01dc01cc013c019c01ec008c006c000c001c007c00fc009c01fc00e
		c003c00ac015c010c00bc018c011c00cc0050b04736574312802000000100000
		0000000100020003000400050006000700080009000a000b000c000d000e000f
		00ff3a0a9697324d19c3
	`)
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 2})
	var set1 = databases[0].ValidateSetObject("set1", 16)
	for i := 0; i < len(set1); i++ {
		s := strconv.Itoa(i)
		assert.Must(set1[s])
	}
	var set2 = databases[0].ValidateSetObject("set2", 32)
	for i := 0; i < len(set2); i++ {
		s := strconv.Itoa(i)
		assert.Must(set2[s])
	}
}
