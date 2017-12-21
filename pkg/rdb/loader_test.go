package rdb_test

import (
	"bytes"
	"io/ioutil"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/CodisLabs/redis-port/pkg/rdb"

	"github.com/CodisLabs/codis/pkg/utils/assert"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

func newLoader(name string) *rdb.Loader {
	b, err := ioutil.ReadFile(filepath.Join("testing", name))
	if err != nil {
		log.PanicErrorf(err, "Read file '%s' failed", name)
	}
	return rdb.NewLoader(bytes.NewReader(b))
}

type Database map[string]*rdb.DBEntry

func (d Database) ValidateStringObject(key string, value string) {
	assert.Must(d != nil)
	assert.Must(d[key] != nil)
	assert.Must(d[key].Value.IsString())
	assert.Must(d[key].Value.AsString().String() == value)
}

func (d Database) ValidateListObject(key string, size int) []string {
	assert.Must(d != nil)
	assert.Must(d[key] != nil)
	assert.Must(d[key].Value.IsList())
	assert.Must(d[key].Value.AsList().Len() == size)
	return d[key].Value.AsList().Strings()
}

func (d Database) ValidateHashObject(key string, size int) map[string]string {
	assert.Must(d != nil)
	assert.Must(d[key] != nil)
	assert.Must(d[key].Value.IsHash())
	assert.Must(d[key].Value.AsHash().Len() == size)
	return d[key].Value.AsHash().Map()
}

func (d Database) ValidateZsetObject(key string, size int) map[string]float64 {
	assert.Must(d != nil)
	assert.Must(d[key] != nil)
	assert.Must(d[key].Value.IsZset())
	assert.Must(d[key].Value.AsZset().Len() == size)
	return d[key].Value.AsZset().Map()
}

func (d Database) ValidateSetObject(key string, size int) map[string]bool {
	assert.Must(d != nil)
	assert.Must(d[key] != nil)
	assert.Must(d[key].Value.IsSet())
	assert.Must(d[key].Value.AsSet().Len() == size)
	return d[key].Value.AsSet().Map()
}

type DatabaseSet map[uint64]Database

func (databases DatabaseSet) ValidateSize(expected map[uint64]int) {
	assert.Must(len(databases) == len(expected))
	for id, db := range databases {
		assert.Must(len(db) == expected[id])
	}
}

func loadFromFile(name string) DatabaseSet {
	databases := make(map[uint64]Database)
	loader := newLoader(name)
	loader.Header()
	loader.ForEach(func(e *rdb.DBEntry) bool {
		db, ok := databases[e.DB]
		if !ok {
			db = make(map[string]*rdb.DBEntry)
			databases[e.DB] = db
		}
		assert.Must(db[e.Key.String()] == nil)
		db[e.Key.String()] = e.IncrRefCount()
		return true
	})
	loader.Footer()
	return databases
}

func release(databases DatabaseSet) {
	for _, db := range databases {
		for _, e := range db {
			e.DecrRefCount()
		}
	}
}

func TestEmptyDatabase(t *testing.T) {
	databases := loadFromFile("empty_database.rdb")
	defer release(databases)
	assert.Must(len(databases) == 0)
}

func TestEmptyDatabaseNoChecksum(t *testing.T) {
	databases := loadFromFile("empty_database_nochecksum.rdb")
	defer release(databases)
	assert.Must(len(databases) == 0)
}

func TestMultipleDatabases(t *testing.T) {
	databases := loadFromFile("multiple_databases.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1, 2: 1})
	databases[0].ValidateStringObject("key_in_zeroth_database", "zero")
	databases[2].ValidateStringObject("key_in_second_database", "second")
}

func TestIntegerKeys(t *testing.T) {
	databases := loadFromFile("integer_keys.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 6})
	databases[0].ValidateStringObject(
		strconv.Itoa(125),
		"Positive 8 bit integer")
	databases[0].ValidateStringObject(
		strconv.Itoa(0xABAB),
		"Positive 16 bit integer")
	databases[0].ValidateStringObject(
		strconv.Itoa(0x0AEDD325),
		"Positive 32 bit integer")
	databases[0].ValidateStringObject(
		strconv.Itoa(-123),
		"Negative 8 bit integer")
	databases[0].ValidateStringObject(
		strconv.Itoa(-0x7325),
		"Negative 16 bit integer")
	databases[0].ValidateStringObject(
		strconv.Itoa(-0x0AEDD325),
		"Negative 32 bit integer")
}

func TestStringKeyWithCompression(t *testing.T) {
	databases := loadFromFile("easily_compressible_string_key.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var key bytes.Buffer
	for i := 0; i < 200; i++ {
		key.WriteByte('a')
	}
	databases[0].ValidateStringObject(key.String(),
		"Key that redis should compress easily")
}

func TestRdbVersion5WithChecksum(t *testing.T) {
	databases := loadFromFile("rdb_version_5_with_checksum.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 6})
	databases[0].ValidateStringObject(
		"abcd", "efgh")
	databases[0].ValidateStringObject(
		"abc", "def")
	databases[0].ValidateStringObject(
		"foo", "bar")
	databases[0].ValidateStringObject(
		"bar", "baz")
	databases[0].ValidateStringObject(
		"abcdef", "abcdef")
	databases[0].ValidateStringObject(
		"longerstring", "thisisalongerstring.idontknowwhatitmeans")
}

func TestListAsLinkedList(t *testing.T) {
	databases := loadFromFile("linkedlist.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var list = databases[0].ValidateListObject("force_linkedlist", 1000)
	var contains = func(key string) bool {
		for _, s := range list {
			if s == key {
				return true
			}
		}
		return false
	}
	assert.Must(contains("JYY4GIFI0ETHKP4VAJF5333082J4R1UPNPLE329YT0EYPGHSJQ"))
	assert.Must(contains("TKBXHJOX9Q99ICF4V78XTCA2Y1UYW6ERL35JCIL1O0KSGXS58S"))
}

func TestListAsZiplistWithCompression(t *testing.T) {
	databases := loadFromFile("ziplist_that_compresses_easily.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var list = databases[0].ValidateListObject("ziplist_compresses_easily", 6)
	for i, length := range []int{6, 12, 18, 24, 30, 36} {
		assert.Must(len(list[i]) == length)
		for _, c := range list[i] {
			assert.Must(c == 'a')
		}
	}
}

func TestListAsZiplistWithoutCompression(t *testing.T) {
	databases := loadFromFile("ziplist_that_doesnt_compress.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var list = databases[0].ValidateListObject("ziplist_doesnt_compress", 2)
	sort.Strings(list)
	assert.Must(list[0] == "aj2410")
	assert.Must(list[1] == "cc953a17a8e096e76a44169ad3f9ac87c5f8248a403274416179aa9fbd852344")
}

func TestListAsZiplistWithIntegers(t *testing.T) {
	databases := loadFromFile("ziplist_with_integers.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var list = databases[0].ValidateListObject("ziplist_with_integers", 24)
	sort.Strings(list)
	var expected []string
	for i := 0; i < 13; i++ {
		expected = append(expected, strconv.Itoa(i))
	}
	for _, v := range []int{
		-2, 13, 25, -61, 63, 16380, -16000, 65535,
		-65523, 4194304, 0x7fffffffffffffff} {
		expected = append(expected, strconv.Itoa(v))
	}
	sort.Strings(expected)
	for i := range list {
		assert.Must(list[i] == expected[i])
	}
}

func TestHashAsHashTable(t *testing.T) {
	databases := loadFromFile("hash_table.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var hash = databases[0].ValidateHashObject("force_dictionary", 1000)
	assert.Must(hash["ZMU5WEJDG7KU89AOG5LJT6K7HMNB3DEI43M6EYTJ83VRJ6XNXQ"] ==
		"T63SOS8DQJF0Q0VJEZ0D1IQFCYTIPSBOUIAI9SB0OV57MQR1FI")
	assert.Must(hash["UHS5ESW4HLK8XOGTM39IK1SJEUGVV9WOPK6JYA5QBZSJU84491"] ==
		"6VULTCV52FXJ8MGVSFTZVAGK2JXZMGQ5F8OVJI0X6GEDDR27RZ")
}

func TestHashAsZiplist(t *testing.T) {
	databases := loadFromFile("hash_as_ziplist.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var hash = databases[0].ValidateHashObject("zipmap_compresses_easily", 3)
	assert.Must(hash["a"] == "aa")
	assert.Must(hash["aa"] == "aaaa")
	assert.Must(hash["aaaaa"] == "aaaaaaaaaaaaaa")
}

func TestHashAsZipmapWithCompression(t *testing.T) {
	databases := loadFromFile("zipmap_that_compresses_easily.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var hash = databases[0].ValidateHashObject("zipmap_compresses_easily", 3)
	assert.Must(hash["a"] == "aa")
	assert.Must(hash["aa"] == "aaaa")
	assert.Must(hash["aaaaa"] == "aaaaaaaaaaaaaa")
}

func TestHashAsZipmapWithoutCompression(t *testing.T) {
	databases := loadFromFile("zipmap_that_doesnt_compress.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var hash = databases[0].ValidateHashObject("zimap_doesnt_compress", 2)
	assert.Must(hash["MKD1G6"] == "2")
	assert.Must(hash["YNNXK"] == "F7TI")
}

func TestHashAsZipmapWithBigValues(t *testing.T) {
	databases := loadFromFile("zipmap_with_big_values.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var hash = databases[0].ValidateHashObject("zipmap_with_big_values", 5)
	assert.Must(len(hash["253bytes"]) == 253)
	assert.Must(len(hash["254bytes"]) == 254)
	assert.Must(len(hash["255bytes"]) == 255)
	assert.Must(len(hash["300bytes"]) == 300)
	assert.Must(len(hash["20kbytes"]) == 20000)
}

func floatEqual(f1, f2 float64) bool {
	return math.Abs(f1-f2) < 0.1
}

func TestZsetAsZiplist(t *testing.T) {
	databases := loadFromFile("sorted_set_as_ziplist.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var zset = databases[0].ValidateZsetObject("sorted_set_as_ziplist", 3)
	assert.Must(floatEqual(zset["8b6ba6718a786daefa69438148361901"], 1))
	assert.Must(floatEqual(zset["cb7a24bb7528f934b841b34c3a73e0c7"], 2.37))
	assert.Must(floatEqual(zset["523af537946b79c4f8369ed39ba78605"], 3.423))
}

func TestZsetRegularZset(t *testing.T) {
	databases := loadFromFile("regular_sorted_set.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var zset = databases[0].ValidateZsetObject("force_sorted_set", 500)
	assert.Must(floatEqual(zset["8URS19PINCX9H1H7UNBF6GWUPZEYCHYGERXAYVAUATVNM2GQRB"], 0.56))
	assert.Must(floatEqual(zset["UH87QXHHKYH8CGD1NQLWOHPKD3YX5ONPOYAQTMAZAUFBGCFY0N"], 1.01))
	assert.Must(floatEqual(zset["SKP3TXT7J6IZBRATLNVPUYV1KXU8WNA0SZCBLPCN20XO97SU3R"], 4.82))
	assert.Must(floatEqual(zset["RJWIR8DLYDF39LG9LVVW68Y32XPIJ7ZD6JYQJHUOWZ34W8R533"], 0.5))
	assert.Must(floatEqual(zset["HEAWIHTQWGDIBIJHM3SUHMO8WFBPWT8TBDQYREDLWOMV3KBIHA"], 3.14))
}

func TestSetIntset16(t *testing.T) {
	databases := loadFromFile("intset_16.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var set = databases[0].ValidateSetObject("intset_16", 3)
	for _, v := range []int{0x7ffe, 0x7ffd, 0x7ffc} {
		assert.Must(set[strconv.Itoa(v)])
	}
}

func TestSetIntset32(t *testing.T) {
	databases := loadFromFile("intset_32.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var set = databases[0].ValidateSetObject("intset_32", 3)
	for _, v := range []int{0x7ffefffe, 0x7ffefffd, 0x7ffefffc} {
		assert.Must(set[strconv.Itoa(v)])
	}
}

func TestSetIntset64(t *testing.T) {
	databases := loadFromFile("intset_64.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var set = databases[0].ValidateSetObject("intset_64", 3)
	for _, v := range []int{0x7ffefffefffefffe, 0x7ffefffefffefffd, 0x7ffefffefffefffc} {
		assert.Must(set[strconv.Itoa(v)])
	}
}

func TestSetRegularSet(t *testing.T) {
	databases := loadFromFile("regular_set.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var set = databases[0].ValidateSetObject("regular_set", 6)
	for _, key := range []string{"alpha", "beta", "gamma", "delta", "phi", "kappa"} {
		assert.Must(set[key])
	}
}

func TestNonASCIIValues(t *testing.T) {
	databases := loadFromFile("non_ascii_values.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 6})
	databases[0].ValidateStringObject("int_value", "123")
	databases[0].ValidateStringObject("378", "int_key_name")
	databases[0].ValidateStringObject("bin", "\x00\x24\x20\x7e\x30\x7f\xff\x0a\xaa\x09\x80\x0d\x41\x62")
	databases[0].ValidateStringObject("utf8", "בדיקה𐀏123עברית")
}

func TestRdbVersion8With64bLengthAndScores(t *testing.T) {
	databases := loadFromFile("rdb_version_8_with_64b_length_and_scores.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 2})
	databases[0].ValidateStringObject("foo", "bar")
	var zset = databases[0].ValidateZsetObject("bigset", 1000)
	assert.Must(floatEqual(zset["finalfield"], 2.718))
}

func TestUncompressibleStringKeys(t *testing.T) {
	databases := loadFromFile("uncompressible_string_keys.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 3})
}

func TestKeysWithExpiry(t *testing.T) {
	databases := loadFromFile("keys_with_expiry.rdb")
	defer release(databases)
	databases.ValidateSize(map[uint64]int{0: 1})
	var entry = databases[0]["expires_ms_precision"]
	assert.Must(entry != nil)
	assert.Must(entry.Expire != rdb.NoExpire)
	var expire = time.Unix(int64(entry.Expire/time.Second), int64(entry.Expire%time.Second))
	var utc, _ = time.LoadLocation("UTC")
	assert.Must(expire.In(utc).Format("2006-01-02 15:04:05.000000") == "2022-12-25 10:11:12.573000")
}

func TestListDecode(t *testing.T) {
	databases := loadFromFile("list_decode.rdb")
	defer release(databases)
	var list = databases[0].ValidateListObject("list", 1000*1000)
	for i := 0; i < 1000*1000; i++ {
		assert.Must(list[i] == strconv.Itoa(i))
	}
}

func BenchmarkListIterator(b *testing.B) {
	databases := loadFromFile("list_decode.rdb")
	defer release(databases)
	b.ResetTimer()
	var iter *rdb.RedisListIterator
	defer func() {
		if iter != nil {
			iter.Release()
		}
	}()
	for i := 0; i < b.N; i++ {
	try_again:
		if iter == nil {
			iter = databases[0]["list"].Value.AsList().NewIterator()
			goto try_again
		}
		if sds := iter.Next(); sds == nil {
			iter.Release()
			iter = nil
			goto try_again
		} else {
			sds.StringUnsafe()
		}
	}
}
