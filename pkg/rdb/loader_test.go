package rdb_test

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"testing"

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
	loader.Scan(func(e *rdb.DBEntry) bool {
		db, ok := databases[e.DB]
		if !ok {
			db = make(map[string]*rdb.DBEntry)
			databases[e.DB] = db
		}
		assert.Must(db[e.Key.String()] == nil)
		db[e.Key.String()] = e
		return true
	})
	loader.Footer()
	return databases
}

func release(databases DatabaseSet) {
	for _, db := range databases {
		for _, e := range db {
			e.Release()
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

func TestLinkedList(t *testing.T) {
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
