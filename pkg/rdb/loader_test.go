package rdb_test

import (
	"bytes"
	"io/ioutil"
	"path/filepath"
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
