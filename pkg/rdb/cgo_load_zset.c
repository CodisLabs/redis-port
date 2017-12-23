#include "cgo_redis.h"

robj* rdbLoadZsetObject(int rdbtype, rio* rdb) {
  return rdbLoadObject(rdbtype, rdb);
}
