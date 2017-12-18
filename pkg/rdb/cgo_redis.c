#include "cgo_redis.h"

extern void initServerConfig(void);
extern void loadServerConfigFromString(char *config);
extern void createSharedObjects(void);

void initRedisServer(const void *buf, size_t len) {
  initServerConfig();
  createSharedObjects();
  if (buf != NULL && len != 0) {
    sds config = sdsnewlen(buf, len);
    loadServerConfigFromString(config);
    sdsfree(config);
  }
}

extern size_t cgoRedisRioRead(rio *rdb, void *buf, size_t len);
static size_t rioRedisRioRead(rio *rdb, void *buf, size_t len) {
  return cgoRedisRioRead(rdb, buf, len);
}

extern size_t cgoRedisRioWrite(rio *rdb, const void *buf, size_t len);
static size_t rioRedisRioWrite(rio *rdb, const void *buf, size_t len) {
  return cgoRedisRioWrite(rdb, buf, len);
}

extern off_t cgoRedisRioTell(rio *rdb);
static off_t rioRedisRioTell(rio *rdb) { return cgoRedisRioTell(rdb); }

extern int cgoRedisRioFlush(rio *rdb);
static int rioRedisRioFlush(rio *rdb) { return cgoRedisRioFlush(rdb); }

extern void cgoRedisRioUpdateChecksum(rio *rdb, uint64_t checksum);
static void rioRedisRioUpdateChecksum(rio *rdb, const void *buf, size_t len) {
  rioGenericUpdateChecksum(rdb, buf, len);
  cgoRedisRioUpdateChecksum(rdb, rdb->cksum);
}

static const rio redisRioIO = {
    rioRedisRioRead,
    rioRedisRioWrite,
    rioRedisRioTell,
    rioRedisRioFlush,
    rioRedisRioUpdateChecksum,
    0,           /* current checksum */
    0,           /* bytes read or written */
    8192,        /* read/write chunk size */
    {{NULL, 0}}, /* union for io-specific vars */
};

void redisRioInit(rio *rdb) { *rdb = redisRioIO; }

int redisRioRead(rio *rdb, void *buf, size_t len) {
  return rioRead(rdb, buf, len) != 0 ? 0 : -1;
}
