#include "cgo_redis.h"

extern void initServerConfig(void);
extern void loadServerConfigFromString(char *config);
extern void createSharedObjects(void);
extern void initLazyfreeThreads(void);

void initRedisServer(char *config) {
  initServerConfig();
  createSharedObjects();
  loadServerConfigFromString(config);
  initLazyfreeThreads();
}

#include <stddef.h>

#define container_of(ptr, type, member) \
  (type *)((char *)(ptr)-offsetof(type, member));

extern size_t onRedisRioRead(redisRio *p, void *buf, size_t len);
static size_t rioRedisRioRead(rio *rdb, void *buf, size_t len) {
  redisRio *p = container_of(rdb, redisRio, rdb);
  while (len) {
    size_t remains = p->end - p->pos;
    if (remains != 0) {
      size_t nbytes = remains < len ? remains : len;
      memcpy(buf, p->buf + p->pos, nbytes);
      len -= nbytes, buf = (char *)buf + nbytes, p->pos += nbytes;
    } else if (len >= sizeof(p->buf)) {
      size_t nbytes = onRedisRioRead(p, buf, len);
      len -= nbytes, buf = (char *)buf + nbytes;
    } else {
      p->pos = 0;
      p->end = onRedisRioRead(p, p->buf, sizeof(p->buf));
    }
  }
  return 1;
}

static size_t rioRedisRioWrite(rio *rdb, const void *buf, size_t len) {
  serverPanic("redisRio doesn't support write.");
}

static off_t rioRedisRioTell(rio *rdb) {
  serverPanic("redisRio doesn't support tell.");
}

static int rioRedisRioFlush(rio *rdb) {
  serverPanic("redisRio doesn't support flush.");
}

static const rio redisRioIO = {
    rioRedisRioRead,
    rioRedisRioWrite,
    rioRedisRioTell,
    rioRedisRioFlush,
    rioGenericUpdateChecksum,
    0,           /* current checksum */
    0,           /* bytes read or written */
    0,           /* read/write chunk size */
    {{NULL, 0}}, /* union for io-specific vars */
};

void redisRioInit(redisRio *p) {
  p->rdb = redisRioIO;
  p->pos = p->end = 0;
  memset(p->buf, 0, sizeof(p->buf));
}

int redisRioRead(redisRio *p, void *buf, size_t len) {
  return rioRead(&p->rdb, buf, len) != 0 ? 0 : -1;
}

int redisRioLoadLen(redisRio *p, uint64_t *len) {
  return (*len = rdbLoadLen(&p->rdb, NULL)) != RDB_LENERR ? 0 : -1;
}

int redisRioLoadType(redisRio *p, int *typ) {
  return (*typ = rdbLoadType(&p->rdb)) >= 0 ? 0 : -1;
}

int redisRioLoadTime(redisRio *p, time_t *val) {
  return (*val = rdbLoadTime(&p->rdb)) >= 0 ? 0 : -1;
}

extern long long rdbLoadMillisecondTime(rio *rdb);

int redisRioLoadTimeMillisecond(redisRio *p, long long *val) {
  return (*val = rdbLoadMillisecondTime(&p->rdb)) >= 0 ? 0 : -1;
}

extern robj *rdbLoadZsetObject(int rdbtype, rio *rdb);

void *redisRioLoadObject(redisRio *p, int typ) {
  switch (typ) {
  default:
    return rdbLoadObject(typ, &p->rdb);
  case RDB_TYPE_ZSET:
  case RDB_TYPE_ZSET_2:
    return rdbLoadZsetObject(typ, &p->rdb);
  }
}

void *redisRioLoadStringObject(redisRio *p) {
  return rdbLoadStringObject(&p->rdb);
}
