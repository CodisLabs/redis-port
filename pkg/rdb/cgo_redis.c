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
    1024 * 1024, /* read/write chunk size */
    {{NULL, 0}}, /* union for io-specific vars */
};

void redisRioInit(rio *rdb) { *rdb = redisRioIO; }

int redisRioRead(rio *rdb, void *buf, size_t len) {
  return rioRead(rdb, buf, len) != 0 ? 0 : -1;
}

int redisRioLoadLen(rio *rdb, uint64_t *len) {
  return (*len = rdbLoadLen(rdb, NULL)) != RDB_LENERR ? 0 : -1;
}

int redisRioLoadType(rio *rdb, int *typ) {
  return (*typ = rdbLoadType(rdb)) >= 0 ? 0 : -1;
}

int redisRioLoadTime(rio *rdb, time_t *val) {
  return (*val = rdbLoadTime(rdb)) >= 0 ? 0 : -1;
}

extern long long rdbLoadMillisecondTime(rio *rdb);

int redisRioLoadTimeMillisecond(rio *rdb, long long *val) {
  return (*val = rdbLoadMillisecondTime(rdb)) >= 0 ? 0 : -1;
}

void *redisRioLoadObject(rio *rdb, int typ) { return rdbLoadObject(typ, rdb); }
void *redisRioLoadStringObject(rio *rdb) { return rdbLoadStringObject(rdb); }

void redisSdsFree(void *ptr) { sdsfree(ptr); }

int redisObjectType(void *obj) { return ((robj *)obj)->type; }
int redisObjectEncoding(void *obj) { return ((robj *)obj)->encoding; }
int redisObjectRefCount(void *obj) { return ((robj *)obj)->refcount; }

void redisObjectIncrRefCount(void *obj) { incrRefCount(obj); }
void redisObjectDecrRefCount(void *obj) { decrRefCount(obj); }

extern void createDumpPayload(rio *payload, robj *o);

void *redisObjectCreateDumpPayload(void *obj, size_t *len) {
  rio payload;
  createDumpPayload(&payload, obj);
  sds buf = payload.io.buffer.ptr;
  *len = sdslen(buf);
  return buf;
}

extern int verifyDumpPayload(const char *buf, size_t len);

void *redisObjectDecodeFromPayload(void *buf, size_t len) {
  rio payload;
  if (verifyDumpPayload(buf, len) != C_OK) {
    return NULL;
  }
  int type;
  robj *obj = NULL;
  sds iobuf = sdsnewlen(buf, len);
  rioInitWithBuffer(&payload, iobuf);
  if ((type = rdbLoadObjectType(&payload)) != -1) {
    obj = rdbLoadObject(type, &payload);
  }
  sdsfree(iobuf);
  return obj;
}

size_t redisStringObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_STRING);
  return stringObjectLen(o);
}

void redisStringObjectLoad(void *obj, redisSds *p) {
  robj *o = obj;
  serverAssert(o->type == OBJ_STRING);
  if (sdsEncodedObject(o)) {
    p->ptr = o->ptr, p->len = sdslen(o->ptr);
  } else if (o->encoding == OBJ_ENCODING_INT) {
    p->val = (long)o->ptr;
  } else {
    serverPanic("Unknown string encoding");
  }
}

size_t redisListObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_LIST);
  return listTypeLength(o);
}

void *redisListObjectNewIterator(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_LIST);
  return listTypeInitIterator(o, 0, LIST_TAIL);
}

void redisListIteratorRelease(void *iter) { listTypeReleaseIterator(iter); }

static int redisListIteratorNext(void *iter, redisSds *p) {
  listTypeEntry entry;
  if (!listTypeNext(iter, &entry)) {
    return C_ERR;
  }
  quicklistEntry *qe = &entry.entry;
  if (qe->value) {
    p->ptr = qe->value, p->len = qe->sz;
  } else {
    p->val = qe->longval;
  }
  return C_OK;
}

size_t redisListIteratorLoad(void *iter, redisSds *buf, size_t len) {
  size_t i = 0;
  while (i < len && redisListIteratorNext(iter, &buf[i]) != C_ERR) {
    i++;
  }
  return i;
}

size_t redisHashObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_HASH);
  return hashTypeLength(o);
}

void *redisHashObjectNewIterator(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_HASH);
  return hashTypeInitIterator(o);
}

void redisHashIteratorRelease(void *iter) { hashTypeReleaseIterator(iter); }

static void hashTypeCurrentObjectWrapper(void *iter, redisSds *p, int what) {
  unsigned char *vstr = NULL;
  unsigned int vlen;
  hashTypeCurrentObject(iter, what, &vstr, &vlen, &(p->val));
  if (vstr) {
    p->ptr = vstr, p->len = vlen;
  }
}

static int redisHashIteratorNext(void *iter, redisSds *k, redisSds *v) {
  if (hashTypeNext(iter) != C_OK) {
    return C_ERR;
  }
  hashTypeCurrentObjectWrapper(iter, k, OBJ_HASH_KEY);
  hashTypeCurrentObjectWrapper(iter, v, OBJ_HASH_VALUE);
  return C_OK;
}

size_t redisHashIteratorLoad(void *iter, redisSds *buf, size_t len) {
  serverAssert(len % 2 == 0);
  size_t i = 0;
  while (i < len &&
         redisHashIteratorNext(iter, &buf[i], &buf[i + 1]) != C_ERR) {
    i += 2;
  }
  return i;
}

size_t redisZsetObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_ZSET);
  return zsetLength(o);
}

typedef struct {
  robj *obj;
  int length;
  unsigned char *eptr;
  unsigned char *sptr;
  zskiplistNode *ln;
} redisZsetIterator;

void *redisZsetObjectNewIterator(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_ZSET);

  redisZsetIterator *it = zcalloc(sizeof(*it));
  it->obj = obj, it->length = zsetLength(o);

  if (o->encoding == OBJ_ENCODING_ZIPLIST) {
    unsigned char *zl = o->ptr;
    it->eptr = ziplistIndex(zl, 0);
    serverAssert(it->eptr != NULL);
    it->sptr = ziplistNext(zl, it->eptr);
    serverAssert(it->sptr != NULL);
    return it;
  } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
    zset *zs = o->ptr;
    zskiplist *zsl = zs->zsl;
    it->ln = zsl->header->level[0].forward;
    serverAssert(it->ln != NULL);
    return it;
  } else {
    serverPanic("Unknown sorted set encoding");
  }
}

void redisZsetIteratorRelease(void *iter) { zfree(iter); }

static int redisZsetIteratorNext(void *iter, redisSds *p) {
  redisZsetIterator *it = iter;
  if (it->length == 0) {
    return C_ERR;
  }
  robj *o = it->obj;
  if (o->encoding == OBJ_ENCODING_ZIPLIST) {
    unsigned char *vstr = NULL;
    unsigned int vlen;
    serverAssert(it->eptr != NULL && it->sptr != NULL);
    serverAssert(ziplistGet(it->eptr, &vstr, &vlen, &(p->val)));
    if (vstr) {
      p->ptr = vstr, p->len = vlen;
    }
    p->score = zzlGetScore(it->sptr);
    zzlNext(o->ptr, &it->eptr, &it->sptr);
  } else {
    zskiplistNode *ln = it->ln;
    serverAssert(ln != NULL);
    p->ptr = ln->ele, p->len = sdslen(ln->ele);
    p->score = ln->score;
    it->ln = ln->level[0].forward;
  }
  it->length--;
  return C_OK;
}

size_t redisZsetIteratorLoad(void *iter, redisSds *buf, size_t len) {
  size_t i = 0;
  while (i < len && redisZsetIteratorNext(iter, &buf[i]) != C_ERR) {
    i++;
  }
  return i;
}

size_t redisSetObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_SET);
  return setTypeSize(o);
}

void *redisSetObjectNewIterator(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_SET);
  return setTypeInitIterator(o);
}

void redisSetIteratorRelease(void *iter) { setTypeReleaseIterator(iter); }

int redisSetIteratorNext(void *iter, void **ptr, size_t *len, long long *val) {
  sds value;
  int64_t llele;
  int encoding = setTypeNext(iter, &value, &llele);
  if (encoding != -1) {
    if (encoding != OBJ_ENCODING_INTSET) {
      *ptr = value, *len = sdslen(value);
    } else {
      *val = llele;
    }
    return 0;
  }
  return -1;
}
