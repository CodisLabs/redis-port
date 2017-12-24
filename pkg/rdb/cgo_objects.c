#include "cgo_redis.h"

int redisObjectType(void *obj) { return ((robj *)obj)->type; }
int redisObjectEncoding(void *obj) { return ((robj *)obj)->encoding; }
int redisObjectRefCount(void *obj) { return ((robj *)obj)->refcount; }

extern size_t lazyfreeObjectGetFreeEffort(robj *o);
extern void decrRefCountByLazyfreeThreads(robj *o);

void redisObjectIncrRefCount(void *obj) { incrRefCount(obj); }

void redisObjectDecrRefCount(void *obj) {
  if (lazyfreeObjectGetFreeEffort((robj *)obj) < 128) {
    decrRefCount(obj);
  } else {
    decrRefCountByLazyfreeThreads((robj *)obj);
  }
}

extern void createDumpPayload(rio *payload, robj *o);

void redisObjectCreateDumpPayload(void *obj, redisSds *p) {
  rio payload;
  createDumpPayload(&payload, obj);
  p->ptr = payload.io.buffer.ptr;
  p->len = sdslen(p->ptr);
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
  memset(p, 0, sizeof(*p));

  if (sdsEncodedObject(o)) {
    p->ptr = o->ptr, p->len = sdslen(o->ptr);
  } else if (o->encoding == OBJ_ENCODING_INT) {
    p->val = (long)o->ptr;
  } else {
    serverPanic("Unknown string encoding.");
  }
}

size_t redisListObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_LIST);
  return listTypeLength(o);
}

size_t redisHashObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_HASH);
  return hashTypeLength(o);
}

size_t redisZsetObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_ZSET);
  return zsetLength(o);
}

size_t redisSetObjectLen(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_SET);
  return setTypeSize(o);
}
