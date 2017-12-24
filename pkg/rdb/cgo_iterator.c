#include "cgo_redis.h"

static redisTypeIterator *redisTypeIteratorInit() {
  return zcalloc(sizeof(redisTypeIterator));
}

static void redisTypeIteratorFree(redisTypeIterator *p) { zfree(p); }

void redisTypeIteratorLoad(redisTypeIterator *p) {
  p->index = 0;
  p->slice.len = p->load(p->iter, p->slice.buf,
                         sizeof(p->slice.buf) / sizeof(p->slice.buf[0]));
}

static int redisListIteratorNext(void *iter, redisSds *p) {
  listTypeEntry entry;
  if (!listTypeNext(iter, &entry)) {
    return C_ERR;
  }
  memset(p, 0, sizeof(*p));

  quicklistEntry *qe = &entry.entry;
  if (qe->value) {
    p->ptr = qe->value, p->len = qe->sz;
  } else {
    p->val = qe->longval;
  }
  return C_OK;
}

static size_t redisListIteratorLoad(void *iter, redisSds *buf, size_t len) {
  size_t i = 0;
  while (i < len && redisListIteratorNext(iter, &buf[i]) != C_ERR) {
    i++;
  }
  return i;
}

redisTypeIterator *redisListObjectNewIterator(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_LIST);
  redisTypeIterator *p = redisTypeIteratorInit();
  p->iter = listTypeInitIterator(o, 0, LIST_TAIL);
  p->load = redisListIteratorLoad;
  return p;
}

void redisListIteratorRelease(redisTypeIterator *p) {
  listTypeReleaseIterator(p->iter);
  redisTypeIteratorFree(p);
}

static void hashTypeCurrentObjectWrapper(void *iter, redisSds *p, int what) {
  unsigned char *vstr = NULL;
  unsigned int vlen;
  hashTypeCurrentObject(iter, what, &vstr, &vlen, &p->val);
  if (vstr) {
    p->ptr = vstr, p->len = vlen;
  }
}

static int redisHashIteratorNext(void *iter, redisSds *p) {
  if (hashTypeNext(iter) != C_OK) {
    return C_ERR;
  }
  memset(p, 0, sizeof(*p) * 2);

  hashTypeCurrentObjectWrapper(iter, &p[0], OBJ_HASH_KEY);
  hashTypeCurrentObjectWrapper(iter, &p[1], OBJ_HASH_VALUE);
  return C_OK;
}

static size_t redisHashIteratorLoad(void *iter, redisSds *buf, size_t len) {
  serverAssert(len % 2 == 0);
  size_t i = 0;
  while (i < len && redisHashIteratorNext(iter, &buf[i]) != C_ERR) {
    i += 2;
  }
  return i;
}

redisTypeIterator *redisHashObjectNewIterator(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_HASH);
  redisTypeIterator *p = redisTypeIteratorInit();
  p->iter = hashTypeInitIterator(o);
  p->load = redisHashIteratorLoad;
  return p;
}

void redisHashIteratorRelease(redisTypeIterator *p) {
  hashTypeReleaseIterator(p->iter);
  redisTypeIteratorFree(p);
}
