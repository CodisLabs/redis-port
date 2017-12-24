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
