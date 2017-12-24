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

typedef struct {
  robj *obj;
  size_t length;
  unsigned char *eptr;
  unsigned char *sptr;
  zskiplistNode *ln;
} redisZsetIterator;

static int redisZsetIteratorNext(void *iter, redisSds *p) {
  redisZsetIterator *it = iter;
  if (it->length == 0) {
    return C_ERR;
  }
  memset(p, 0, sizeof(*p));

  robj *o = it->obj;
  if (o->encoding == OBJ_ENCODING_ZIPLIST) {
    unsigned char *vstr = NULL;
    unsigned int vlen;
    serverAssert(it->eptr != NULL && it->sptr != NULL);
    serverAssert(ziplistGet(it->eptr, &vstr, &vlen, &p->val));
    if (vstr) {
      p->ptr = vstr, p->len = vlen;
    }
    p->score = zzlGetScore(it->sptr);
    zzlNext(o->ptr, &it->eptr, &it->sptr);
  } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
    zskiplistNode *ln = it->ln;
    serverAssert(ln != NULL);
    p->ptr = ln->ele, p->len = sdslen(ln->ele);
    p->score = ln->score;
    it->ln = ln->level[0].forward;
  } else {
    serverPanic("Unknown sorted set encoding.");
  }
  it->length--;
  return C_OK;
}

static size_t redisZsetIteratorLoad(void *iter, redisSds *buf, size_t len) {
  size_t i = 0;
  while (i < len && redisZsetIteratorNext(iter, &buf[i]) != C_ERR) {
    i++;
  }
  return i;
}

redisTypeIterator *redisZsetObjectNewIterator(void *obj) {
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
  } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
    zset *zs = o->ptr;
    zskiplist *zsl = zs->zsl;
    it->ln = zsl->header->level[0].forward;
    serverAssert(it->ln != NULL);
  } else {
    serverPanic("Unknown sorted set encoding.");
  }

  redisTypeIterator *p = redisTypeIteratorInit();
  p->iter = it;
  p->load = redisZsetIteratorLoad;
  return p;
}

void redisZsetIteratorRelease(redisTypeIterator *p) {
  zfree(p->iter);
  redisTypeIteratorFree(p);
}

static int redisSetIteratorNext(void *iter, redisSds *p) {
  sds value;
  int64_t llele;
  int encoding = setTypeNext(iter, &value, &llele);
  if (encoding == -1) {
    return C_ERR;
  }
  memset(p, 0, sizeof(*p));

  if (encoding == OBJ_ENCODING_HT) {
    p->ptr = value, p->len = sdslen(value);
  } else if (encoding == OBJ_ENCODING_INTSET) {
    p->val = llele;
  } else {
    serverPanic("Unknown set encoding.");
  }
  return C_OK;
}

static size_t redisSetIteratorLoad(void *iter, redisSds *buf, size_t len) {
  size_t i = 0;
  while (i < len && redisSetIteratorNext(iter, &buf[i]) != C_ERR) {
    i++;
  }
  return i;
}

redisTypeIterator *redisSetObjectNewIterator(void *obj) {
  robj *o = obj;
  serverAssert(o->type == OBJ_SET);
  redisTypeIterator *p = redisTypeIteratorInit();
  p->iter = setTypeInitIterator(o);
  p->load = redisSetIteratorLoad;
  return p;
}

void redisSetIteratorRelease(redisTypeIterator *p) {
  setTypeReleaseIterator(p->iter);
  redisTypeIteratorFree(p);
}
