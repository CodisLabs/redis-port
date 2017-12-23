#include "cgo_redis.h"

typedef struct {
  sds sdsele;
  double score;
} zsetNode;

typedef struct {
  size_t len, cap;
  zsetNode* buf;
} zsetNodeVector;

static zsetNodeVector* zsetNodeVectorInit(size_t cap) {
  zsetNodeVector* v = zmalloc(sizeof(*v));
  v->len = 0, v->cap = cap;
  if (v->cap != 0) {
    v->buf = zmalloc(sizeof(v->buf[0]) * v->cap);
  } else {
    v->buf = NULL;
  }
  return v;
}

static void zsetNodeVectorFree(zsetNodeVector* v) {
  zfree(v->buf);
  zfree(v);
}

static size_t zsetNodeVectorPush(zsetNodeVector* v, sds sdsele, double score) {
  if (v->len == v->cap) {
    v->cap = (v->cap != 0) ? v->cap * 4 : 1024;
    v->buf = zrealloc(v->buf, sizeof(v->buf[0]) * v->cap);
  }
  zsetNode node = {.sdsele = sdsele, .score = score};
  memcpy(v->buf + (v->len++), &node, sizeof(node));
  return v->len;
}

static int zsetNodeCompareInOrder(const void* node1, const void* node2) {
  double score1 = ((zsetNode*)node1)->score;
  double score2 = ((zsetNode*)node2)->score;
  return score1 < score2 ? -1 : (score1 > score2 ? 1 : 0);
}

static int zsetNodeCompareInReverseOrder(const void* node1, const void* node2) {
  return zsetNodeCompareInOrder(node2, node1);
}

static int zsetNodeVectorIsSorted(zsetNodeVector* v,
                                  int (*compare)(const void*, const void*)) {
  for (size_t i = 0, j = 1; j < v->len; i++, j++) {
    if (compare(&v->buf[i], &v->buf[j]) > 0) {
      return 0;
    }
  }
  return 1;
}

static void zsetNodeVectorSort(zsetNodeVector* v,
                               int (*compare)(const void*, const void*)) {
  if (!zsetNodeVectorIsSorted(v, compare)) {
    qsort(v->buf, v->len, sizeof(v->buf[0]), compare);
  }
}

extern int rdbLoadDoubleValue(rio* rdb, double* val);

robj* rdbLoadZsetObject(int rdbtype, rio* rdb) {
  serverAssert(rdbtype == RDB_TYPE_ZSET || rdbtype == RDB_TYPE_ZSET_2);

  /* Copied from redis/src/rdb.c */
  uint64_t zsetlen;
  if ((zsetlen = rdbLoadLen(rdb, NULL)) == RDB_LENERR) return NULL;

  robj* o = createZsetObject();

  /* Create zsetNodeVector if zset's length is greater than the threshold. */
  zsetNodeVector* v = (zsetlen < 128) ? NULL : zsetNodeVectorInit(zsetlen);

  zset* zs = o->ptr;

  serverAssert(zsetlen != 0);
  dictExpand(zs->dict, zsetlen);

  size_t maxelelen = 0;

  /* Load every single element of the sorted set. */
  while (zsetlen--) {
    sds sdsele;
    double score;

    if ((sdsele = rdbGenericLoadStringObject(rdb, RDB_LOAD_SDS, NULL)) == NULL)
      return NULL;
    if (rdbtype == RDB_TYPE_ZSET_2) {
      if (rdbLoadBinaryDoubleValue(rdb, &score) == -1) return NULL;
    } else {
      if (rdbLoadDoubleValue(rdb, &score) == -1) return NULL;
    }

    /* Don't care about integer-encoded strings. */
    if (sdslen(sdsele) > maxelelen) maxelelen = sdslen(sdsele);

    if (v != NULL) {
      zsetNodeVectorPush(v, sdsele, score);
    } else {
      zskiplistNode* znode = zslInsert(zs->zsl, score, sdsele);
      dictAdd(zs->dict, sdsele, &znode->score);
    }
  }

  if (v != NULL) {
    zsetNodeVectorSort(v, zsetNodeCompareInReverseOrder);
    for (size_t i = 0; i < v->len; i++) {
      zsetNode* n = &v->buf[i];
      zskiplistNode* znode = zslInsert(zs->zsl, n->score, n->sdsele);
      dictAdd(zs->dict, n->sdsele, &znode->score);
    }
    zsetNodeVectorFree(v);
  }

  /* Convert *after* loading, since sorted sets are not stored ordered. */
  if (zsetLength(o) <= server.zset_max_ziplist_entries &&
      maxelelen <= server.zset_max_ziplist_value) {
    zsetConvert(o, OBJ_ENCODING_ZIPLIST);
  }
  return o;
}
