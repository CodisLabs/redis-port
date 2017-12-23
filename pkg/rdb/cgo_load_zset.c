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

robj* rdbLoadZsetObject(int rdbtype, rio* rdb) {
  return rdbLoadObject(rdbtype, rdb);
}
