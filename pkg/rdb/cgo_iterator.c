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
