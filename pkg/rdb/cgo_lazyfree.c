#include "cgo_redis.h"

#define LAZYFREE_NUM_THREADS 8

typedef struct {
  pthread_t threads[LAZYFREE_NUM_THREADS];
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  list *objs;
} lazyfreeThreads;

static void *lazyfreeThreadMain(void *args) {
  lazyfreeThreads *p = args;
  while (1) {
    pthread_mutex_lock(&p->mutex);
    while (listLength(p->objs) == 0) {
      pthread_cond_wait(&p->cond, &p->mutex);
    }
    listNode *head = listFirst(p->objs);
    robj *o = listNodeValue(head);
    listDelNode(p->objs, head);
    pthread_mutex_unlock(&p->mutex);
    serverAssert(o->refcount == 1);
    decrRefCount(o);
  }
  return NULL;
}

static lazyfreeThreads *createLazyfreeThreads(void) {
  lazyfreeThreads *p = zmalloc(sizeof(*p));
  pthread_mutex_init(&p->mutex, NULL);
  pthread_cond_init(&p->cond, NULL);
  p->objs = listCreate();
  for (int i = 0; i < LAZYFREE_NUM_THREADS; i++) {
    int ret = pthread_create(&p->threads[i], NULL, lazyfreeThreadMain, p);
    if (ret != 0) {
      serverPanic("Can't create Lazyfree Threads-[%d] [%d].", i, ret);
    }
  }
  return p;
}

static lazyfreeThreads *lazyfree_threads = NULL;

void initLazyfreeThreads() { lazyfree_threads = createLazyfreeThreads(); }

void decrRefCountByLazyfreeThreads(robj *o) {
  serverAssert(o->refcount == 1 && lazyfree_threads != NULL);
  lazyfreeThreads *p = lazyfree_threads;
  pthread_mutex_lock(&p->mutex);
  if (listLength(p->objs) == 0) {
    pthread_cond_signal(&p->cond);
  }
  listAddNodeTail(p->objs, o);
  pthread_mutex_unlock(&p->mutex);
}

size_t lazyfreeObjectGetFreeEffort(robj *o) {
  if (o->refcount == 1 && lazyfree_threads != NULL) {
    switch (o->type) {
    case OBJ_LIST:
      if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        return listTypeLength(o);
      }
      return 0;
    case OBJ_HASH:
      if (o->encoding == OBJ_ENCODING_HT) {
        return hashTypeLength(o) * 2;
      }
      return 0;
    case OBJ_SET:
      if (o->encoding == OBJ_ENCODING_HT) {
        return setTypeSize(o);
      }
      return 0;
    case OBJ_ZSET:
      if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        return zsetLength(o) * 2;
      }
      return 0;
    }
  }
  return 0;
}
