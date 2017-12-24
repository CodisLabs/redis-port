#include <redis/src/server.h>

void initRedisServer(char *config);

/* API of Redis Rio/Rdb */

#define REDIS_RIO_BUFSIZE (1024 * 16)

typedef struct {
  rio rdb;
  size_t pos, end;
  char buf[REDIS_RIO_BUFSIZE];
} redisRio;

void redisRioInit(redisRio *p);

int redisRioRead(redisRio *p, void *buf, size_t len);
int redisRioLoadLen(redisRio *p, uint64_t *len);
int redisRioLoadType(redisRio *p, int *typ);
int redisRioLoadTime(redisRio *p, time_t *val);
int redisRioLoadTimeMillisecond(redisRio *p, long long *val);

void *redisRioLoadObject(redisRio *p, int typ);
void *redisRioLoadStringObject(redisRio *p);

inline uint64_t redisRioChecksum(redisRio *p) { return p->rdb.cksum; }

/* API of Sds */
typedef struct {
  void *ptr;
  size_t len;
  long long val;
  double score;
} redisSds;

inline void redisSdsFreePtr(void *ptr) { sdsfree(ptr); }

typedef struct {
  void *iter;
  struct {
    size_t len;
    redisSds buf[512];
  } slice;
  size_t index;
  size_t (*load)(void *iter, redisSds *buf, size_t len);
} redisTypeIterator;

void redisTypeIteratorLoad(redisTypeIterator *p);

/* API of redis Object */
int redisObjectType(void *obj);
int redisObjectEncoding(void *obj);
int redisObjectRefCount(void *obj);

void redisObjectIncrRefCount(void *obj);
void redisObjectDecrRefCount(void *obj);

void redisObjectCreateDumpPayload(void *obj, redisSds *p);
void *redisObjectDecodeFromPayload(void *buf, size_t len);

/* API of redis String */
size_t redisStringObjectLen(void *obj);
void redisStringObjectLoad(void *obj, redisSds *sds);

/* API of redis List */
size_t redisListObjectLen(void *obj);
void *redisListObjectNewIterator(void *obj);
void redisListIteratorRelease(void *iter);
size_t redisListIteratorLoad(void *iter, redisSds *buf, size_t len);

/* API of redis Hash */
size_t redisHashObjectLen(void *obj);
void *redisHashObjectNewIterator(void *obj);
void redisHashIteratorRelease(void *iter);
size_t redisHashIteratorLoad(void *iter, redisSds *buf, size_t len);

/* API of redis Zset */
size_t redisZsetObjectLen(void *obj);
void *redisZsetObjectNewIterator(void *obj);
void redisZsetIteratorRelease(void *iter);
size_t redisZsetIteratorLoad(void *iter, redisSds *buf, size_t len);

/* API of redis Set */
size_t redisSetObjectLen(void *obj);
void *redisSetObjectNewIterator(void *obj);
void redisSetIteratorRelease(void *iter);
size_t redisSetIteratorLoad(void *iter, redisSds *buf, size_t len);

/* API of iterator loader */
typedef size_t (*redisTypeIteratorLoader)(void *iter, redisSds *buf,
                                          size_t len);
size_t redisTypeIteratorLoaderInvoke(redisTypeIteratorLoader *loader,
                                     void *iter, redisSds *buf, size_t len);

/* API of redis zmalloc */
extern size_t zmalloc_used_memory(void);
extern size_t zmalloc_memory_size(void);
extern size_t zmalloc_get_rss(void);
