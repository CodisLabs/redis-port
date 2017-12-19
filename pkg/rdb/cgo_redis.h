#include <redis/src/server.h>

void initRedisServer(const void *buf, size_t len);

/* API for Redis Rio/Rdb */
void redisRioInit(rio *rdb);

int redisRioRead(rio *rdb, void *buf, size_t len);
int redisRioLoadLen(rio *rdb, uint64_t *len);
int redisRioLoadType(rio *rdb, int *typ);
int redisRioLoadTime(rio *rdb, time_t *val);
int redisRioLoadTimeMillisecond(rio *rdb, long long *val);

void *redisRioLoadObject(rio *rdb, int typ);
void *redisRioLoadStringObject(rio *rdb);

/* API for Sds */
void redisSdsFree(void *buf);

/* API for redisObject */
int redisObjectType(void *obj);
int redisObjectEncoding(void *obj);
int redisObjectRefCount(void *obj);

void redisObjectIncrRefCount(void *obj);
void redisObjectDecrRefCount(void *obj);

void *redisObjectCreateDumpPayload(void *obj, size_t *len);
void *redisObjectDecodeFromPayload(void *buf, size_t len);

/* API for redisObject:string */
size_t redisStringObjectLen(void *obj);
void *redisStringObjectUnsafeSds(void *obj, size_t *len, long long *val);

/* API for redisObject:list */
size_t redisListObjectLen(void *obj);
void *redisListObjectNewIterator(void *obj);
void redisListIteratorRelease(void *iter);
int redisListIteratorNext(void *iter, void **ptr, size_t *len, long long *val);

/* API for redisObject:hash */
size_t redisHashObjectLen(void *obj);
void *redisHashObjectNewIterator(void *obj);
void redisHashIteratorRelease(void *iter);
int redisHashIteratorNext(void *iter, void **kptr, size_t *klen,
                          long long *kval, void **vptr, size_t *vlen,
                          long long *vval);

/* API for redisObject:zset */
size_t redisZsetObjectLen(void *obj);

/* API for redisObject:set */
size_t redisSetObjectLen(void *obj);
