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
