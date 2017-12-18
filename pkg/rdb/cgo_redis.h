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
