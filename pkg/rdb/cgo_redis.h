#include <redis/src/server.h>

void initRedisServer(const void *buf, size_t len);

/* API for Redis Rio/Rdb */
void redisRioInit(rio *rdb);
