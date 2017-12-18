#include "cgo_redis.h"

static const rio redisRioIO = {0};

void redisRioInit(rio *rdb) { *rdb = redisRioIO; }
