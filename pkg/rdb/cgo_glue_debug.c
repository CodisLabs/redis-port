#include "cgo_redis.h"

#ifdef LOG_MAX_LEN
#undef LOG_MAX_LEN
#endif

#define LOG_MAX_LEN (1024 * 512)

extern void cgoRedisLogPanic(const char *buf, size_t len);

void _serverAssert(const char *estr, const char *file, int line) {
  char buf[LOG_MAX_LEN];
  const char *format =
      "===== ASSERTION FAILED =====\n"
      "====> %s:%d '%s' is not true\n";
  size_t len = snprintf(buf, sizeof(buf), format, file, line, estr);

  cgoRedisLogPanic(buf, len);
  exit(1);
}

void _serverAssertWithInfo(const client *c, const robj *o, const char *estr,
                           const char *file, int line) {
  _serverAssert(estr, file, line);
}

void _serverPanic(const char *file, int line, const char *vformat, ...) {
  char buf[LOG_MAX_LEN];
  const char *format =
      "===== REDIS CGO PANIC ======\n"
      "====> %s:%d\n";
  size_t len1 = snprintf(buf, sizeof(buf), format, file, line);

  va_list ap;
  va_start(ap, vformat);
  size_t len2 = vsnprintf(buf + len1, sizeof(buf) - len1, vformat, ap);
  va_end(ap);

  cgoRedisLogPanic(buf, len1 + len2);
  exit(1);
}

extern void cgoRedisLogLevel(const char *buf, size_t len, int level);

void serverLog(int level, const char *format, ...) {
  char buf[LOG_MAX_LEN];
  va_list ap;
  va_start(ap, format);
  size_t len = vsnprintf(buf, sizeof(buf), format, ap);
  va_end(ap);

  cgoRedisLogLevel(buf, len, level);
}

int rdbCheckMode = 0;

void rdbCheckError(const char *vformat, ...) {
  char buf[LOG_MAX_LEN];
  const char *format =
      "===== RDB CHECK ERROR ======\n"
      "====> %s:%d\n";
  size_t len1 = snprintf(buf, sizeof(buf), format, __FILE__, __LINE__);

  va_list ap;
  va_start(ap, vformat);
  size_t len2 = vsnprintf(buf + len1, sizeof(buf) - len1, vformat, ap);
  va_end(ap);

  cgoRedisLogPanic(buf, len1 + len2);
  exit(1);
}

void rdbCheckSetError(const char *vformat, ...) {
  char buf[LOG_MAX_LEN];
  const char *format =
      "===== RDB CHECK ERROR ======\n"
      "====> %s:%d\n";
  size_t len1 = snprintf(buf, sizeof(buf), format, __FILE__, __LINE__);

  va_list ap;
  va_start(ap, vformat);
  size_t len2 = vsnprintf(buf + len1, sizeof(buf) - len1, vformat, ap);
  va_end(ap);

  cgoRedisLogPanic(buf, len1 + len2);
  exit(1);
}
