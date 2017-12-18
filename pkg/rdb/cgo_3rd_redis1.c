#include <stdio.h>
#include <stdlib.h>
#include <strings.h>

extern long int random(void);

#include <redis/src/adlist.c>
#include <redis/src/crc64.c>
#include <redis/src/dict.c>
#include <redis/src/endianconv.c>
#include <redis/src/intset.c>
#include <redis/src/listpack.c>
#include <redis/src/lzf_c.c>
#include <redis/src/lzf_d.c>
#include <redis/src/quicklist.c>
#include <redis/src/rax.c>
#include <redis/src/sds.c>
#include <redis/src/sha1.c>
#include <redis/src/siphash.c>
#include <redis/src/util.c>
#include <redis/src/ziplist.c>
#include <redis/src/zipmap.c>
#include <redis/src/zmalloc.c>
