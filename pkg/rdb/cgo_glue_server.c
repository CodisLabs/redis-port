#include "cgo_redis.h"

#include <strings.h>
#include <sys/time.h>

struct sharedObjectsStruct shared;
struct redisServer server;

// Copied from redis/src/server.c.
void dictListDestructor(void *privdata, void *val) {
  DICT_NOTUSED(privdata);
  listRelease((list *)val);
}

// Copied from redis/src/server.c.
void dictObjectDestructor(void *privdata, void *val) {
  DICT_NOTUSED(privdata);
  if (val == NULL) return; /* Lazy freeing will set value to NULL. */
  decrRefCount(val);
}

// Copied from redis/src/server.c.
uint64_t dictEncObjHash(const void *key) {
  robj *o = (robj *)key;
  if (sdsEncodedObject(o)) {
    return dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
  } else {
    if (o->encoding == OBJ_ENCODING_INT) {
      char buf[32];
      int len;
      len = ll2string(buf, 32, (long)o->ptr);
      return dictGenHashFunction((unsigned char *)buf, len);
    } else {
      uint64_t hash;
      o = getDecodedObject(o);
      hash = dictGenHashFunction(o->ptr, sdslen((sds)o->ptr));
      decrRefCount(o);
      return hash;
    }
  }
}

// Copied from redis/src/server.c.
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2) {
  int l1, l2;
  DICT_NOTUSED(privdata);
  l1 = sdslen((sds)key1);
  l2 = sdslen((sds)key2);
  if (l1 != l2) return 0;
  return memcmp(key1, key2, l1) == 0;
}

// Copied from redis/src/server.c.
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2) {
  DICT_NOTUSED(privdata);
  return strcasecmp(key1, key2) == 0;
}

// Copied from redis/src/server.c.
uint64_t dictSdsHash(const void *key) {
  return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
}

// Copied from redis/src/server.c.
uint64_t dictSdsCaseHash(const void *key) {
  return dictGenCaseHashFunction((unsigned char *)key, sdslen((char *)key));
}

// Copied from redis/src/server.c.
int dictEncObjKeyCompare(void *privdata, const void *key1, const void *key2) {
  robj *o1 = (robj *)key1, *o2 = (robj *)key2;
  int cmp;
  if (o1->encoding == OBJ_ENCODING_INT && o2->encoding == OBJ_ENCODING_INT)
    return o1->ptr == o2->ptr;
  o1 = getDecodedObject(o1);
  o2 = getDecodedObject(o2);
  cmp = dictSdsKeyCompare(privdata, o1->ptr, o2->ptr);
  decrRefCount(o1);
  decrRefCount(o2);
  return cmp;
}

// Copied from redis/src/server.c.
void dictSdsDestructor(void *privdata, void *val) {
  DICT_NOTUSED(privdata);
  sdsfree(val);
}

// Copied from redis/src/server.c.
dictType objectKeyPointerValueDictType = {
    dictEncObjHash,       /* hash function */
    NULL,                 /* key dup */
    NULL,                 /* val dup */
    dictEncObjKeyCompare, /* key compare */
    dictObjectDestructor, /* key destructor */
    NULL                  /* val destructor */
};

// Copied from redis/src/server.c.
dictType zsetDictType = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    NULL,              /* Note: SDS string shared & freed by skiplist */
    NULL               /* val destructor */
};

// Copied from redis/src/server.c.
dictType hashDictType = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    dictSdsDestructor, /* key destructor */
    dictSdsDestructor  /* val destructor */
};

// Copied from redis/src/server.c.
dictType commandTableDictType = {
    dictSdsCaseHash,       /* hash function */
    NULL,                  /* key dup */
    NULL,                  /* val dup */
    dictSdsKeyCaseCompare, /* key compare */
    dictSdsDestructor,     /* key destructor */
    NULL                   /* val destructor */
};

// Copied from redis/src/server.c.
dictType migrateCacheDictType = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    dictSdsDestructor, /* key destructor */
    NULL               /* val destructor */
};

// Copied from redis/src/server.c.
dictType clusterNodesDictType = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    dictSdsDestructor, /* key destructor */
    NULL               /* val destructor */
};

// Copied from redis/src/server.c.
dictType clusterNodesBlackListDictType = {
    dictSdsCaseHash,       /* hash function */
    NULL,                  /* key dup */
    NULL,                  /* val dup */
    dictSdsKeyCaseCompare, /* key compare */
    dictSdsDestructor,     /* key destructor */
    NULL                   /* val destructor */
};

// Copied from redis/src/server.c.
int htNeedsResize(dict *dict) {
  long long size, used;

  size = dictSlots(dict);
  used = dictSize(dict);
  return (size > DICT_HT_INITIAL_SIZE &&
          (used * 100 / size < HASHTABLE_MIN_FILL));
}

// Copied from redis/src/server.c.
long long ustime(void) {
  struct timeval tv;
  long long ust;
  gettimeofday(&tv, NULL);
  ust = ((long long)tv.tv_sec) * 1000000;
  ust += tv.tv_usec;
  return ust;
}

// Copied from redis/src/server.c.
mstime_t mstime(void) { return ustime() / 1000; }

// Copied from redis/src/server.c.
void createSharedObjects(void) {
  int j;
  shared.crlf = createObject(OBJ_STRING, sdsnew("\r\n"));
  shared.ok = createObject(OBJ_STRING, sdsnew("+OK\r\n"));
  shared.err = createObject(OBJ_STRING, sdsnew("-ERR\r\n"));
  shared.emptybulk = createObject(OBJ_STRING, sdsnew("$0\r\n\r\n"));
  shared.czero = createObject(OBJ_STRING, sdsnew(":0\r\n"));
  shared.cone = createObject(OBJ_STRING, sdsnew(":1\r\n"));
  shared.cnegone = createObject(OBJ_STRING, sdsnew(":-1\r\n"));
  shared.nullbulk = createObject(OBJ_STRING, sdsnew("$-1\r\n"));
  shared.nullmultibulk = createObject(OBJ_STRING, sdsnew("*-1\r\n"));
  shared.emptymultibulk = createObject(OBJ_STRING, sdsnew("*0\r\n"));
  shared.pong = createObject(OBJ_STRING, sdsnew("+PONG\r\n"));
  shared.queued = createObject(OBJ_STRING, sdsnew("+QUEUED\r\n"));
  shared.emptyscan =
      createObject(OBJ_STRING, sdsnew("*2\r\n$1\r\n0\r\n*0\r\n"));
  shared.wrongtypeerr =
      createObject(OBJ_STRING, sdsnew("-WRONGTYPE Operation against a key "
                                      "holding the wrong kind of value\r\n"));
  shared.nokeyerr = createObject(OBJ_STRING, sdsnew("-ERR no such key\r\n"));
  shared.syntaxerr = createObject(OBJ_STRING, sdsnew("-ERR syntax error\r\n"));
  shared.sameobjecterr = createObject(
      OBJ_STRING,
      sdsnew("-ERR source and destination objects are the same\r\n"));
  shared.outofrangeerr =
      createObject(OBJ_STRING, sdsnew("-ERR index out of range\r\n"));
  shared.noscripterr = createObject(
      OBJ_STRING, sdsnew("-NOSCRIPT No matching script. Please use EVAL.\r\n"));
  shared.loadingerr = createObject(
      OBJ_STRING,
      sdsnew("-LOADING Redis is loading the dataset in memory\r\n"));
  shared.slowscripterr = createObject(
      OBJ_STRING, sdsnew("-BUSY Redis is busy running a script. You can only "
                         "call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n"));
  shared.masterdownerr = createObject(
      OBJ_STRING, sdsnew("-MASTERDOWN Link with MASTER is down and "
                         "slave-serve-stale-data is set to 'no'.\r\n"));
  shared.bgsaveerr = createObject(
      OBJ_STRING,
      sdsnew("-MISCONF Redis is configured to save RDB snapshots, but it is "
             "currently not able to persist on disk. Commands that may modify "
             "the data set are disabled, because this instance is configured "
             "to report errors during writes if RDB snapshotting fails "
             "(stop-writes-on-bgsave-error option). Please check the Redis "
             "logs for details about the RDB error.\r\n"));
  shared.roslaveerr = createObject(
      OBJ_STRING,
      sdsnew("-READONLY You can't write against a read only slave.\r\n"));
  shared.noautherr =
      createObject(OBJ_STRING, sdsnew("-NOAUTH Authentication required.\r\n"));
  shared.oomerr = createObject(
      OBJ_STRING,
      sdsnew("-OOM command not allowed when used memory > 'maxmemory'.\r\n"));
  shared.execaborterr =
      createObject(OBJ_STRING, sdsnew("-EXECABORT Transaction discarded "
                                      "because of previous errors.\r\n"));
  shared.noreplicaserr = createObject(
      OBJ_STRING, sdsnew("-NOREPLICAS Not enough good slaves to write.\r\n"));
  shared.busykeyerr = createObject(
      OBJ_STRING, sdsnew("-BUSYKEY Target key name already exists.\r\n"));
  shared.space = createObject(OBJ_STRING, sdsnew(" "));
  shared.colon = createObject(OBJ_STRING, sdsnew(":"));
  shared.plus = createObject(OBJ_STRING, sdsnew("+"));

  for (j = 0; j < PROTO_SHARED_SELECT_CMDS; j++) {
    char dictid_str[64];
    int dictid_len;

    dictid_len = ll2string(dictid_str, sizeof(dictid_str), j);
    shared.select[j] = createObject(
        OBJ_STRING,
        sdscatprintf(sdsempty(), "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                     dictid_len, dictid_str));
  }
  shared.messagebulk = createStringObject("$7\r\nmessage\r\n", 13);
  shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n", 14);
  shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n", 15);
  shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n", 18);
  shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n", 17);
  shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n", 19);
  shared.del = createStringObject("DEL", 3);
  shared.unlink = createStringObject("UNLINK", 6);
  shared.rpop = createStringObject("RPOP", 4);
  shared.lpop = createStringObject("LPOP", 4);
  shared.lpush = createStringObject("LPUSH", 5);
  for (j = 0; j < OBJ_SHARED_INTEGERS; j++) {
    shared.integers[j] =
        makeObjectShared(createObject(OBJ_STRING, (void *)(long)j));
    shared.integers[j]->encoding = OBJ_ENCODING_INT;
  }
  for (j = 0; j < OBJ_SHARED_BULKHDR_LEN; j++) {
    shared.mbulkhdr[j] =
        createObject(OBJ_STRING, sdscatprintf(sdsempty(), "*%d\r\n", j));
    shared.bulkhdr[j] =
        createObject(OBJ_STRING, sdscatprintf(sdsempty(), "$%d\r\n", j));
  }
  /* The following two shared objects, minstring and maxstrings, are not
   * actually used for their value but as a special object meaning
   * respectively the minimum possible string and the maximum possible
   * string in string comparisons for the ZRANGEBYLEX command. */
  shared.minstring = sdsnew("minstring");
  shared.maxstring = sdsnew("maxstring");
}

// Copied from redis/src/server.c.
double R_Zero, R_PosInf, R_NegInf, R_Nan;

#include <redis/src/atomicvar.h>
#include <redis/src/cluster.h>

// Copied from redis/src/server.c.
void initServerConfig(void) {
  int j;

  pthread_mutex_init(&server.next_client_id_mutex, NULL);
  pthread_mutex_init(&server.lruclock_mutex, NULL);
  pthread_mutex_init(&server.unixtime_mutex, NULL);

  getRandomHexChars(server.runid, CONFIG_RUN_ID_SIZE);
  server.runid[CONFIG_RUN_ID_SIZE] = '\0';
  changeReplicationId();
  clearReplicationId2();
  server.configfile = NULL;
  server.executable = NULL;
  server.hz = CONFIG_DEFAULT_HZ;
  server.arch_bits = (sizeof(long) == 8) ? 64 : 32;
  server.port = CONFIG_DEFAULT_SERVER_PORT;
  server.tcp_backlog = CONFIG_DEFAULT_TCP_BACKLOG;
  server.bindaddr_count = 0;
  server.unixsocket = NULL;
  server.unixsocketperm = CONFIG_DEFAULT_UNIX_SOCKET_PERM;
  server.ipfd_count = 0;
  server.sofd = -1;
  server.protected_mode = CONFIG_DEFAULT_PROTECTED_MODE;
  server.dbnum = CONFIG_DEFAULT_DBNUM;
  server.verbosity = CONFIG_DEFAULT_VERBOSITY;
  server.maxidletime = CONFIG_DEFAULT_CLIENT_TIMEOUT;
  server.tcpkeepalive = CONFIG_DEFAULT_TCP_KEEPALIVE;
  server.active_expire_enabled = 1;
  server.active_defrag_enabled = CONFIG_DEFAULT_ACTIVE_DEFRAG;
  server.active_defrag_ignore_bytes = CONFIG_DEFAULT_DEFRAG_IGNORE_BYTES;
  server.active_defrag_threshold_lower = CONFIG_DEFAULT_DEFRAG_THRESHOLD_LOWER;
  server.active_defrag_threshold_upper = CONFIG_DEFAULT_DEFRAG_THRESHOLD_UPPER;
  server.active_defrag_cycle_min = CONFIG_DEFAULT_DEFRAG_CYCLE_MIN;
  server.active_defrag_cycle_max = CONFIG_DEFAULT_DEFRAG_CYCLE_MAX;
  server.client_max_querybuf_len = PROTO_MAX_QUERYBUF_LEN;
  server.saveparams = NULL;
  server.loading = 0;
  server.logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
  server.syslog_enabled = CONFIG_DEFAULT_SYSLOG_ENABLED;
  server.syslog_ident = zstrdup(CONFIG_DEFAULT_SYSLOG_IDENT);
  server.syslog_facility = LOG_LOCAL0;
  server.daemonize = CONFIG_DEFAULT_DAEMONIZE;
  server.supervised = 0;
  server.supervised_mode = SUPERVISED_NONE;
  server.aof_state = AOF_OFF;
  server.aof_fsync = CONFIG_DEFAULT_AOF_FSYNC;
  server.aof_no_fsync_on_rewrite = CONFIG_DEFAULT_AOF_NO_FSYNC_ON_REWRITE;
  server.aof_rewrite_perc = AOF_REWRITE_PERC;
  server.aof_rewrite_min_size = AOF_REWRITE_MIN_SIZE;
  server.aof_rewrite_base_size = 0;
  server.aof_rewrite_scheduled = 0;
  server.aof_last_fsync = time(NULL);
  server.aof_rewrite_time_last = -1;
  server.aof_rewrite_time_start = -1;
  server.aof_lastbgrewrite_status = C_OK;
  server.aof_delayed_fsync = 0;
  server.aof_fd = -1;
  server.aof_selected_db = -1; /* Make sure the first time will not match */
  server.aof_flush_postponed_start = 0;
  server.aof_rewrite_incremental_fsync =
      CONFIG_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC;
  server.aof_load_truncated = CONFIG_DEFAULT_AOF_LOAD_TRUNCATED;
  server.aof_use_rdb_preamble = CONFIG_DEFAULT_AOF_USE_RDB_PREAMBLE;
  server.pidfile = NULL;
  server.rdb_filename = zstrdup(CONFIG_DEFAULT_RDB_FILENAME);
  server.aof_filename = zstrdup(CONFIG_DEFAULT_AOF_FILENAME);
  server.requirepass = NULL;
  server.rdb_compression = CONFIG_DEFAULT_RDB_COMPRESSION;
  server.rdb_checksum = CONFIG_DEFAULT_RDB_CHECKSUM;
  server.stop_writes_on_bgsave_err = CONFIG_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR;
  server.activerehashing = CONFIG_DEFAULT_ACTIVE_REHASHING;
  server.active_defrag_running = 0;
  server.notify_keyspace_events = 0;
  server.maxclients = CONFIG_DEFAULT_MAX_CLIENTS;
  server.blocked_clients = 0;
  memset(server.blocked_clients_by_type, 0,
         sizeof(server.blocked_clients_by_type));
  server.maxmemory = CONFIG_DEFAULT_MAXMEMORY;
  server.maxmemory_policy = CONFIG_DEFAULT_MAXMEMORY_POLICY;
  server.maxmemory_samples = CONFIG_DEFAULT_MAXMEMORY_SAMPLES;
  server.lfu_log_factor = CONFIG_DEFAULT_LFU_LOG_FACTOR;
  server.lfu_decay_time = CONFIG_DEFAULT_LFU_DECAY_TIME;
  server.hash_max_ziplist_entries = OBJ_HASH_MAX_ZIPLIST_ENTRIES;
  server.hash_max_ziplist_value = OBJ_HASH_MAX_ZIPLIST_VALUE;
  server.list_max_ziplist_size = OBJ_LIST_MAX_ZIPLIST_SIZE;
  server.list_compress_depth = OBJ_LIST_COMPRESS_DEPTH;
  server.set_max_intset_entries = OBJ_SET_MAX_INTSET_ENTRIES;
  server.zset_max_ziplist_entries = OBJ_ZSET_MAX_ZIPLIST_ENTRIES;
  server.zset_max_ziplist_value = OBJ_ZSET_MAX_ZIPLIST_VALUE;
  server.hll_sparse_max_bytes = CONFIG_DEFAULT_HLL_SPARSE_MAX_BYTES;
  server.shutdown_asap = 0;
  server.cluster_enabled = 0;
  server.cluster_node_timeout = CLUSTER_DEFAULT_NODE_TIMEOUT;
  server.cluster_migration_barrier = CLUSTER_DEFAULT_MIGRATION_BARRIER;
  server.cluster_slave_validity_factor = CLUSTER_DEFAULT_SLAVE_VALIDITY;
  server.cluster_require_full_coverage = CLUSTER_DEFAULT_REQUIRE_FULL_COVERAGE;
  server.cluster_configfile = zstrdup(CONFIG_DEFAULT_CLUSTER_CONFIG_FILE);
  server.cluster_announce_ip = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_IP;
  server.cluster_announce_port = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_PORT;
  server.cluster_announce_bus_port = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_BUS_PORT;
  server.migrate_cached_sockets = dictCreate(&migrateCacheDictType, NULL);
  server.next_client_id = 1; /* Client IDs, start from 1 .*/
  server.loading_process_events_interval_bytes = (1024 * 1024 * 2);
  server.lazyfree_lazy_eviction = CONFIG_DEFAULT_LAZYFREE_LAZY_EVICTION;
  server.lazyfree_lazy_expire = CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE;
  server.lazyfree_lazy_server_del = CONFIG_DEFAULT_LAZYFREE_LAZY_SERVER_DEL;
  server.always_show_logo = CONFIG_DEFAULT_ALWAYS_SHOW_LOGO;
  server.lua_time_limit = LUA_SCRIPT_TIME_LIMIT;

  unsigned int lruclock = getLRUClock();
  atomicSet(server.lruclock, lruclock);
  resetServerSaveParams();

  appendServerSaveParams(60 * 60, 1); /* save after 1 hour and 1 change */
  appendServerSaveParams(300, 100);   /* save after 5 minutes and 100 changes */
  appendServerSaveParams(60, 10000); /* save after 1 minute and 10000 changes */

  /* Replication related */
  server.masterauth = NULL;
  server.masterhost = NULL;
  server.masterport = 6379;
  server.master = NULL;
  server.cached_master = NULL;
  server.master_initial_offset = -1;
  server.repl_state = REPL_STATE_NONE;
  server.repl_syncio_timeout = CONFIG_REPL_SYNCIO_TIMEOUT;
  server.repl_serve_stale_data = CONFIG_DEFAULT_SLAVE_SERVE_STALE_DATA;
  server.repl_slave_ro = CONFIG_DEFAULT_SLAVE_READ_ONLY;
  server.repl_slave_lazy_flush = CONFIG_DEFAULT_SLAVE_LAZY_FLUSH;
  server.repl_down_since = 0; /* Never connected, repl is down since EVER. */
  server.repl_disable_tcp_nodelay = CONFIG_DEFAULT_REPL_DISABLE_TCP_NODELAY;
  server.repl_diskless_sync = CONFIG_DEFAULT_REPL_DISKLESS_SYNC;
  server.repl_diskless_sync_delay = CONFIG_DEFAULT_REPL_DISKLESS_SYNC_DELAY;
  server.repl_ping_slave_period = CONFIG_DEFAULT_REPL_PING_SLAVE_PERIOD;
  server.repl_timeout = CONFIG_DEFAULT_REPL_TIMEOUT;
  server.repl_min_slaves_to_write = CONFIG_DEFAULT_MIN_SLAVES_TO_WRITE;
  server.repl_min_slaves_max_lag = CONFIG_DEFAULT_MIN_SLAVES_MAX_LAG;
  server.slave_priority = CONFIG_DEFAULT_SLAVE_PRIORITY;
  server.slave_announce_ip = CONFIG_DEFAULT_SLAVE_ANNOUNCE_IP;
  server.slave_announce_port = CONFIG_DEFAULT_SLAVE_ANNOUNCE_PORT;
  server.master_repl_offset = 0;

  /* Replication partial resync backlog */
  server.repl_backlog = NULL;
  server.repl_backlog_size = CONFIG_DEFAULT_REPL_BACKLOG_SIZE;
  server.repl_backlog_histlen = 0;
  server.repl_backlog_idx = 0;
  server.repl_backlog_off = 0;
  server.repl_backlog_time_limit = CONFIG_DEFAULT_REPL_BACKLOG_TIME_LIMIT;
  server.repl_no_slaves_since = time(NULL);

  /* Client output buffer limits */
  for (j = 0; j < CLIENT_TYPE_OBUF_COUNT; j++)
    server.client_obuf_limits[j] = clientBufferLimitsDefaults[j];

  /* Double constants initialization */
  R_Zero = 0.0;
  R_PosInf = 1.0 / R_Zero;
  R_NegInf = -1.0 / R_Zero;
  R_Nan = R_Zero / R_Zero;

  /* Command table -- we initiialize it here as it is part of the
   * initial configuration, since command names may be changed via
   * redis.conf using the rename-command directive. */
  server.commands = dictCreate(&commandTableDictType, NULL);
  server.orig_commands = dictCreate(&commandTableDictType, NULL);
  populateCommandTable();
  server.delCommand = lookupCommandByCString("del");
  server.multiCommand = lookupCommandByCString("multi");
  server.lpushCommand = lookupCommandByCString("lpush");
  server.lpopCommand = lookupCommandByCString("lpop");
  server.rpopCommand = lookupCommandByCString("rpop");
  server.sremCommand = lookupCommandByCString("srem");
  server.execCommand = lookupCommandByCString("exec");
  server.expireCommand = lookupCommandByCString("expire");
  server.pexpireCommand = lookupCommandByCString("pexpire");

  /* Slow log */
  server.slowlog_log_slower_than = CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN;
  server.slowlog_max_len = CONFIG_DEFAULT_SLOWLOG_MAX_LEN;

  /* Latency monitor */
  server.latency_monitor_threshold = CONFIG_DEFAULT_LATENCY_MONITOR_THRESHOLD;

  /* Debugging */
  server.assert_failed = "<no assertion failed>";
  server.assert_file = "<no file>";
  server.assert_line = 0;
  server.bug_report_start = 0;
  server.watchdog_period = 0;
}
