redis-port
===========

[![Build Status](https://travis-ci.org/CodisLabs/redis-port.svg)](https://travis-ci.org/CodisLabs/redis-port)

parse redis rdb file, sync data between redis master and slave

* **DECODE** dumped payload to human readable format (hex-encoding)

```sh
redis-port decode    [--ncpu=N] [--parallel=M] \
    [--input=INPUT] \
    [--output=OUTPUT]
```

* **RESTORE** rdb file to target redis

```sh
redis-port restore   [--ncpu=N] [--parallel=M] \
    [--input=INPUT]  [--faketime=FAKETIME] [--extra] [--filterdb=DB] \
     --target=TARGET [--auth=AUTH] [--redis|--codis]
```

* **DUMP** rdb file from master redis

```sh
redis-port dump      [--ncpu=N] [--parallel=M] \
     --from=MASTER   [--password=PASSWORD] [--extra] \
    [--output=OUTPUT]
```

* **SYNC** data from master to slave

```sh
redis-port sync     [--ncpu=N]  [--parallel=M]   --from=MASTER  \
    [--password=PASSWORD] [--psync] [--filterdb=DB] --target=TARGET \
    [--auth=AUTH] [--redis|--codis] [--sockfile=FILE [--filesize=SIZE]] \
    [--keyfile=FILE] [--keypattern=string]
```

Options
-------
+ -n _N_, --ncpu=_N_

> set runtime.GOMAXPROCS to _N_

+ -p _M_, --parallel=_M_

> set number of parallel routines

+ -i _INPUT_, --input=_INPUT_

> use _INPUT_ as input file, or if it is not given, redis-port reads from stdin (means '/dev/stdin')

+ -o _OUTPUT_, --output=_OUTPUT_

> use _OUTPUT_ as output file, or if it is not given, redis-port writes to stdout (means '/dev/stdout')

+ -m _MASTER_, --master=_MASTER_

> specify the master redis

+ -t _TARGET_, --target=_TARGET_

> specify the slave redis (or target redis)

+ -P PASSWORD, --password=PASSWORD

> specify the redis auth password

+ -A AUTH, --auth=AUTH

> specify the auth password for target

+ -e, --extra

> dump or restore following redis backlog commands

+ --redis

> target is normal redis instance, default value is **false**.

+ --codis

> target is codis proxy, default value is **true**.

+ --filterdb=DB

> filter specifed db number, default value is '*'

Examples
-------

* **DECODE**

```sh
$ cat dump.rdb | ./redis-port decode 2>/dev/null
 {"db":0,"type":"string","key":"a","value":"hello"}
 {"db":1,"type":"string","key":"a","value":"9"}
 {"db":0,"type":"hash","key":"c","field":"hello","value":"world"}
 {"db":0,"type":"expire","key":"c","expireat":1487663341422}
 {"db":0,"type":"list","key":"b","index":0,"value":"hello"}
 {"db":0,"type":"list","key":"b","index":1,"value":"world"}
 {"db":0,"type":"zset","key":"d","member":"hello","score":1}
 {"db":0,"type":"zset","key":"d","member":"world","score":1.8}
  ... ...
```

* **RESTORE**

```sh
$ ./redis-port restore -i dump.rdb -t 127.0.0.1:6379 -n 8
  2014/10/28 15:08:26 [ncpu=8] restore from 'dump.rdb' to '127.0.0.1:6379'
  2014/10/28 15:08:27 total = 280149161 -     14267777 [  5%]
  2014/10/28 15:08:28 total = 280149161 -     27325530 [  9%]
  2014/10/28 15:08:29 total = 280149161 -     40670677 [ 14%]
  ... ...                                                    
  2014/10/28 15:08:47 total = 280149161 -    278070563 [ 99%]
  2014/10/28 15:08:47 total = 280149161 -    280149161 [100%]
  2014/10/28 15:08:47 done
```

* **DUMP**

```sh
$ ./redis-port dump -f 127.0.0.1:6379 -o save.rdb
  2014/10/28 15:12:05 [ncpu=1] dump from '127.0.0.1:6379' to 'save.rdb'
  2014/10/28 15:12:06 -
  2014/10/28 15:12:07 -
  ... ...
  2014/10/28 15:12:10 total = 278110192 -            0 [  0%]
  2014/10/28 15:12:11 total = 278110192 -    278110192 [100%]
  2014/10/28 15:12:11 done

$ ./redis-port dump -f 127.0.0.1:6379 | tee save.rdb | ./redis-port decode -o save.log -n 8 2>/dev/null
  2014/10/28 15:12:55 [ncpu=1] dump from '127.0.0.1:6379' to '/dev/stdout'
  2014/10/28 15:12:56 -
  ... ...
  2014/10/28 15:13:10 total = 278110192  -   264373070 [  0%]
  2014/10/28 15:13:11 total = 278110192  -   278110192 [100%]
  2014/10/28 15:13:11 done
```

* **SYNC**

```sh
$ ./redis-port sync -f 127.0.0.1:6379 -t 127.0.0.1:6380 -n 8
  2014/10/28 15:15:41 [ncpu=8] sync from '127.0.0.1:6379' to '127.0.0.1:6380'
  2014/10/28 15:15:42 -
  2014/10/28 15:15:43 -
  2014/10/28 15:15:44 -
  2014/10/28 15:15:46 total = 278110192 -      9380927 [  3%]
  2014/10/28 15:15:47 total = 278110192 -     18605075 [  6%]
  ... ...                                              [    ]
  2014/10/28 15:16:14 total = 278110192 -    269990892 [ 97%]
  2014/10/28 15:16:15 total = 278110192 -    278110192 [100%]
  2014/10/28 15:16:15 done
  2014/10/28 15:16:17 pipe: send = 0             recv = 0
  2014/10/28 15:16:18 pipe: send = 0             recv = 0
  ... ...
```
