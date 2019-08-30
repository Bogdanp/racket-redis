# racket-redis

(WIP) Redis bindings for Racket.

## Installation

    $ raco pkg install redis-rkt  # NOT "redis", that's a different, older package!
    $ raco doc redis

## Basic Usage

``` racket
(require redis)

(define c (make-redis))
(redis-bytes-set! c "some-key" "hello, world!")
(redis-bytes-ref c "some-key")  ;; => #"hello, world!"
```

## Missing commands

The commands below are yet to be implemented:

* [ ]  BITFIELD
* [ ]  BITOP
* [ ]  BITPOS
* [ ]  BLPOP
* [ ]  BRPOP
* [ ]  BRPOPLPUSH
* [ ]  BZPOPMAX
* [ ]  BZPOPMIN
* [ ]  CLIENT KILL
* [ ]  CLIENT LIST
* [ ]  CLIENT PAUSE
* [ ]  CLIENT REPLY
* [ ]  CLIENT UNBLOCK
* [ ]  CLUSTER ADDSLOTS
* [ ]  CLUSTER COUNT-FAILURE-REPORTS
* [ ]  CLUSTER COUNTKEYSINSLOT
* [ ]  CLUSTER DELSLOTS
* [ ]  CLUSTER FAILOVER
* [ ]  CLUSTER FORGET
* [ ]  CLUSTER GETKEYSINSLOT
* [ ]  CLUSTER INFO
* [ ]  CLUSTER KEYSLOT
* [ ]  CLUSTER MEET
* [ ]  CLUSTER NODES
* [ ]  CLUSTER REPLICAS
* [ ]  CLUSTER REPLICATE
* [ ]  CLUSTER RESET
* [ ]  CLUSTER SAVECONFIG
* [ ]  CLUSTER SET-CONFIG-EPOCH
* [ ]  CLUSTER SETSLOT
* [ ]  CLUSTER SLAVES
* [ ]  CLUSTER SLOTS
* [ ]  COMMAND
* [ ]  COMMAND COUNT
* [ ]  COMMAND GETKEYS
* [ ]  COMMAND INFO
* [ ]  CONFIG GET
* [ ]  CONFIG RESETSTAT
* [ ]  CONFIG REWRITE
* [ ]  CONFIG SET
* [ ]  DEBUG OBJECT
* [ ]  DEBUG SEGFAULT
* [ ]  DISCARD
* [ ]  DUMP
* [ ]  EXEC
* [ ]  GEOADD
* [ ]  GEODIST
* [ ]  GEOHASH
* [ ]  GEOPOS
* [ ]  GEORADIUS
* [ ]  GEORADIUSBYMEMBER
* [ ]  GETBIT
* [ ]  GETRANGE
* [ ]  GETSET
* [ ]  HDEL
* [ ]  HEXISTS
* [ ]  HGET
* [ ]  HGETALL
* [ ]  HINCRBY
* [ ]  HINCRBYFLOAT
* [ ]  HKEYS
* [ ]  HLEN
* [ ]  HMGET
* [ ]  HMSET
* [ ]  HSCAN
* [ ]  HSET
* [ ]  HSETNX
* [ ]  HSTRLEN
* [ ]  HVALS
* [ ]  INFO
* [ ]  KEYS
* [ ]  LASTSAVE
* [ ]  LPUSHX
* [ ]  MEMORY DOCTOR
* [ ]  MEMORY HELP
* [ ]  MEMORY MALLOC-STATS
* [ ]  MEMORY PURGE
* [ ]  MEMORY STATS
* [ ]  MEMORY USAGE
* [ ]  MIGRATE
* [ ]  MONITOR
* [ ]  MOVE
* [ ]  MSET
* [ ]  MSETNX
* [ ]  MULTI
* [ ]  OBJECT
* [ ]  PFADD
* [ ]  PFCOUNT
* [ ]  PFMERGE
* [ ]  PING
* [ ]  PSETEX
* [ ]  PSUBSCRIBE
* [ ]  PUBLISH
* [ ]  PUBSUB
* [ ]  PUNSUBSCRIBE
* [ ]  READONLY
* [ ]  READWRITE
* [ ]  REPLICAOF
* [ ]  RESTORE
* [ ]  ROLE
* [ ]  RPOPLPUSH
* [ ]  RPUSHX
* [ ]  SADD
* [ ]  SAVE
* [ ]  SCAN
* [ ]  SCARD
* [ ]  SDIFF
* [ ]  SDIFFSTORE
* [ ]  SETBIT
* [ ]  SETEX
* [ ]  SETNX
* [ ]  SETRANGE
* [ ]  SHUTDOWN
* [ ]  SINTER
* [ ]  SINTERSTORE
* [ ]  SISMEMBER
* [ ]  SLAVEOF
* [ ]  SLOWLOG
* [ ]  SMEMBERS
* [ ]  SMOVE
* [ ]  SORT
* [ ]  SPOP
* [ ]  SRANDMEMBER
* [ ]  SREM
* [ ]  SSCAN
* [ ]  STRLEN
* [ ]  SUBSCRIBE
* [ ]  SUNION
* [ ]  SUNIONSTORE
* [ ]  SWAPDB
* [ ]  SYNC
* [ ]  TIME
* [ ]  TYPE
* [ ]  UNLINK
* [ ]  UNSUBSCRIBE
* [ ]  UNWATCH
* [ ]  WAIT
* [ ]  WATCH
* [ ]  XACK
* [ ]  XADD
* [ ]  XCLAIM
* [ ]  XDEL
* [ ]  XGROUP
* [ ]  XINFO
* [ ]  XLEN
* [ ]  XPENDING
* [ ]  XRANGE
* [ ]  XREAD
* [ ]  XREADGROUP
* [ ]  XREVRANGE
* [ ]  XTRIM
* [ ]  ZADD
* [ ]  ZCARD
* [ ]  ZCOUNT
* [ ]  ZINCRBY
* [ ]  ZINTERSTORE
* [ ]  ZLEXCOUNT
* [ ]  ZPOPMAX
* [ ]  ZPOPMIN
* [ ]  ZRANGE
* [ ]  ZRANGEBYLEX
* [ ]  ZRANGEBYSCORE
* [ ]  ZRANK
* [ ]  ZREM
* [ ]  ZREMRANGEBYLEX
* [ ]  ZREMRANGEBYRANK
* [ ]  ZREMRANGEBYSCORE
* [ ]  ZREVRANGE
* [ ]  ZREVRANGEBYLEX
* [ ]  ZREVRANGEBYSCORE
* [ ]  ZREVRANK
* [ ]  ZSCAN
* [ ]  ZSCORE
* [ ]  ZUNIONSTORE

## Acknowledgements

This project was originally based on [rackdis].

[rackdis]: https://github.com/eu90h/rackdis
