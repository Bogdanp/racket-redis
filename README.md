# racket-redis

Fast, idiomatic redis bindings for Racket.

## Installation

    $ raco pkg install redis-rkt  # NOT "redis", that's a different, older package!
    $ raco doc redis

## Basic Usage

```racket
(require redis)

(define c (make-redis))
(redis-bytes-set! c "some-key" "hello, world!")
(redis-bytes-get c "some-key")  ;; => #"hello, world!"
(redis-bytes-get c "some-key" "some-other-key")  ;; => '(#"hello, world!" #f)

;; or, with a connection pool:

(define pool (make-redis-pool))
(call-with-redis-client pool
  (lambda (c)
    (redis-bytes-set! c "some-key" "hello, world!")
    (redis-bytes-get c "some-key")))
```

## Missing commands

The commands below are yet to be implemented:

* [ ]  BITFIELD
* [ ]  BITPOS
* [ ]  BZPOPMAX
* [ ]  BZPOPMIN
* [ ]  CLIENT KILL
* [ ]  CLIENT LIST
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
* [ ]  GEOADD
* [ ]  GEODIST
* [ ]  GEOHASH
* [ ]  GEOPOS
* [ ]  GEORADIUS
* [ ]  GEORADIUSBYMEMBER
* [ ]  GETBIT
* [ ]  GETRANGE
* [ ]  GETSET
* [ ]  HINCRBY
* [ ]  HINCRBYFLOAT
* [ ]  HSCAN
* [ ]  HSETNX
* [ ]  HSTRLEN
* [ ]  LPUSHX
* [ ]  MIGRATE
* [ ]  MONITOR
* [ ]  MSET
* [ ]  MSETNX
* [ ]  OBJECT
* [ ]  PSETEX
* [ ]  PSUBSCRIBE
* [ ]  PUBLISH
* [ ]  PUBSUB
* [ ]  PUNSUBSCRIBE
* [ ]  READONLY
* [ ]  READWRITE
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
* [ ]  UNSUBSCRIBE
* [ ]  WAIT
* [ ]  XCLAIM
* [ ]  XPENDING
* [ ]  XREAD
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

This project was originally based on [rackdis], but has since been
rewritten from the ground up for increased performance and safety.

[rackdis]: https://github.com/eu90h/rackdis
